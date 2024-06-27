import ast
import os
import pandas as pd
import pika
import time
import spacy
from pyresparser import ResumeParser
import warnings
import json
from azure.storage.blob import BlobClient
from spacy.matcher import Matcher
import jsonlines
from bs4 import BeautifulSoup
from services.Rules import Rules
from source.schemas.matched_resume import ResumeMatchedModel
import PyPDF2, pdfplumber

warnings.filterwarnings("ignore")
nlp = spacy.load('en_core_web_sm')
matcher = Matcher(nlp.vocab)





# Read environment variables
rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest')
rabbitmq_pass = os.getenv('RABBITMQ_PASS', 'guest')

sleepTime = 1
print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

# Create RabbitMQ connection
credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        credentials=credentials
    )
)

channel = connection.channel()
channel.queue_declare(queue='Flask-Processing', durable=True)
channelAck = connection.channel()
channelAck.queue_declare(queue='Flask-Ack', durable=True)
print(' [*] Waiting for messages.')



# Function to add pattern to PhraseMatcher
def add_pattern_to_matcher(matcher, label, pattern):
    matcher.add(label, [pattern])


# Load patterns from JSONL file
def load_patterns_from_jsonl(file_path):

    patterns = []
    labels = []
    with jsonlines.open(file_path, "r") as file:
        for item in file:
            label = item["label"]
            pattern_text = item["pattern"]
            pattern = [pattern_token for pattern_token in pattern_text]
            patterns.append(pattern)
            labels.append(label)
    return patterns, labels


# Path to your skills JSONL file
degrees_file_path = "Resources/data/degrees.jsonl"
majors_file_path = "Resources/data/majors.jsonl"
skills_file_path = "Resources/data/skills.jsonl"

# Load skills patterns and labels
degrees_patterns, degrees_labels = load_patterns_from_jsonl(degrees_file_path)
majors_patterns, majors_labels = load_patterns_from_jsonl(majors_file_path)
skills_patterns, skills_labels = load_patterns_from_jsonl(skills_file_path)

# Add skills patterns to PhraseMatcher
for label, pattern in zip(degrees_labels, degrees_patterns):
    add_pattern_to_matcher(matcher, label, pattern)
for label, pattern in zip(majors_labels, majors_patterns):
    add_pattern_to_matcher(matcher, label, pattern)
    for label, pattern in zip(skills_labels, skills_patterns):
        add_pattern_to_matcher(matcher, label, pattern)


def modifying_type_resume(resumes):

    for i in range(len(resumes["degrees"])):
        resumes["degrees"][i] = ast.literal_eval(resumes["degrees"][i])
    for i in range(len(resumes["skills"])):
        resumes["skills"][i] = ast.literal_eval(resumes["skills"][i])
    for i in range(len(resumes["majors"])):
        resumes["majors"][i] = ast.literal_eval(resumes["majors"][i])
    return resumes


def modifying_type_job(jobs):

    for i in range(len(jobs["Skills"])):
        jobs["Skills"][i] = ast.literal_eval(jobs["Skills"][i])
    for i in range(len(jobs["Acceptable majors"])):
        jobs["Acceptable majors"][i] = ast.literal_eval(jobs["Acceptable majors"][i])
    for i in range(len(jobs["Minimum degree level"])):
        jobs["Minimum degree level"][i] = ast.literal_eval(jobs["Minimum degree level"][i])

    return jobs






def callback(ch, method, properties, body):
    print(" [x] Received %s" % body)
    cmd = body.decode()
    cmd = json.loads(cmd)
    print(cmd)
    
    cv = cmd['url']
    job = cmd['jobhtml']
    soup = BeautifulSoup(job, 'html.parser')

    job = soup.get_text()
    print(job)

    # Extract skills entities
    job_doc = nlp(job)
    job_matches = matcher(job_doc)
    job_entities = []
    for match_id, start, end in job_matches:
        label = nlp.vocab.strings[match_id]
        span = job_doc[start:end]
        job_entities.append((span.text, label))

    cv_doc = nlp(cv)
    cv_matches = matcher(cv_doc)
    cv_entities = []
    for match_id, start, end in cv_matches:
        label = nlp.vocab.strings[match_id]
        span = cv_doc[start:end]
        cv_entities.append((span.text, label))

    job_data = {
        'Qualifications': [job],
        'Minimum degree level': [],
        'Acceptable majors': [],
        'Skills': []
    }

    cv_data = {
        '_id': "62ea4aef9964a9062d2b18dc",
        'degrees': [],
        'majors': [],
        'skills': []
    }

    # Print extracted skills entities
    for entity, label in job_entities:
        # print(entity, "-", label)
        if "DEGREE" in label:
            job_data['Minimum degree level'].append(label.split('|')[1])
        elif "MAJOR" in label:
            job_data['Acceptable majors'].append(entity)
        elif "SKILL" in label:
            job_data['Skills'].append(entity)
    job_df = pd.DataFrame([job_data])
    job_df.to_csv("new_job.csv")
    # Print extracted skills entities
    for entity, label in cv_entities:
        print(entity, "-", label)
        if "DEGREE" in label:
            cv_data['degrees'].append(label.split('|')[1])
        elif "MAJOR" in label:
            cv_data['majors'].append(entity)
        elif "SKILL" in label:
            cv_data['skills'].append(entity)
    cv_df = pd.DataFrame([cv_data])
    cv_df.to_csv("new_resume.csv")

    with open('Resources/data/plus/labels.json') as fp:
        labels = json.load(fp)
        jobs = pd.read_csv('new_job.csv', index_col=0)
        resumes = pd.read_csv('new_resume.csv', index_col=0)
        resumes = modifying_type_resume(resumes)
        jobs = modifying_type_job(jobs)
        rules = Rules(labels, resumes, jobs)
        job_indexes = [0]
        resumes_matched_jobs = pd.DataFrame()
        for job_index in job_indexes:
            resumes_matched = rules.matching_score(resumes, jobs, job_index)
            resumes_matched_jobs = pd.concat([resumes_matched_jobs, resumes_matched])

            # adding matched resumes to database
            for i, row in resumes_matched.iterrows():
                id_resume = resumes_matched['_id'][i]
                degree_matching = float(resumes_matched['Degree job ' + str(job_index) + ' matching'][i])

                major_matching = float(resumes_matched['Major job ' + str(job_index) + ' matching'][i])
                skills_semantic_matching = float(
                    resumes_matched['Skills job ' + str(job_index) + ' semantic matching'][i])
                matching_score = float(resumes_matched['matching score job ' + str(job_index)][i])
                matched_resume = ResumeMatchedModel(id_resume=id_resume if id_resume else '',
                                                    job_index=job_index if job_index else 0,
                                                    degree_matching=degree_matching if degree_matching else 0,
                                                    major_matching=major_matching if major_matching else 0,
                                                    skills_semantic_matching=skills_semantic_matching
                                                    if skills_semantic_matching else 0,
                                                    matching_score=matching_score if matching_score else 0)

                print(matched_resume)
    data = {}
    data['id'] = cmd['id']
    data['degree_matching'] =int(matched_resume.degree_matching * 100)
    data['major_matching'] = int(matched_resume.major_matching * 100)
    data['skills_semantic_matching'] = int(matched_resume.skills_semantic_matching * 100)
    data['final_matching_score'] = int(matched_resume.matching_score * 100)
    data['job_information']= job_data
    data['cv_information']= cv_data
    data['Qualifications'] = job_data['Qualifications'] 
    data['Minimum degree level'] = job_data['Minimum degree level']
    data['Acceptable majors']= job_data['Acceptable majors']
    data['Skills'] = job_data['Skills']
    data['degrees'] = cv_data['degrees']
    data['majors']= cv_data['majors']
    data['skills']= cv_data['skills']
    json_formatted_str = json.dumps(data, indent=2)

    print(json_formatted_str)

    print(" [x] Done")

    channelAck.basic_publish(
        exchange='',
        routing_key='Flask-Ack',
        body=json_formatted_str,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    ch.basic_ack(delivery_tag=method.delivery_tag)






channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='Flask-Processing', on_message_callback=callback)
channel.start_consuming()
















