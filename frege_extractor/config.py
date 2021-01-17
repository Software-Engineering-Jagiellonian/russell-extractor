import os
import sys
import logging


INPUT_QUEUE = 'extract'

# Names of languages (held in database) mapped to their output queue names
OUTPUT_QUEUES = {
    'C': 'analyze-c',
    'C++': 'analyze-cpp',
    'C#': 'analyze-csharp',
    'CSS': 'analyze-css',
    'Java': 'analyze-java',
    'JS': 'analyze-js',
    'PHP': 'analyze-php',
    'Python': 'analyze-python',
    'Ruby': 'analyze-ruby',
}

try:
    RABBITMQ_HOST = os.environ['RMQ_HOST']
    RABBITMQ_PORT = os.environ['RMQ_PORT']
    DB_HOST = os.environ['DB_HOST']
    DB_PORT = os.environ['DB_PORT']
    DB_DATABASE = os.environ['DB_DATABASE']
    DB_USERNAME = os.environ['DB_USERNAME']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    # Path for the downloaded repos
    REPOSITORIES_DIRECTORY = os.environ['REPOSITORIES_DIRECTORY']

except KeyError as ke:
    logging.error('Environmental variable {} not set!'.format(ke))
    sys.exit(1)

