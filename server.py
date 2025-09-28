from flask import Flask, request, Response
import os
import boto3
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config

# Set up logging - reduce verbosity for performance
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

app = Flask(__name__)

config = Config(
    max_pool_connections=100,
    retries={'max_attempts': 3, 'mode': 'standard'},
    region_name='us-east-1'
)

s3_client = boto3.client('s3', config=config)
sdb_client = boto3.client('sdb', config=config)
executor = ThreadPoolExecutor(max_workers=50)

def upload_to_s3(file_content, filename):
    try:
        file_like = io.BytesIO(file_content)
        s3_client.upload_fileobj(file_like, '1233383933-in-bucket', filename)
        return True
    except:
        return False

def get_classification(item_name):
    try:
        response = sdb_client.get_attributes(
            DomainName='1233383933-simpleDB',
            ItemName=item_name,
            AttributeNames=['classification'],
            ConsistentRead=False
        )
        for attr in response.get('Attributes', []):
            if attr['Name'] == 'classification':
                return attr['Value']
        return None
    except:
        return None

@app.route('/', methods=['POST'])
def handle_request():
    if 'inputFile' not in request.files:
        return Response('No inputFile provided', status=400, mimetype='text/plain')
    
    file = request.files['inputFile']
    if file.filename == '':
        return Response('No file selected', status=400, mimetype='text/plain')
    
    filename = file.filename
    filename_without_ext = os.path.splitext(filename)[0]
    
    file.seek(0)
    file_content = file.read()
    
    upload_to_s3(file_content, filename)
    classification = get_classification(filename_without_ext)
    
    if classification:
        result = f'{filename_without_ext}:{classification}'
    else:
        result = f'{filename_without_ext}:Unknown'
    
    return Response(result, status=200, mimetype='text/plain')

@app.route('/health', methods=['GET'])
def health_check():
    return Response('OK', status=200, mimetype='text/plain')

if __name__ == '__main__':
    # Use gunicorn-like settings for better concurrency
    app.run(host='0.0.0.0', port=8000, threaded=True, processes=1)
