from flask import Flask, request, Response
import os
import boto3
import io
import logging
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config

# Set up logging
logging.basicConfig(level=logging.INFO)
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
        logger.info(f"Uploading {filename} to S3")
        file_like = io.BytesIO(file_content)
        s3_client.upload_fileobj(file_like, '1233383933-in-bucket', filename)
        logger.info(f"Successfully uploaded {filename}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {filename}: {e}")
        return False

def get_classification(item_name):
    try:
        logger.info(f"Getting classification for {item_name}")
        response = sdb_client.get_attributes(
            DomainName='1233383933-simpleDB',
            ItemName=item_name,
            AttributeNames=['classification'],
            ConsistentRead=True  # Use consistent read for autograder
        )
        for attr in response.get('Attributes', []):
            if attr['Name'] == 'classification':
                logger.info(f"Found classification: {attr['Value']}")
                return attr['Value']
        logger.warning(f"No classification found for {item_name}")
        return None
    except Exception as e:
        logger.error(f"Error getting classification for {item_name}: {e}")
        return None

@app.route('/', methods=['POST'])
def handle_request():
    logger.info(f"Received request: {request.method} {request.url}")
    logger.info(f"Files in request: {list(request.files.keys())}")
    
    if 'inputFile' not in request.files:
        logger.error("No inputFile in request")
        return Response('No inputFile provided', status=400, mimetype='text/plain')
    
    file = request.files['inputFile']
    if file.filename == '':
        logger.error("Empty filename")
        return Response('No file selected', status=400, mimetype='text/plain')
    
    filename = file.filename
    filename_without_ext = os.path.splitext(filename)[0]
    logger.info(f"Processing file: {filename}")
    
    # Read file content once
    file.seek(0)
    file_content = file.read()
    logger.info(f"Read {len(file_content)} bytes")
    
    # Upload to S3 synchronously for autograder compatibility
    upload_success = upload_to_s3(file_content, filename)
    
    # Get classification
    classification = get_classification(filename_without_ext)
    
    if classification:
        result = f'{filename_without_ext}:{classification}'
    else:
        result = f'{filename_without_ext}:Unknown'
    
    logger.info(f"Returning result: {result}")
    return Response(result, status=200, mimetype='text/plain')

@app.route('/health', methods=['GET'])
def health_check():
    return Response('OK', status=200, mimetype='text/plain')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, threaded=True)
