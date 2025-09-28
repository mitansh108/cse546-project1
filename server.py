from flask import Flask, request, Response
import boto3
import os

# Initialize Flask app
app = Flask(__name__)

# Initialize Boto3 clients.
# By not providing a custom config, Boto3 will use safe, default settings
# that are perfect for a t2.micro instance.
s3_client = boto3.client('s3', region_name='us-east-1')
sdb_client = boto3.client('sdb', region_name='us-east-1')

# Define your bucket and domain names
S3_BUCKET_NAME = "1233383933-in-bucket"
SIMPLEDB_DOMAIN = "1233383933-simpleDB"

@app.route('/', methods=['POST'])
def handle_request():
    # Check if the inputFile is in the request
    if 'inputFile' not in request.files:
        return Response("No inputFile provided", status=400, mimetype='text/plain')

    file = request.files['inputFile']
    
    # Check if a file was selected
    if file.filename == '':
        return Response("No file selected", status=400, mimetype='text/plain')

    filename = file.filename
    filename_without_ext = os.path.splitext(filename)[0]

    try:
        # 1. Upload the file to S3
        # Use upload_fileobj which is efficient for file-like objects from Flask
        s3_client.upload_fileobj(file, S3_BUCKET_NAME, filename)

        # 2. Query SimpleDB for the classification
        # CRITICAL FIX: The lookup key in SimpleDB is the FULL filename (e.g., "test_00.jpg")
        response = sdb_client.get_attributes(
            DomainName=SIMPLEDB_DOMAIN,
            ItemName=filename 
        )

        classification = "Unknown" # Default value
        if 'Attributes' in response:
            for attr in response['Attributes']:
                if attr['Name'] == 'classification':
                    classification = attr['Value']
                    break
        
        # 3. Format and return the response
        result = f"{filename_without_ext}:{classification}"
        return Response(result, status=200, mimetype='text/plain')

    except Exception as e:
        # If anything goes wrong, return a server error
        return Response(f"An error occurred: {e}", status=500, mimetype='text/plain')

# This part is only for running the app directly with `python server.py` for testing.
# It will not be used by Gunicorn.
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
