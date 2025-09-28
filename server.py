from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import PlainTextResponse
import boto3
import io
import os
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config
import asyncio

app = FastAPI()

# AWS Configuration - MAXIMUM PERFORMANCE
config = Config(
    max_pool_connections=500,
    retries={'max_attempts': 1, 'mode': 'standard'},
    region_name='us-east-1'
)

s3_client = boto3.client('s3', config=config)
sdb_client = boto3.client('sdb', config=config)
executor = ThreadPoolExecutor(max_workers=100)

def upload_to_s3(file_content: bytes, filename: str) -> bool:
    try:
        file_like = io.BytesIO(file_content)
        s3_client.upload_fileobj(file_like, '1233383933-in-bucket', filename)
        return True
    except:
        return False

def get_classification(item_name: str) -> str:
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

@app.post("/", response_class=PlainTextResponse)
async def handle_request(inputFile: UploadFile = File(...)):
    try:
        if not inputFile.filename:
            raise HTTPException(status_code=400, detail="No file selected")
        
        filename = inputFile.filename
        filename_without_ext = os.path.splitext(filename)[0]
        
        # Read file content
        file_content = await inputFile.read()
        
        # Start S3 upload in background (don't wait)
        executor.submit(upload_to_s3, file_content, filename)
        
        # Get classification immediately
        classification = get_classification(filename_without_ext)
        
        # Return result immediately
        if classification:
            result = f'{filename_without_ext}:{classification}'
        else:
            result = f'{filename_without_ext}:Unknown'
        
        return result
    
    except Exception as e:
        return f"Error: {str(e)}"

@app.get("/health", response_class=PlainTextResponse)
async def health_check():
    return "OK"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1)
