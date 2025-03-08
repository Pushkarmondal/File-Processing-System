import json
import boto3
import csv
import uuid
from datetime import datetime

# AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('event_data')

def lambda_handler(event, context):
    try:
        # Extract bucket name and object key from event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        # Fetch CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8').splitlines()
        csv_reader = csv.reader(content)

        # Extract headers (column names)
        headers = next(csv_reader)
        
        # Read all remaining rows into a list to count them
        rows = list(csv_reader)
        row_count = len(rows)

        # Metadata extraction
        metadata = {
            'event_id': str(uuid.uuid4()),
            'filename': object_key,
            'upload_timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'timestamp': int(datetime.utcnow().timestamp()),
            'file_size_bytes': response.get('ContentLength', 0),
            'row_count': row_count,
            'column_count': len(headers),
            'column_names': headers
        }

        # Store metadata in DynamoDB
        table.put_item(Item=metadata)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Metadata stored successfully', 'metadata': metadata})
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}