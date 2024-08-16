import json

def handler(event, context):
    for record in event['Records']:
        body = record.get('body', '')
        
        # Process the body payload
        print(f"Processing message: {body}")
        # Add your Python script logic here

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }