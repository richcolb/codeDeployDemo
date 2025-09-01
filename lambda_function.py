import json

def lambda_handler(event, context):
    response = {
        'statusCode': 200,
        'headers': {"Content-Type": "application/json"},
        'body': "Hello from Lambda - via codebuild version 2"
    }
    return(response)


