import json

def lambda_handler(event, context):
    # TODO
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda - via codebuild1!')
    }
