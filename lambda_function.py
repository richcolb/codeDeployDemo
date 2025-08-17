import json

def lambda_handler(event, context):
    # TODO
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda - via codebuild - this is now working - via API GW! - test change- and again!!')
    }
