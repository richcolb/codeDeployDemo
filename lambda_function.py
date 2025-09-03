import boto3
import botocore
import json
import logging
import os
from botocore.exceptions import ClientError
from typing import Dict, Any

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize global clients outside the handler for connection reuse
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
rekognition_client = boto3.client('rekognition')
sns_client = boto3.client('sns')

# Constants
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
print(DYNAMODB_TABLE)
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')
print(SNS_TOPIC_ARN)
PRESIGNED_URL_EXPIRATION = 3600
OWNER = "richcolb"

#main_fucntion_logic
class ImageProcessor:
    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key
        self.labels_dict = {}
        self.put_dict = {}
        
    def process_image(self) -> Dict[str, Any]:
        try:
            # Build the image path
            self.put_dict["image_path"] = f"https://{self.bucket}.s3.eu-west-1.amazonaws.com/{self.key}"
            self.put_dict["owner"] = OWNER
            
            # Detect labels and generate presigned URL
            self.detect_labels()
            presigned_url = self.generate_presigned_url()
            
            # Write to DynamoDB and send SNS notification
            self.write_to_ddb()
            self.send_to_sns(presigned_url)
            
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Image processed successfully"})
            }
            
        except ClientError as e:
            logger.error(f"Error processing image: {str(e)}")
            raise
            
    def detect_labels(self) -> None:
        try:
            response = rekognition_client.detect_labels(
                Image={'S3Object': {"Bucket": self.bucket, "Name": self.key}}
            )
            
            self.labels_dict = {
                label["Name"]: str(label["Confidence"])
                for label in response["Labels"]
            }
            self.put_dict["labels"] = self.labels_dict
            
        except ClientError as e:
            logger.error(f"Error detecting labels: {str(e)}")
            raise

    def write_to_ddb(self) -> None:
        try:
            table = dynamodb.Table(DYNAMODB_TABLE)
            table.put_item(Item=self.put_dict)
        except ClientError as e:
            logger.error(f"Error writing to DynamoDB: {str(e)}")
            raise

    def generate_presigned_url(self) -> str:
        try:
            return s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': self.key},
                ExpiresIn=PRESIGNED_URL_EXPIRATION
            )
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {str(e)}")
            raise

    def send_to_sns(self, presigned_url: str) -> None:
        try:
            labels_str = '\n'.join(f"{k}:{v}" for k, v in self.labels_dict.items())
            
            message = (
                f"A new object has arrived in the Amazon S3 bucket\n\n"
                f"Bucket Name: {self.bucket}\n"
                f"Object Name: {self.key}\n\n"
                f"The detected labels are:\n{labels_str}\n\n"
                f"You can view the image by clicking on the following URL:\n{presigned_url}"
            )
            
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message,
                Subject="New object detected labels"
            )
            
        except ClientError as e:
            logger.error(f"Error sending SNS notification: {str(e)}")
            raise
        finally:
            self.labels_dict.clear()
            self.put_dict.clear()

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function
    """
    try:
        logger.info("Processing event: %s", json.dumps(event))
        
        # Extract S3 event details
        record = event["Records"][0]["s3"]
        bucket = record["bucket"]["name"]
        key = record["object"]["key"]
        
        # Process the image
        processor = ImageProcessor(bucket, key)
        return processor.process_image()
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
