import boto3
import logging

logger = logging.getLogger(__name__)

class S3():
    def __init__(self, metadata):
        self.region = metadata['region']
        self.create_client()

    def create_client(self):
        self.client = boto3.client('s3', region_name=self.region)

    def get_object(self, bucket, key):
        obj = self.client.get_object(
            Bucket=bucket,
            Key=key
        )
        return obj['Body'].read().decode()
