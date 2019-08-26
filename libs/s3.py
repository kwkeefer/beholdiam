import boto3
import botocore
import time
import logging

logger = logging.getLogger(__name__)


class S3():
    """ Class for interacting with S3 service in AWS. """
    def __init__(self, metadata):
        """ Sets region and creates boto3 client. """
        self.region = metadata['region']
        self.create_client()

    def create_client(self):
        """ Creates boto3 client as self.client. """
        self.client = boto3.client('s3', region_name=self.region)

    def get_object(self, bucket, key):
        """ Checks if object exists.  If it doesn't, wait half a second and check again.
        Once the object is found it is downloaded."""
        logger.info(f"Downloading {key} from {bucket}.")
        obj_exists = False
        while not obj_exists:
            try:
                self.client.head_object(
                    Bucket=bucket,
                    Key=key
                )
                obj_exists = True
            except botocore.exceptions.ClientError:
                time.sleep(.500)

        obj = self.client.get_object(
            Bucket=bucket,
            Key=key
        )
        return obj['Body'].read().decode()

    def put_object(self, bucket, key, encoded_object):
        """ Uploads object to S3. """
        logger.info(f"Uploading {key} to {bucket}.")
        self.client.put_object(
            Body=encoded_object,
            Bucket=bucket,
            Key=key
        )
