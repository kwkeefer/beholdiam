import boto3
import logging
from . import athena_query_strings

logger = logging.getLogger(__name__)

class Athena():
    def __init__(self, metadata):
        self.cloudtrail_bucket = metadata['cloudtrail_bucket']
        self.output_bucket = metadata['output_bucket']
        self.region = metadata['region']
        self.create_client()

    def create_client(self):
        self.client = boto3.client('athena', region_name=self.region)

    def create_athena_table(self):
        self.client.start_query_execution(
            QueryString = athena_query_strings.create_table(self.cloudtrail_bucket),
            ResultConfiguration={
                "OutputLocation": f"s3://{self.output_bucket}"
            }
        )
