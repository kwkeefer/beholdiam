import boto3
import logging

logger = logging.getLogger(__name__)

class IAM():
    def __init__(self, metadata):
        self.create_client()

    def create_client(self):
        self.client = boto3.client('iam', region_name='us-east-1')

    def get_all_roles(self):
        self.role_arns = []
        paginator = self.client.get_paginator('list_roles')
        response_iterator = paginator.paginate()
        for page in response_iterator:
            for role in page['Roles']:
                self.role_arns.append(role['Arn'])
