import boto3
import logging
from datetime import datetime
from . import athena_query_strings
from .iam import IAM

logger = logging.getLogger(__name__)

class Athena():
    def __init__(self, metadata):
        self.metadata = metadata
        self.cloudtrail_bucket = metadata['cloudtrail_bucket']
        self.behold_bucket = metadata['behold_bucket']
        self.region = metadata['region']
        self.queries = metadata['queries']
        self.create_client()

    def create_client(self):
        self.client = boto3.client('athena', region_name=self.region)

    def start_query_execution(self, query_string, path):
        response = self.client.start_query_execution(
            QueryString = query_string,
            ResultConfiguration={
                "OutputLocation": f"s3://{self.behold_bucket}/{path}"
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return response['QueryExecutionId']
        else:
            logger.error(f"Response failed:\n{response}")

    def set_up_table_and_patitions(self):
        if 'years_to_partition' in self.metadata:
            years = self.metadata['years_to_partition']
        else:
            years = [datetime.now().year]
        query_string, path = athena_query_strings.create_table(self.cloudtrail_bucket)
        self.start_query_execution(query_string, path)
        for account in self.metadata['accounts_to_partition']:
            for region in self.metadata['regions_to_partition']:
                for year in years:
                    query_string, path = athena_query_strings.add_to_partition(
                            cloudtrail_bucket=self.cloudtrail_bucket,
                            account=account,
                            region=region,
                            year=year
                        )
                    self.start_query_execution(query_string, path)

    def active_roles_query(self):
        try:
            accounts = [self.queries['active_roles']['account']]
        except KeyError:
            accounts = self.metadata['accounts_to_partition']
        try:
            days_back = self.queries['active_roles']['days_back']
        except KeyError:
            days_back = 7

        for account in accounts:
            query_string, path = athena_query_strings.active_roles(
                    account=account,
                    days_back=days_back
                )
            self.start_query_execution(query_string, path)

    def active_users_query(self):
        try:
            accounts = [self.queries['active_users']['account']]
        except KeyError:
            accounts = self.metadata['accounts_to_partition']
        try:
            days_back = self.queries['active_users']['days_back']
        except KeyError:
            days_back = 7

        for account in accounts:
            query_string, path = athena_query_strings.active_users(
                    account=account,
                    days_back=days_back
                )
            self.start_query_execution(query_string, path)

    def services_by_role_query(self):
        logger.info(self.queries['services_by_role'])

    def services_by_user_query(self):
        pass

    def parse_queries(self):
        for query in self.queries:
            if query == 'active_roles':
                self.active_roles_query()
            elif query == 'active_users':
                self.active_users_query()
            elif query == 'services_by_role':
                self.services_by_role_query()
            elif query == 'services_by_user':
                self.services_by_user_query()
            else:
                logger.error("No valid queries found in metadata.")
