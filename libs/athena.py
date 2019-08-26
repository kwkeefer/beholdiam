import boto3
import logging
from datetime import datetime
from . import athena_query_strings

logger = logging.getLogger(__name__)


class Athena():
    def __init__(self, metadata):
        try:
            self.days_back = metadata['days_back']
        except KeyError:
            self.days_back = 30
        self.metadata = metadata
        self.accounts = self.metadata['accounts_to_partition']
        self.cloudtrail_bucket = metadata['cloudtrail_bucket']
        self.behold_bucket = metadata['behold_bucket']
        self.region = metadata['region']
        self.create_client()

    def active_resources(self):
        self.active_roles_output_files = []
        self.active_users_output_files = []
        self.active_roles_query()
        self.active_users_query()

    def create_client(self):
        self.client = boto3.client('athena', region_name=self.region)

    def start_query_execution(self, query_string, path):
        response = self.client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration={
                "OutputLocation": f"s3://{self.behold_bucket}/{path}"
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return response['QueryExecutionId']
        else:
            logger.error(f"Response failed:\n{response}")

    def set_up_table_and_patitions(self):
        logger.info("Setting up Athena table.")
        if 'years_to_partition' in self.metadata:
            years = self.metadata['years_to_partition']
        else:
            years = [datetime.now().year]
        query_string, path = athena_query_strings.create_table(
            self.cloudtrail_bucket)
        self.start_query_execution(query_string, path)
        for account in self.metadata['accounts_to_partition']:
            for region in self.metadata['regions_to_partition']:
                for year in years:
                    logger.info(f"Adding partition to Athena table: {account} | {region} | {year}")
                    query_string, path = athena_query_strings.add_to_partition(
                        cloudtrail_bucket=self.cloudtrail_bucket,
                        account=account,
                        region=region,
                        year=year
                    )
                    self.start_query_execution(query_string, path)

    def active_roles_query(self):
        for account in self.accounts:
            logger.info(f"Querying Athena for active roles in {account} (past {self.days_back} days).")
            query_string, path = athena_query_strings.active_roles(
                account=account,
                days_back=self.days_back
            )
            execution_id = self.start_query_execution(query_string, path)
            output_dict = {
                "account": account,
                "path": f"{path}/{execution_id}.csv"
            }
            self.active_roles_output_files.append(output_dict)

    def active_users_query(self):
        for account in self.accounts:
            logger.info(f"Querying Athena for active users in {account} (past {self.days_back} days).")
            query_string, path = athena_query_strings.active_users(
                account=account,
                days_back=self.days_back
            )
            execution_id = self.start_query_execution(query_string, path)
            output_dict = {
                "account": account,
                "path": f"{path}/{execution_id}.csv"
            }
            self.active_users_output_files.append(output_dict)

    def services_by_role_query(self, account, list_of_arns):
        logger.info(f"Querying Athena for services used by role in {account}.")
        self.services_by_role_output_files = []
        for role_arn in list_of_arns:
            role_name = role_arn.split('/')[1]
            query_string, path = athena_query_strings.services_by_role(
                account=account,
                days_back=self.days_back,
                role_arn=role_arn,
                role_name=role_name
            )
            execution_id = self.start_query_execution(query_string, path)
            output_dict = {
                "account": account,
                "role_arn": role_arn,
                "name": role_name,
                "path": f"{path}/{execution_id}.csv"
            }
            self.services_by_role_output_files.append(output_dict)

    def services_by_user_query(self, account, list_of_arns):
        logger.info(f"Querying Athena for services used by user in {account}")
        self.services_by_user_output_files = []
        for user_arn in list_of_arns:
            user_name = user_arn.split('/')[1]
            query_string, path = athena_query_strings.services_by_user(
                account=account,
                user_arn=user_arn,
                user_name=user_name,
                days_back=self.days_back
            )
            execution_id = self.start_query_execution(query_string, path)
            output_dict = {
                "account": account,
                "user_arn": user_arn,
                "name": user_name,
                "path": f"{path}/{execution_id}.csv"
            }
            self.services_by_user_output_files.append(output_dict)
