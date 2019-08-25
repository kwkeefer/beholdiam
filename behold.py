from libs import metadata
from libs.athena import Athena
from libs.iam import IAM
from libs.s3 import S3
from libs.csv_parser import CSVParser
from libs.policy_generator import PolicyGenerator
import argparse
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger("main")

parser = argparse.ArgumentParser()
parser.add_argument("metadata")
parser.add_argument("--setup", action="store_true")
args = parser.parse_args()

meta = metadata.read(args.metadata)
s3 = S3(meta)
athena = Athena(meta)
csv = CSVParser()
policygen = PolicyGenerator()

if args.setup:
    athena.set_up_table_and_patitions()

athena.active_resources()

for dictionary in athena.active_roles_output_files:
    obj = s3.get_object(meta['behold_bucket'], dictionary['path'])
    roles_list = csv.single_column_csv_to_list(obj)
    athena.services_by_role_query(
        account=dictionary['account'],
        roles=roles_list
    )

for dictionary in athena.services_by_role_output_files:
    obj = s3.get_object(meta['behold_bucket'], dictionary['path'])
    list_of_dicts = csv.csv_to_list_of_dicts(obj)
    actions = policygen.generate_list_of_actions(list_of_dicts)
    formatted_actions = policygen.format_actions(actions)
    s3.put_object(
        bucket=meta['behold_bucket'],
        key=f"behold_results/{dictionary['account']}/roles/{dictionary['role_name']}.txt",
        encoded_object=formatted_actions.encode()
    )
    policy = policygen.build_policy(actions)
    s3.put_object(
        bucket=meta['behold_bucket'],
        key=f"behold_results/{dictionary['account']}/roles/{dictionary['role_name']}.json",
        encoded_object=policy.encode()
    )    

for dictionary in athena.active_users_output_files:
    obj = s3.get_object(meta['behold_bucket'], dictionary['path'])
    users_list = csv.single_column_csv_to_list(obj)
    athena.services_by_user_query(
        account=dictionary['account'],
        users=users_list
    )

for dictionary in athena.services_by_user_output_files:
    obj = s3.get_object(meta['behold_bucket'], dictionary['path'])
    list_of_dicts = csv.csv_to_list_of_dicts(obj)
    actions = policygen.generate_list_of_actions(list_of_dicts)
    formatted_actions = policygen.format_actions(actions)
    s3.put_object(
        bucket=meta['behold_bucket'],
        key=f"behold_results/{dictionary['account']}/users/{dictionary['user_name']}.txt",
        encoded_object=formatted_actions.encode()
    )
    policy = policygen.build_policy(actions)
    s3.put_object(
        bucket=meta['behold_bucket'],
        key=f"behold_results/{dictionary['account']}/users/{dictionary['user_name']}.json",
        encoded_object=policy.encode()
    )

