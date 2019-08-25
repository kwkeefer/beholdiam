from libs import metadata
from libs.athena import Athena
from libs.iam import IAM
from libs.s3 import S3
from libs.csv_parser import CSVParser
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

if args.setup:
    athena.set_up_table_and_patitions()

athena.active_resources()

logger.info(athena.active_roles_output_files)
logger.info(athena.active_users_output_files)

for dictionary in athena.active_roles_output_files:
    obj = s3.get_object(meta['behold_bucket'], dictionary['path'])
    roles_list = csv.single_column_csv_to_list(obj)
    athena.services_by_role_query(
        account=dictionary['account'],
        roles=roles_list
    )

logger.info(athena.services_by_role_output_files)

for dictionary in athena.active_users_output_files:
    obj = s3.get_object(meta['behold_bucket'], dictionary['path'])
    users_list = csv.single_column_csv_to_list(obj)
    athena.services_by_user_query(
        account=dictionary['account'],
        users=users_list
    )

logger.info(athena.services_by_user_output_files)
