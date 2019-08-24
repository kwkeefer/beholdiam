from libs import metadata
from libs.athena import Athena
from libs.iam import IAM
from libs.s3 import S3
from libs.csv_parser import CSVParser
import argparse
import logging

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

# athena = Athena(meta)
# if args.setup:
#     athena.set_up_table_and_patitions()
# athena.parse_queries()

# iam = IAM(meta)
# iam.get_all_roles()
# print(iam.role_arns)

s3 = S3(meta)
# obj = s3.get_object("athena-output-keefer", "results/active_roles/032193469888/0a75a128-9900-4806-942a-39308dca664c.csv")
obj = s3.get_object("athena-output-keefer", "results/active_roles/032193469888/0a75a128-9900-4806-942a-39308dca664c.csv")
print(obj)
print(type(obj))

csv = CSVParser()
# csv.csv_to_list_of_dicts(obj)
csv.single_column_csv_to_list(obj)