from libs import metadata
from libs.athena import Athena
from libs.iam import IAM
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

athena = Athena(meta)
if args.setup:
    athena.set_up_table_and_patitions()
athena.parse_queries()

iam = IAM(meta)
iam.get_all_roles()
print(iam.role_arns)