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
parser.add_argument("--metadata")
args = parser.parse_args()

meta = metadata.read(args.metadata)
athena_obj = Athena(meta)
#athena_obj.create_athena_table()

iam_obj = IAM(meta)
iam_obj.get_all_roles()
print(iam_obj.role_arns)