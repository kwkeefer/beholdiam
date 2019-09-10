from libs import metadata
from libs import utils
from libs.athena import Athena
from libs.s3 import S3
from libs.csv_parser import CSVParser
from libs.policy_generator import PolicyGenerator
import argparse
import logging


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("metadata")
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    if args.debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    logger = logging.getLogger("main")

    # Reading metadata, performing metadata validation, initializing required classes.
    meta = metadata.read(args.metadata)
    boto = utils.Boto(meta)
    meta = metadata.set_defaults(meta, boto)
    s3 = S3(meta, boto.session)
    athena = Athena(meta, boto.session)
    csv = CSVParser()
    policygen = PolicyGenerator()

    # If --setup flag is passed, the Athena table and partition tables are set up.
    # Only needs to be done once unless metadata is updated to add more accounts, regions, or years.
    if args.setup:
        athena.set_up_table_and_patitions()

    athena.active_resources()

    def get_arns_from_athena_output(users_or_roles):
        """ Function to get list of arns of active users or roles. """
        if users_or_roles == "users":
            athena_output_files = athena.active_users_output_files
            services_by_query = athena.services_by_user_query
        elif users_or_roles == "roles":
            athena_output_files = athena.active_roles_output_files
            services_by_query = athena.services_by_role_query

        for dictionary in athena_output_files:
            obj = s3.get_object(meta["behold_bucket"], dictionary["path"])
            list_of_arns = csv.single_column_csv_to_list(obj)
            services_by_query(
                account=dictionary["account"],
                list_of_arns=list_of_arns
            )

    def build_behold_output_files(users_or_roles):
        """ Builds list of services/actions and IAM policy for each role or user. """
        if users_or_roles == "users":
            athena_services_by_output_files = athena.services_by_user_output_files
        elif users_or_roles == "roles":
            athena_services_by_output_files = athena.services_by_role_output_files

        for dictionary in athena_services_by_output_files:
            obj = s3.get_object(meta["behold_bucket"], dictionary["path"])
            list_of_dicts = csv.csv_to_list_of_dicts(obj)
            actions = policygen.generate_list_of_actions(list_of_dicts)
            formatted_actions = policygen.format_actions(actions)
            s3.put_object(
                bucket=meta["behold_bucket"],
                key=f"behold_results/{dictionary['account']}/{users_or_roles}/{dictionary['name']}.txt",
                encoded_object=formatted_actions.encode()
            )
            policy = policygen.build_policy(actions)
            s3.put_object(
                bucket=meta['behold_bucket'],
                key=f"behold_results/{dictionary['account']}/{users_or_roles}/{dictionary['name']}.json",
                encoded_object=policy.encode()
            )

    get_arns_from_athena_output("users")
    get_arns_from_athena_output("roles")
    build_behold_output_files("users")
    build_behold_output_files("roles")


if __name__ == '__main__':
    main()