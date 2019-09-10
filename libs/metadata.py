from . import utils
from datetime import datetime
import os
import logging
import sys
import yaml

logger = logging.getLogger(__name__)


def read(filename):
    """ Reads metadata file.  Returns dict after performing validation and setting defaults. """
    logger.info(f"Reading {filename} from metadata folder.")
    metadata = os.path.abspath(filename)

    with open(metadata, "r") as meta:
        metadata = yaml.load(meta.read())

    validate_metadata(metadata)

    return metadata


def validate_metadata(metadata):
    """ Ensuring required values are set. """
    if 'region' not in metadata:
        sys.exit("'region' must be included in metadata file.\n" +
                 "'region' specifies the region in which to create the Athena table.\n" +
                 "'region' should be in the same region as your CloudTrail bucket.")
    elif 'cloudtrail_bucket' not in metadata:
        sys.exit("'cloudtrail_bucket' must be included in metadata file.\n" +
                 "'cloudtrail_bucket' specifies the name of your CloudTrail bucket.")
    elif 'behold_bucket' not in metadata:
        sys.exit("'behold_bucket' must be included in metadata file.\n" +
                 "'behold_bucket' specifies the bucket that behold will store its output files.\n" +
                 "'behold_bucket' should be in the same region as your CloudTrail bucket.")


def set_defaults(metadata, boto=None):
    """ Sets defaults for non-required values if they are not specified in the metadata file. """
    if boto is None:
        boto = utils.Boto(metadata)
    if 'years_to_partition' not in metadata:
        current_year = datetime.now().year
        logger.info(f"Setting 'years_to_partition' to {current_year}")
        metadata['years_to_partition'] = [current_year]
    if 'days_back' not in metadata:
        logger.info(f"Setting 'days_back' to 30.")
        metadata['days_back'] = 30
    if 'regions_to_partition' not in metadata:
        logger.info("Setting 'regions_to_partition' to all supported CloudTrail regions.")
        metadata['regions_to_partition'] = boto.get_cloudtrail_regions()
    if 'accounts_to_partition' not in metadata:
        local_account = boto.get_account()
        logger.info(f"Setting 'accounts_to_partition' to {local_account}")
        metadata['accounts_to_partition'] = [local_account]
    return metadata
