import os
import yaml
import logging

logger = logging.getLogger(__name__)


def read(filename):
    logger.info(f"Reading {filename} from metadata folder.")
    current_dir = os.path.dirname(__file__)
    parent_dir = os.path.dirname(current_dir)
    metadata = os.path.join(parent_dir, f"metadata/{filename}")

    with open(metadata, "r") as meta:
        yaml_dict = yaml.load(meta.read())

    return yaml_dict
