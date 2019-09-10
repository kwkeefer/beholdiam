import json
import logging

logger = logging.getLogger(__name__)


class PolicyGenerator():
    """ Class for parsing actions used by service. """
    def __init__(self):
        """ Nothing happens during init. """
        pass

    def generate_list_of_actions(self, list_of_dicts):
        """ Generates a list of service:action to be used for creating IAM policies.
        CloudTrail records eventsource in format s3.amazonaws.com (using S3 as an example).
        Service is extracted from the full event source. """
        actions = []
        for dictionary in list_of_dicts:
            service = dictionary['eventsource'].split('.')[0]
            action = f"{service}:{dictionary['eventname']}"
            actions.append(action)
        actions.sort()
        return actions

    def format_actions(self, list_of_actions):
        """ Returns formatted list of actions that will be outputted in .txt files. """
        formatted_actions = []
        for action in list_of_actions:
            formatted_actions.append(f'"{action}"')
        formatted_actions = ",\n".join(formatted_actions)
        return formatted_actions

    def build_policy(self, list_of_actions):
        """ Returns IAM policy using list of actions.  Will be outputted to .json files. """
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "behold",
                    "Effect": "Allow",
                    "Action": list_of_actions,
                    "Resource": "*"
                }
            ]
        }
        return json.dumps(policy)
