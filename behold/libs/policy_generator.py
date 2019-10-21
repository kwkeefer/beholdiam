import json
import logging
import requests

logger = logging.getLogger(__name__)


class PolicyGenerator():
    """ Class for parsing actions used by service. """
    def __init__(self):
        self.actions = []
        self.get_service_actions()

    def get_service_actions(self):
        """ Gets a list of possible actions from AWS Policy Generator site. """
        services = requests.get('https://awspolicygen.s3.amazonaws.com/js/policies.js').text.strip('app.PolicyEditorConfig=')
        services = json.loads(services)['serviceMap']
        for service in services:
            for action in services[service]['Actions']:
                self.actions.append(f"{services[service]['StringPrefix']}:{action}")

    def generate_list_of_actions(self, list_of_dicts):
        """ Generates a list of service:action to be used for creating IAM policies.
        CloudTrail records eventsource in format s3.amazonaws.com (using S3 as an example).
        Service is extracted from the full event source. """
        supported_actions = []
        unsupported_actions = []
        for dictionary in list_of_dicts:
            service = dictionary['eventsource'].split('.')[0]
            action = f"{service}:{dictionary['eventname']}"
            if action in self.actions:
                supported_actions.append(action)
            else:
                logger.info(f"{action} not supported.")
                unsupported_actions.append(action)

        supported_actions.sort()
        unsupported_actions.sort()
        return supported_actions, unsupported_actions

    @staticmethod
    def format_actions(list_of_actions):
        """ Returns formatted list of actions that will be outputted in .txt files. """
        formatted_actions = []
        for action in list_of_actions:
            formatted_actions.append(f'"{action}"')
        formatted_actions = ",\n".join(formatted_actions)
        return formatted_actions

    @staticmethod
    def build_policy(list_of_actions):
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
