import json
import logging

logger = logging.getLogger(__name__)

class PolicyGenerator():
    def __init__(self):
        pass

    def generate_list_of_actions(self, list_of_dicts):
        actions = []
        for dictionary in list_of_dicts:
            service = dictionary['eventsource'].split('.')[0]
            action = f"{service}:{dictionary['eventname']}"
            actions.append(action)
        actions.sort()
        return actions

    def format_actions(self, list_of_actions):
        formatted_actions = []
        for action in list_of_actions:
            formatted_actions.append(f'"{action}"')
        formatted_actions = ",\n".join(formatted_actions)
        return formatted_actions

    def build_policy(self, list_of_actions):
        
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
