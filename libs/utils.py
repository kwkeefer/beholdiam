import boto3


class Boto():
    def __init__(self, metadata):
        self.region = metadata['region']
        self.create_session()

    def create_session(self):
        """ Creates boto3 session. """
        self.session = boto3.session.Session(region_name=self.region)

    def get_cloudtrail_regions(self):
        """ Uses boto3 session to return list of supported regions for CloudTrail. """
        cloudtrail_regions = self.session.get_available_regions('cloudtrail')
        return cloudtrail_regions

    def get_account(self):
        """ Uses sts get_caller_identity function to return local account ID. """
        self.stsclient = boto3.client('sts')
        account = self.stsclient.get_caller_identity()['Account']
        return account
