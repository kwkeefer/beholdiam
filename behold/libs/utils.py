import boto3
import datetime


def year_month_parser(days_back=30):
    """ Calculates a list of tuples formatted in (year, month) given days_back """
    year_month = set()
    for x in range(days_back):
        date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=x)
        year_month_tuple = date.strftime("%Y"), date.strftime("%m")
        year_month.add(year_month_tuple)
    return list(year_month)


class Boto():
    def __init__(self, metadata):
        self.region = metadata['region']
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
