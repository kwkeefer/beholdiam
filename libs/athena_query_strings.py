import logging

logger = logging.getLogger(__name__)

def create_table(bucketname):
    logger.info(f"Creating Athena table from CloudTrail bucket {bucketname}")
    query_string = f"""CREATE EXTERNAL TABLE cloudtrail_logs (
            eventversion STRING,
            useridentity STRUCT<
                        type:STRING,
                        principalid:STRING,
                        arn:STRING,
                        accountid:STRING,
                        invokedby:STRING,
                        accesskeyid:STRING,
                        userName:STRING,
            sessioncontext:STRUCT<
            attributes:STRUCT<
                        mfaauthenticated:STRING,
                        creationdate:STRING>,
            sessionissuer:STRUCT<
                        type:STRING,
                        principalId:STRING,
                        arn:STRING,
                        accountId:STRING,
                        userName:STRING>>>,
            eventtime STRING,
            eventsource STRING,
            eventname STRING,
            awsregion STRING,
            sourceipaddress STRING,
            useragent STRING,
            errorcode STRING,
            errormessage STRING,
            requestparameters STRING,
            responseelements STRING,
            additionaleventdata STRING,
            requestid STRING,
            eventid STRING,
            resources ARRAY<STRUCT<
                        ARN:STRING,
                        accountId:STRING,
                        type:STRING>>,
            eventtype STRING,
            apiversion STRING,
            readonly STRING,
            recipientaccountid STRING,
            serviceeventdetails STRING,
            sharedeventid STRING,
            vpcendpointid STRING
            )
            PARTITIONED BY (account string, region string, year string)
            ROW FORMAT SERDE 'com.amazon.emr.hive.serde.CloudTrailSerde'
            STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION 's3://{bucketname}/AWSLogs/';
            """
    return (query_string, "setup/create_table")


def add_to_partition(cloudtrail_bucket, account, region, year):
    logger.info(f"Adding to partition: {account} | {region} | {year}")
    query_string = f"""ALTER TABLE cloudtrail_logs 
        ADD PARTITION (account='{account}', region='{region}', year='{year}') 
        LOCATION 's3://{cloudtrail_bucket}/AWSLogs/{account}/CloudTrail/{region}/{year}/';"""
    return (query_string, f"setup/add_to_partition/{account}-{region}-{year}")


def active_roles(account, days_back):
    logger.info(f"Running active_roles: {account} | {days_back} days back.")
    query_string = f"""SELECT DISTINCT useridentity.sessioncontext.sessionissuer.arn
        FROM cloudtrail_logs
        WHERE account = '{account}'
        AND useridentity.type = 'AssumedRole'
        AND from_iso8601_timestamp(eventtime) > date_add('day', -{days_back}, now());"""
    return (query_string, f"results/active_roles/{account}")


def active_users(account, days_back):
    logger.info(f"Running active_users: {account} | {days_back} days back.")
    query_string = f"""SELECT DISTINCT useridentity.arn
        FROM cloudtrail_logs
        WHERE account = '{account}'
        AND useridentity.type = 'IAMUser'
        AND useridentity.arn IS NOT NULL
        AND from_iso8601_timestamp(eventtime) > date_add('day', -{days_back}, now());"""
    return (query_string, f"results/active_users/{account}")


def services_by_role(account, days_back, role_arn):
    query_string = f"""SELECT DISTINCT eventsource, eventname FROM cloudtrail_logs
        WHERE account = '{account}'
        AND (useridentity.sessioncontext.sessionissuer.arn = '{role_arn}')
        AND from_iso8601_timestamp(eventtime) > date_add('day', -{days_back}, now())
        ORDER BY eventsource, eventname;"""
    role_name = role_arn.split('/')[1]
    return (query_string, f"results/services_by_role/{account}/{role_name}")


def services_by_user(account, days_back, user_arn):
    query_string = f"""SELECT DISTINCT eventsource, eventname FROM cloudtrail_logs 
        WHERE account = '{account}' 
        AND (useridentity.arn = '{user_arn}') 
        AND from_iso8601_timestamp(eventtime) > date_add('day', -{days_back}, now())
        ORDER BY eventsource, eventname;"""
    user_name = user_arn.split('/')[1]
    return (query_string, f"results/services_by_user/{account}/{user_name}")
