""" Used to build and return Athena queries. """


def create_table(bucketname):
    """ Returns query for behold table in Athena. """
    query_string = f"""CREATE EXTERNAL TABLE behold (
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
            PARTITIONED BY (account string, region string, year string, month string)
            ROW FORMAT SERDE 'com.amazon.emr.hive.serde.CloudTrailSerde'
            STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION 's3://{bucketname}/AWSLogs/';
            """
    return (query_string, "setup/create_table")


def add_to_partition(cloudtrail_bucket, account, region, year, month):
    """ Returns query for adding partition to behold Athena table. """
    query_string = f"""ALTER TABLE behold
        ADD PARTITION (account='{account}', region='{region}', year='{year}', month='{month}')
        LOCATION 's3://{cloudtrail_bucket}/AWSLogs/{account}/CloudTrail/{region}/{year}/{month}/';"""
    return (query_string, f"setup/add_to_partition/{account}-{region}-{year}-{month}")


def active_roles(account, days_back):
    """ Returns query for finding active roles (since days_back value). """
    query_string = f"""SELECT DISTINCT useridentity.sessioncontext.sessionissuer.arn
        FROM behold
        WHERE account = '{account}'
        AND useridentity.type = 'AssumedRole'
        AND from_iso8601_timestamp(eventtime) > date_add('day', -{days_back}, now());"""
    return (query_string, f"athena_results/active_roles/{account}")


def active_users(account, days_back):
    """ Returns query for finding active users (since days_back value)."""
    query_string = f"""SELECT DISTINCT useridentity.arn
        FROM behold
        WHERE account = '{account}'
        AND useridentity.type = 'IAMUser'
        AND useridentity.arn IS NOT NULL
        AND from_iso8601_timestamp(eventtime) > date_add('day', -{days_back}, now());"""
    return (query_string, f"athena_results/active_users/{account}")


def services_by_role(account, days_back, role_arn, role_name):
    """ Returns query for eventsource (service) / actions performed by role. """
    query_string = f"""SELECT DISTINCT eventsource, eventname FROM behold
        WHERE account = '{account}'
        AND (useridentity.sessioncontext.sessionissuer.arn = '{role_arn}')
        AND from_iso8601_timestamp(eventtime) > date_add('day', -{days_back}, now())
        ORDER BY eventsource, eventname;"""
    return (query_string, f"athena_results/services_by_role/{account}/{role_name}")


def services_by_user(account, days_back, user_arn, user_name):
    """ Returns query for eventsource (service) / actions performed by user. """
    query_string = f"""SELECT DISTINCT eventsource, eventname FROM behold
        WHERE account = '{account}'
        AND (useridentity.arn = '{user_arn}')
        AND from_iso8601_timestamp(eventtime) > date_add('day', -{days_back}, now())
        ORDER BY eventsource, eventname;"""
    return (query_string, f"athena_results/services_by_user/{account}/{user_name}")
