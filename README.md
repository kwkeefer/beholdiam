# behold

Behold uses [Athena](https://docs.aws.amazon.com/athena/latest/ug/what-is.html) to query your [CloudTrail](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-getting-started.html) log bucket and determine which users and roles are being actively used.  Behold uses this list of active users and roles to determine which actions have been used by each user and role, and generates least-privilege IAM policies for each user and role.    

Note that [not all AWS services](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-aws-service-specific-topics.html) are supported by CloudTrail.  Also note that, [by default, trails log all management events and don't include data events.](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/logging-management-and-data-events-with-cloudtrail.html)  This means that IAM policies generated by Behold may be incomplete and need a bit of tuning.  (Use caution when removing IAM actions from resources in your environments.)    

