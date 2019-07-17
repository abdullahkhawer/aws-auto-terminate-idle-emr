# Auto Terminate Idle AWS EMR Clusters Framework v1.0

-   Founder: Abdullah Khawer (LinkedIn: https://www.linkedin.com/in/abdullah-khawer/)

## Introduction

Auto Terminate Idle AWS EMR Clusters Framework v1.0 is an AWS based solution using AWS CloudWatch and AWS Lambda using a Python script that is using Boto3 to terminate AWS EMR clusters that have been idle for a specified period of time.

You specify the maximum idle time threshold and AWS CloudWatch event/rule triggers an AWS Lambda function that queries all AWS EMR clusters in WAITING state and for each, compares the current time with AWS EMR cluster's ready time in case of no EMR steps added so far or compares the current time with AWS EMR cluster's last step's end time. If the threshold has been compromised, the AWS EMR will be terminated after removing termination protection if enabled. If not, it will skip that AWS EMR cluster.

AWS CloudWatch event/rule will decide how often AWS Lambda function should check for idle AWS EMR clusters.

You can disable the AWS CloudWatch event/rule at any time to disable this framework in a single click without deleting its AWS CloudFormation stack.

AWS Lambda function is using Python 3.7 as its runtime environment.

### Any contributions, improvements and suggestions will be highly appreciated.

## Usage Notes

Following are the steps to successfully deploy and use this framework:
-   Clone this repository from the master branch.
-   Compress *auto_terminate_idle_emr.py* file in zip format and put it on AWS S3 bucket.
-   Login to AWS console with IAM user credentials having the required admin privileges to create resources via AWS CloudFormation.
-   Go to AWS CloudFormation and choose to *Create Stack*.
-   Under *Choose a template* Either upload *cft_auto_terminate_idle_emr.json* from here or put it on AWS S3 bucket and enter AWS S3 URL for that file.
-   Enter any suitable *Stack Name*.
-   Enter *CloudWatchEventScheduleExpression* which is AWS CloudWatch Event's Schedule Expression in the form of either Rate Function (e.g., rate(5 minutes)) or CRON Expression (e.g., cron(0/5 * * * ? *)) which will decide how ofter to trigger AWS Lambda function that does the actual job.
-   Enter *LambdaCodeS3Bucket* which is AWS S3 Bucket Name having AWS Lambda Function Code (e.g., my-bucket).
-   Enter *LambdaCodeS3BucketKey* which is AWS S3 Bucket Key having AWS Lambda Function Code (e.g., lambda/code/auto_terminate_idle_emr.zip).
-   Enter *MaxIdleTimeInMinutes* which is Maximum Idle Time in Minutes for Any AWS EMR Cluster.
-   Enter suitable Tags if required.
-   Under *Review*, select *I acknowledge that AWS CloudFormation might create IAM resources with custom names.* and click create.
-   Wait for the stack to change its *Status* to *CREATE_COMPLETE*.
-   Voila, you are done and everything is now up and running.

### Warning: You will be billed for the AWS resources used if you create a stack for this framework.

## Future Improvements

Following are the future improvements in the queue:
-   Add the capability to avoid a race condition (e.g., using AWS DynamoDB).
-   Update the AWS CloudFormation template with the new AWS resources added to achieve avoiding a race condition.
