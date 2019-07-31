#!/usr/bin/python

"""Python Script to be used by a Lambda Function to Terminate Idle EMRs"""

# Required Imports
import os
import time
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

def format_datetime_object(datetime_object, date_time_format):
    """Get Formatted Datetime Object Routine"""

    return datetime.strptime(datetime_object.strftime(date_time_format), date_time_format)

def get_emr_client(tries=1):
    """Get Boto3 EMR Client Routine"""

    try:
        return boto3.client('emr')
    except ClientError as exception_obj:
        if exception_obj.response['Error']['Code'] == 'ThrottlingException':
            if tries <= 3:
                print("Throttling Exception Occured.")
                print("Retrying.....")
                print("Attempt No.: " + str(tries))
                time.sleep(3)
                return get_emr_client(tries + 1)
            else:
                print("Attempted 3 Times But No Success.")
                print("Raising Exception.....")
                raise
        else:
            raise

def get_waiting_emr_clusters(emr_client, tries=1):
    """Get List of EMR Clusters in WAITING State Routine"""

    try:
        return emr_client.list_clusters(ClusterStates=['WAITING'])
    except ClientError as exception_obj:
        if exception_obj.response['Error']['Code'] == 'ThrottlingException':
            if tries <= 3:
                print("Throttling Exception Occured.")
                print("Retrying.....")
                print("Attempt No.: " + str(tries))
                time.sleep(3)
                return get_waiting_emr_clusters(emr_client, tries + 1)
            else:
                print("Attempted 3 Times But No Success.")
                print("Raising Exception.....")
                raise
        else:
            raise

def get_non_running_emr_steps(emr_client, emr_cluster_id, tries=1):
    """Get List of EMR Cluster Steps in COMPLETED, CANCELLED, FAILED or INTERRUPTED State Routine"""

    try:
        return emr_client.list_steps(
            ClusterId=emr_cluster_id,
            StepStates=['COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED'])
    except ClientError as exception_obj:
        if exception_obj.response['Error']['Code'] == 'ThrottlingException':
            if tries <= 3:
                print("Throttling Exception Occured.")
                print("Retrying.....")
                print("Attempt No.: " + str(tries))
                time.sleep(3)
                return get_non_running_emr_steps(
                    emr_client, emr_cluster_id, tries + 1)
            else:
                print("Attempted 3 Times But No Success.")
                print("Raising Exception.....")
                raise
        else:
            raise

def terminate_emr_cluster(emr_client, emr_cluster_id, tries=1):
    """Terminate EMR Routine"""

    try:
        emr_client.set_termination_protection(JobFlowIds=[emr_cluster_id], TerminationProtected=False)
        emr_client.terminate_job_flows(JobFlowIds=[emr_cluster_id])
        return 1
    except ClientError as exception_obj:
        if exception_obj.response['Error']['Code'] == 'ThrottlingException':
            if tries <= 3:
                print("Throttling Exception Occured.")
                print("Retrying.....")
                print("Attempt No.: " + str(tries))
                time.sleep(3)
                return terminate_emr_cluster(emr_client, emr_cluster_id, tries + 1)
            else:
                print("Attempted 3 Times But No Success.")
                print("Raising Exception.....")
                raise
        else:
            print("ERROR: " + str(exception_obj))
            return 0

def auto_terminate_idle_emr(max_idle_time_in_minutes):
    """Auto Terminate IDLE EMR Main Routine"""

    total_emr_clusters = 0
    emr_clusters_terminated = 0

    date_time_format = "%Y-%m-%d %H:%M:%S"
    current_datetime = datetime.now()
    current_datetime = format_datetime_object(current_datetime, date_time_format)
    print("Current Time: " + str(current_datetime))

    print("Creating EMR Client.....")
    emr_client = get_emr_client()

    marker = None
    marker_found = True
    while marker_found is True:
        print("Fetching and Checking EMR Clusters in WAITING State, If Any.....")
        emr_clusters = get_waiting_emr_clusters(emr_client, marker)

        if 'Marker' in emr_clusters:
            print("Marker Found.")
            marker = emr_clusters['Marker']
        else:
            marker_found = False

        total_emr_clusters += len(emr_clusters['Clusters'])

        for emr_cluster in emr_clusters['Clusters']:
            print("EMR Cluster ID: " + emr_cluster['Id'])
            emr_cluster_ready_datetime = emr_cluster['Status']['Timeline']['ReadyDateTime']
            emr_cluster_ready_datetime = format_datetime_object(
                emr_cluster_ready_datetime, date_time_format)

            emr_cluster_steps = get_non_running_emr_steps(
                emr_client, emr_cluster['Id'])

            if emr_cluster_steps['Steps'][0:1]:
                emr_step = emr_cluster_steps['Steps'][0]
                emr_cluster_step_end_datetime = emr_step['Status']['Timeline']['EndDateTime']
                emr_cluster_step_end_datetime = format_datetime_object(
                    emr_cluster_step_end_datetime, date_time_format)

                datetime_difference_in_minutes = (
                    current_datetime - emr_cluster_step_end_datetime).total_seconds() / 60.0

                if datetime_difference_in_minutes >= max_idle_time_in_minutes:
                    print("Current Time: " + str(current_datetime))
                    print("EMR Cluster Last Step End Time: " + str(emr_cluster_step_end_datetime))
                    print("Datetime Difference in Minutes: " + str(datetime_difference_in_minutes))

                    print("Terminating EMR Cluster.....")
                    emr_clusters_terminated += terminate_emr_cluster(emr_client, emr_cluster['Id'])
            else:
                datetime_difference_in_minutes = (
                    current_datetime - emr_cluster_ready_datetime).total_seconds() / 60.0

                if datetime_difference_in_minutes >= max_idle_time_in_minutes:
                    print("Current Time: " + str(current_datetime))
                    print("EMR Cluster Ready Time: " + str(emr_cluster_ready_datetime))
                    print("Datetime Difference in Minutes: " + str(datetime_difference_in_minutes))

                    print("Terminating EMR Cluster.....")
                    emr_clusters_terminated += terminate_emr_cluster(emr_client, emr_cluster['Id'])

    print("Printing Terminated EMR Clusters Stats.....")
    print("Total EMR Clusters in WAITING State: " + str(total_emr_clusters))
    print("Total EMR Clusters Terminated Successfully: " + str(emr_clusters_terminated))

    return

def lambda_handler(event, context):
    """Lambda Handler"""

    try:
        print("FUNCTION START")

        auto_terminate_idle_emr(int(os.environ["MAX_IDLE_TIME_IN_MINUTES"]))

        print("FUNCTION END")
    except Exception as exception_obj:
        print("ERROR: " + str(exception_obj))

    return None
