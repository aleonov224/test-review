# Test task 1
# Creates an S3 bucket if it doesn't exist.
# S3 bucket name is not configurable via parameters
# (keep in mind that script may run across different accounts and regions;
# follows best practices for bucket name)
# Gather a list of running EC2 instances
# (in the specified account and region) and upload this information to the
# bucket you created.
# The list must be in CSV format and contain the following fields:
#  the name of the instance (if not specified, then Instance ID)
#  description of the EC2 instance AMI
#
# Use following pattern for the S3 items name: ec2-instances-<timestamp>.csv
#
# Parameters:
#  Account ID - !removed after Q&A discussion!
#  Region
# ###
# Execution, environment:
# Instal pipenv virtual environment tool by 'pip install pipenv'
# Place this file with Pip*.* files in some directory
# Run 'pipenv install' to setup dependencies
# A: Run shell using 'pipenv shell' command, and next, run
#    'python task-a.py' for info or 'python task-a.py --region <AWS_REGION>'
# B: In the other way, run directly 'pipenv run python task-a.py' or
#    'pipenv run python task-a.py --region <AWS_REGION>'
#
# AWS credentials must be configured for the local run
#
# Known script weaknes:
# - SCV file writing not wrapped in the try/except block
# - Local CSV file will not deleted after running error


import os
import boto3
from botocore.client import ClientError
from enum import Enum
import click
import csv
import datetime
import logging


logger = logging.getLogger()


class RunInfo(Enum):
    """ CSV report column IDs
    """
    Name = 1
    Ami = 2


def get_bucket_name(account_id, region):
    """ Generate s3 bucket name

    :param account_id: AWS Account ID
    :param region: AWS region
    """
    return f'ec2-{account_id}-{region}-list'


def is_bucket_exists(s3, bucket_name):
    """ Check bucket_name exists and available access

    :param s3: AWS s3 resource
    :param bucket_name: AWS s3 bucket name
    """
    exists = False
    allow_access = False
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        exists = True
        allow_access = True
    except ClientError as ce:
        error_code = int(ce.response['Error']['Code'])
        if error_code == 403:
            exists = True
            logger.warning(
                f'Bucket {bucket_name} is private. Forbidden access.')
        elif error_code == 404:
            logger.info(
                f'Bucket {bucket_name} does not exist. Will be created.')
    return exists and allow_access


def create_not_exists_bucket(s3, bucket_name):
    """ Create named bucket if it is not exists

    :param s3: AWS s3 resource
    :param bucket_name: Expected bucket name
    """
    try:
        if not is_bucket_exists(s3=s3, bucket_name=bucket_name):
            s3.create_bucket(Bucket=bucket_name)
    except ClientError as ce:
        logger.warning(str(ce))


def get_ec2_name(instance):
    """ Get ec2 instance name

    :param instance: AWS ec2 instance
    """
    for tag in instance.tags:
        if tag['Key'] == 'Name':
            return tag['Value']


def generate_file_name():
    """ Generate file name based on timestamp
    """
    now = datetime.datetime.now()
    timestamp = str(datetime.datetime.timestamp(now))
    timestamp_f_name = timestamp.replace('.', '-')
    return f'ec2-instances-{timestamp_f_name}.csv'


def get_ec2_list(ec2):
    """ Get running ec2 instances list

    :param ec2: AWS ec2 resource
    """
    filters = [
        {
            'Name': 'instance-state-name',
            'Values': ['running']
        }
    ]
    running_istances = []
    try:
        instances = ec2.instances.filter(Filters=filters)
        for instance in instances:
            instance_info = {}
            instance_name = get_ec2_name(instance=instance)
            if instance_name is None:
                instance_name = instance.instance_id
            image = ec2.Image(instance.image_id)
            instance_ami = image.description
            if instance_ami is None:
                instance_ami = image.name
            instance_info[RunInfo.Name] = instance_name
            instance_info[RunInfo.Ami] = instance_ami
            running_istances.append(instance_info)
    except ClientError as ce:
        logger.warning(str(ce))
    return running_istances


def write_local_file(file_name, running_istances):
    """ Write local file for uploading

    :param file_name: CSV report local file name
    :param running_instances: List of current sript ec2 parameters dict
    """
    with open(file_name, mode='w') as ec2_run_file:
        writer = csv.writer(
            ec2_run_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
        for r_instance in running_istances:
            writer.writerow(
                [r_instance[RunInfo.Name], r_instance[RunInfo.Ami]])


@click.command()
@click.option("--region", required=True)
def create_running_ec2_list(region):
    """ Create runnig ec2 istances list CSV file uploaded to S3 bucket

    :param region: AWS region
    """
    try:
        account_id = boto3.client('sts').get_caller_identity().get('Account')
        s3 = boto3.resource('s3')
        ec2 = boto3.resource('ec2', region_name=region)
        client = boto3.client('ecr', region_name=region)
        client = boto3.client('sts')

        account_id = client.get_caller_identity()["Account"]
        s3_bucket_name = get_bucket_name(account_id=account_id, region=region)

        create_not_exists_bucket(s3=s3, bucket_name=s3_bucket_name)

        csv_f_name = generate_file_name()

        write_local_file(
            file_name=csv_f_name, running_istances=get_ec2_list(ec2=ec2))

        s3_client = boto3.client('s3')
        s3_client.upload_file(csv_f_name, s3_bucket_name, csv_f_name)

        if os.path.exists(csv_f_name):
            os.remove(csv_f_name)
    except ClientError as ce:
        logger.error(ce)


if __name__ == "__main__":
    create_running_ec2_list()
