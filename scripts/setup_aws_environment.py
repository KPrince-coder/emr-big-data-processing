#!/usr/bin/env python3
"""
AWS Environment Setup Script

This script sets up the AWS environment for the Big Data Processing with EMR project.
It creates the S3 bucket and uploads the data files.
"""

import sys
import os

import boto3
from botocore.exceptions import ClientError

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project configuration
from config.aws_config import S3_CONFIG, AWS_REGION
from utils.logging_config import configure_logger

# Configure logger
logger = configure_logger(__name__)


def create_s3_bucket(bucket_name: str, region=AWS_REGION) -> bool:
    """
    Create an S3 bucket in the specified region.

    Args:
        bucket_name (str): Name of the bucket to create
        region (str): AWS region where the bucket will be created

    Returns:
        bool: True if bucket was created or already exists, False on error
    """
    s3_client = boto3.client("s3", region_name=region)

    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {"LocationConstraint": region}
            s3_client.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration=location
            )
        logger.info(f"S3 bucket '{bucket_name}' created successfully")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logger.info(f"S3 bucket '{bucket_name}' already exists")
            return True
        else:
            logger.error(f"Error creating S3 bucket '{bucket_name}': {e}")
            return False


def upload_file_to_s3(file_path: str, bucket_name: str, object_name=None) -> bool:
    """
    Upload a file to an S3 bucket.

    Args:
        file_path (str): Path to the file to upload
        bucket_name (str): Name of the bucket to upload to
        object_name (str): S3 object name. If not specified, file_path is used

    Returns:
        bool: True if file was uploaded, False on error
    """
    if object_name is None:
        object_name = os.path.basename(file_path)

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info(f"Uploaded '{file_path}' to '{bucket_name}/{object_name}'")
        return True
    except ClientError as e:
        logger.error(
            f"Error uploading '{file_path}' to '{bucket_name}/{object_name}': {e}"
        )
        return False


def upload_data_to_s3(data_dir: str, bucket_name: str, prefix: str) -> bool:
    """
    Upload all data files from a directory to an S3 bucket.

    Args:
        data_dir (str): Directory containing data files
        bucket_name (str): Name of the bucket to upload to
        prefix (str): Prefix to add to the S3 object names

    Returns:
        bool: True if all files were uploaded, False if any upload failed
    """
    success = True

    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                dataset_name = os.path.splitext(file)[0]
                object_name = f"{prefix}{dataset_name}/{file}"

                if not upload_file_to_s3(file_path, bucket_name, object_name):
                    success = False

    return success


def main() -> None:
    """Main function to set up the AWS environment."""
    logger.info("Starting AWS environment setup")

    # Create S3 bucket
    if not create_s3_bucket(S3_CONFIG["bucket_name"]):
        logger.error("Failed to create S3 bucket. Exiting.")
        return

    # Upload data files to S3
    data_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
    )
    if not upload_data_to_s3(
        data_dir, S3_CONFIG["bucket_name"], S3_CONFIG["raw_data_prefix"]
    ):
        logger.warning("Some data files could not be uploaded to S3")

    logger.info("AWS environment setup completed successfully")


if __name__ == "__main__":
    main()
