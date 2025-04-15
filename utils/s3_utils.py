"""S3 Utility Functions

This module provides utility functions for working with S3 buckets and objects.
It includes functions for creating buckets, checking if buckets exist, uploading files, etc.
"""

import os
import glob

import boto3
from botocore.exceptions import ClientError

from config.aws_config import AWS_REGION, S3_CONFIG
from utils.logging_config import configure_logger

# Configure logger
logger = configure_logger(__name__)


def check_s3_bucket_exists(bucket_name: str, region=AWS_REGION) -> bool:
    """
    Check if an S3 bucket exists and is accessible.

    Args:
        bucket_name (str): Name of the bucket to check
        region (str): AWS region where the bucket should be located

    Returns:
        bool: True if bucket exists and is accessible, False otherwise
    """
    s3_client = boto3.client("s3", region_name=region)

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"S3 bucket '{bucket_name}' exists and is accessible")
        return True
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            logger.info(f"S3 bucket '{bucket_name}' does not exist")
        elif error_code == 403:
            logger.error(
                f"S3 bucket '{bucket_name}' exists but you don't have access to it"
            )
        else:
            logger.error(f"Error checking S3 bucket '{bucket_name}': {e}")
        return False


def create_s3_bucket(bucket_name: str, region=AWS_REGION) -> bool:
    """
    Create an S3 bucket in the specified region.

    Args:
        bucket_name (str): Name of the bucket to create
        region (str): AWS region where the bucket will be created

    Returns:
        bool: True if bucket was created or already exists, False on error
    """
    # First check if bucket already exists
    if check_s3_bucket_exists(bucket_name, region):
        return True

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
            logger.info(f"S3 bucket '{bucket_name}' already exists and is owned by you")
            return True
        elif e.response["Error"]["Code"] == "BucketAlreadyExists":
            logger.error(
                f"S3 bucket '{bucket_name}' already exists but is owned by another account"
            )
            return False
        else:
            logger.error(f"Error creating S3 bucket '{bucket_name}': {e}")
            return False


def create_folder_structure(bucket_name: str, region=AWS_REGION) -> bool:
    """
    Create the folder structure in the S3 bucket.

    Args:
        bucket_name (str): Name of the bucket
        region (str): AWS region

    Returns:
        bool: True if folder structure was created successfully, False on error
    """
    try:
        s3_client = boto3.client("s3", region_name=region)

        # Get folder structure from S3_CONFIG
        folders = list(S3_CONFIG["folders"].values())

        # Create each folder (S3 doesn't actually have folders, but we can create empty objects with folder names)
        for folder in folders:
            s3_client.put_object(Bucket=bucket_name, Key=folder)
            logger.info(f"Created folder '{folder}' in bucket '{bucket_name}'")

        return True
    except ClientError as e:
        logger.error(f"Error creating folder structure in bucket '{bucket_name}': {e}")
        return False


def upload_file_to_s3(
    file_path: str, bucket_name: str, object_name=None, region=AWS_REGION
) -> bool:
    """
    Upload a file to an S3 bucket.

    Args:
        file_path (str): Path to the file to upload
        bucket_name (str): Name of the bucket to upload to
        object_name (str): S3 object name. If not specified, file_path is used
        region (str): AWS region

    Returns:
        bool: True if file was uploaded, False on error
    """
    if object_name is None:
        object_name = os.path.basename(file_path)

    s3_client = boto3.client("s3", region_name=region)

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info(f"Uploaded '{file_path}' to '{bucket_name}/{object_name}'")
        return True
    except ClientError as e:
        logger.error(
            f"Error uploading '{file_path}' to '{bucket_name}/{object_name}': {e}"
        )
        return False
    except FileNotFoundError:
        logger.error(f"File '{file_path}' not found")
        return False


def upload_data_files(bucket_name: str, data_dir: str, region=AWS_REGION) -> bool:
    """
    Upload data files to the S3 bucket.

    Args:
        bucket_name (str): Name of the bucket
        data_dir (str): Directory containing data files
        region (str): AWS region

    Returns:
        bool: True if all files were uploaded successfully, False on error
    """
    try:
        # Check if data directory exists
        if not os.path.exists(data_dir):
            logger.error(f"Data directory '{data_dir}' does not exist")
            return False

        # Define dataset folders and their S3 destinations from S3_CONFIG
        datasets = {
            "vehicles": S3_CONFIG["folders"]["vehicles"],
            "users": S3_CONFIG["folders"]["users"],
            "locations": S3_CONFIG["folders"]["locations"],
            "rental_transactions": S3_CONFIG["folders"]["rental_transactions"],
        }

        success = True

        # Upload each dataset
        for dataset, s3_prefix in datasets.items():
            # Look for CSV files in the dataset directory
            dataset_dir = os.path.join(data_dir, dataset)
            if os.path.exists(dataset_dir):
                csv_files = glob.glob(os.path.join(dataset_dir, "*.csv"))

                if not csv_files:
                    # If no CSV files in subdirectory, look for a CSV file with the dataset name
                    csv_file = os.path.join(data_dir, f"{dataset}.csv")
                    if os.path.exists(csv_file):
                        csv_files = [csv_file]

                for csv_file in csv_files:
                    object_name = s3_prefix + os.path.basename(csv_file)
                    if not upload_file_to_s3(
                        csv_file, bucket_name, object_name, region
                    ):
                        success = False
            else:
                logger.warning(f"Dataset directory '{dataset_dir}' does not exist")

        return success
    except Exception as e:
        logger.error(f"Error uploading data files to bucket '{bucket_name}': {e}")
        return False


def upload_spark_scripts(bucket_name: str, spark_dir: str, region=AWS_REGION) -> bool:
    """
    Upload Spark scripts to the S3 bucket.

    Args:
        bucket_name (str): Name of the bucket
        spark_dir (str): Directory containing Spark scripts
        region (str): AWS region

    Returns:
        bool: True if all scripts were uploaded successfully, False on error
    """
    try:
        # Check if spark directory exists
        if not os.path.exists(spark_dir):
            logger.error(f"Spark directory '{spark_dir}' does not exist")
            return False

        success = True

        # Upload each Python script in the spark directory
        for script_file in glob.glob(os.path.join(spark_dir, "*.py")):
            object_name = S3_CONFIG["folders"]["scripts"] + os.path.basename(
                script_file
            )
            if not upload_file_to_s3(script_file, bucket_name, object_name, region):
                success = False

        return success
    except Exception as e:
        logger.error(f"Error uploading Spark scripts to bucket '{bucket_name}': {e}")
        return False


def upload_data_to_s3(
    data_dir: str, bucket_name: str, prefix: str, region=AWS_REGION
) -> bool:
    """
    Upload all data files from a directory to an S3 bucket.

    Args:
        data_dir (str): Directory containing data files
        bucket_name (str): Name of the bucket to upload to
        prefix (str): Prefix to add to the S3 object names
        region (str): AWS region

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

                if not upload_file_to_s3(file_path, bucket_name, object_name, region):
                    success = False

    return success


def upload_utils_modules(bucket_name: str, utils_dir: str, region=AWS_REGION) -> bool:
    """
    Upload utility Python modules to the S3 bucket.

    Args:
        bucket_name (str): Name of the bucket
        utils_dir (str): Directory containing utility modules
        region (str): AWS region

    Returns:
        bool: True if all modules were uploaded successfully, False on error
    """
    try:
        # Check if utils directory exists
        if not os.path.exists(utils_dir):
            logger.error(f"Utils directory '{utils_dir}' does not exist")
            return False

        success = True

        # Create utils directory in scripts folder
        s3_client = boto3.client("s3", region_name=region)
        utils_s3_prefix = S3_CONFIG["folders"]["scripts"] + "utils/"
        s3_client.put_object(Bucket=bucket_name, Key=utils_s3_prefix)
        logger.info(f"Created folder '{utils_s3_prefix}' in bucket '{bucket_name}'")

        # Upload each Python module in the utils directory
        for module_file in glob.glob(os.path.join(utils_dir, "*.py")):
            # Skip __pycache__ and other non-Python files
            if (
                os.path.basename(module_file).startswith("__")
                and os.path.basename(module_file) != "__init__.py"
            ):
                continue

            object_name = utils_s3_prefix + os.path.basename(module_file)
            if not upload_file_to_s3(module_file, bucket_name, object_name, region):
                success = False

        return success
    except Exception as e:
        logger.error(f"Error uploading utility modules to bucket '{bucket_name}': {e}")
        return False
