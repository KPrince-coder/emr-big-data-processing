#!/usr/bin/env python3
"""
S3 Bucket Setup Script

This script creates an S3 bucket and loads data into the appropriate subfolders.
It sets up the initial storage structure for the EMR data processing pipeline.

Usage:
    python setup_s3_bucket.py --bucket-name your-bucket-name [--region your-region]
"""

import os
import sys

import argparse

import boto3
from botocore.exceptions import ClientError

import glob

# Import project configuration
from config.aws_config import AWS_REGION, S3_CONFIG
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
    try:
        s3_client = boto3.client("s3", region_name=region)

        # Set up bucket configuration
        if region == "us-east-1":
            # 'us-east-1' is the default region, and doesn't use LocationConstraint
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
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


def main() -> None:
    """Main function to set up the S3 bucket and load data."""
    parser = argparse.ArgumentParser(description="Set up S3 bucket and load data")
    parser.add_argument(
        "--bucket-name", required=True, help="Name of the S3 bucket to create"
    )
    parser.add_argument(
        "--region",
        default=AWS_REGION,
        help="AWS region (default: use AWS CLI configuration)",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing data files (default: data)",
    )
    parser.add_argument(
        "--spark-dir",
        default="spark",
        help="Directory containing Spark scripts (default: spark)",
    )

    args = parser.parse_args()

    logger.info(f"Setting up S3 bucket '{args.bucket_name}'")

    # Create the S3 bucket
    if not create_s3_bucket(args.bucket_name, args.region):
        logger.error("Failed to create S3 bucket. Exiting.")
        sys.exit(1)

    # Create the folder structure
    if not create_folder_structure(args.bucket_name, args.region):
        logger.error("Failed to create folder structure. Exiting.")
        sys.exit(1)

    # Upload data files
    data_dir = os.path.abspath(args.data_dir)
    logger.info(f"Uploading data files from '{data_dir}'")
    if not upload_data_files(args.bucket_name, data_dir, args.region):
        logger.warning("Some data files could not be uploaded")

    # Upload Spark scripts
    spark_dir = os.path.abspath(args.spark_dir)
    logger.info(f"Uploading Spark scripts from '{spark_dir}'")
    if not upload_spark_scripts(args.bucket_name, spark_dir, args.region):
        logger.warning("Some Spark scripts could not be uploaded")

    logger.info(f"S3 bucket '{args.bucket_name}' setup completed successfully")


if __name__ == "__main__":
    main()
