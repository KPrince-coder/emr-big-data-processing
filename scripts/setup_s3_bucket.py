#!/usr/bin/env python3
"""
S3 Bucket Setup Script

This script creates an S3 bucket and loads data into the appropriate subfolders.
It sets up the initial storage structure for the EMR data processing pipeline.

Usage:
    python setup_s3_bucket.py [--bucket-name your-bucket-name] [--region your-region]
"""

import os
import sys
import argparse

# Import project configuration
from config.aws_config import AWS_REGION, S3_CONFIG
from utils.logging_config import configure_logger
from utils.s3_utils import (
    create_s3_bucket,
    create_folder_structure,
    upload_data_files,
    upload_spark_scripts,
)

# Configure logger
logger = configure_logger(__name__)


def main() -> None:
    """Main function to set up the S3 bucket and load data."""
    parser = argparse.ArgumentParser(description="Set up S3 bucket and load data")
    parser.add_argument(
        "--bucket-name",
        default=S3_CONFIG["bucket_name"],
        help=f"Name of the S3 bucket to create (default: {S3_CONFIG['bucket_name']})",
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

    # Debug: Print the bucket name from S3_CONFIG and from args
    logger.info(f"S3_CONFIG bucket name: {S3_CONFIG['bucket_name']}")
    logger.info(f"Args bucket name: {args.bucket_name}")

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
