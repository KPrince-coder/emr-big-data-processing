#!/usr/bin/env python3
"""
AWS Environment Setup Script

This script sets up the AWS environment for the Big Data Processing with EMR project.
It checks if the S3 bucket exists and uploads all data files and scripts.

Usage:
    python setup_aws_environment.py [--bucket-name your-bucket-name] [--region your-region]
"""

import sys
import os
import argparse

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project configuration
from config.aws_config import S3_CONFIG, AWS_REGION
from utils.logging_config import configure_logger
from utils.s3_utils import (
    check_s3_bucket_exists,
    upload_data_files,
    upload_spark_scripts,
    upload_data_to_s3,
    upload_utils_modules,
)

# Configure logger
logger = configure_logger(__name__)


def main() -> None:
    """Main function to set up the AWS environment."""
    parser = argparse.ArgumentParser(description="Set up AWS environment")
    parser.add_argument(
        "--bucket-name",
        default=S3_CONFIG["bucket_name"],
        help=f"Name of the S3 bucket (default: {S3_CONFIG['bucket_name']})",
    )
    parser.add_argument(
        "--region",
        default=AWS_REGION,
        help="AWS region (default: use AWS CLI configuration)",
    )

    args = parser.parse_args()

    logger.info(f"S3_CONFIG bucket name: {S3_CONFIG['bucket_name']}")
    logger.info(f"Args bucket name: {args.bucket_name}")

    logger.info("Starting AWS environment setup")

    # Check if S3 bucket exists
    if not check_s3_bucket_exists(args.bucket_name, args.region):
        logger.error(
            f"S3 bucket '{args.bucket_name}' does not exist or is not accessible. Please run setup_s3_bucket.py first."
        )
        sys.exit(1)

    # Upload data files to S3
    data_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
    )
    logger.info(f"Uploading data files from '{data_dir}'")
    if not upload_data_files(args.bucket_name, data_dir, args.region):
        logger.warning("Some data files could not be uploaded")

    # Also upload any loose CSV files using the generic upload method
    if not upload_data_to_s3(
        data_dir, args.bucket_name, S3_CONFIG["raw_data_prefix"], args.region
    ):
        logger.warning("Some additional data files could not be uploaded to S3")

    # Upload Spark scripts
    spark_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "spark"
    )
    logger.info(f"Uploading Spark scripts from '{spark_dir}'")
    if not upload_spark_scripts(args.bucket_name, spark_dir, args.region):
        logger.warning("Some Spark scripts could not be uploaded")

    # Upload utility modules
    utils_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils"
    )
    logger.info(f"Uploading utility modules from '{utils_dir}'")
    if not upload_utils_modules(args.bucket_name, utils_dir, args.region):
        logger.warning("Some utility modules could not be uploaded")

    logger.info("AWS environment setup completed successfully")


if __name__ == "__main__":
    main()
