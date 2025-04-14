#!/usr/bin/env python3
"""
AWS Environment Setup Script

This script sets up the AWS environment for the Big Data Processing with EMR project.
It verifies the existence of the S3 bucket and uploads the data files.

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
from utils.s3_utils import create_s3_bucket, upload_data_to_s3

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

    # Create S3 bucket
    if not create_s3_bucket(args.bucket_name, args.region):
        logger.error("Failed to create S3 bucket. Exiting.")
        sys.exit(1)

    # Upload data files to S3
    data_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
    )
    if not upload_data_to_s3(
        data_dir, args.bucket_name, S3_CONFIG["raw_data_prefix"], args.region
    ):
        logger.warning("Some data files could not be uploaded to S3")

    logger.info("AWS environment setup completed successfully")


if __name__ == "__main__":
    main()
