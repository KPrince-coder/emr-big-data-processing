#!/usr/bin/env python3
"""
AWS Glue Crawler Setup Script

This script automates the setup and configuration of AWS Glue crawlers for data cataloging.
It creates necessary Glue databases and crawlers to scan and catalog data stored in S3 buckets.
The crawlers automatically detect schema and create metadata tables in the AWS Glue Data Catalog,
making the data readily available for analytics and querying.

Features:
- Creates Glue databases if they don't exist
- Configures and deploys Glue crawlers for different data sources
- Handles crawler scheduling and permissions
- Provides logging and error handling

Dependencies:
- boto3: AWS SDK for Python
- AWS credentials configured with appropriate permissions
"""

import os
import sys
import time

import boto3

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project configuration
from config.aws_config import GLUE_CONFIG, AWS_REGION
from utils.logging_config import configure_logger

# Configure logger
logger = configure_logger(__name__)


def create_glue_database(database_name: str) -> bool:
    """
    Create a Glue database if it doesn't exist.

    Args:
        database_name (str): Name of the database to create

    Returns:
        bool: True if database was created or already exists, False on error
    """
    glue_client = boto3.client("glue", region_name=AWS_REGION)

    try:
        # Check if database already exists
        glue_client.get_database(Name=database_name)
        logger.info(f"Glue database '{database_name}' already exists")
        return True
    except glue_client.exceptions.EntityNotFoundException:
        # Create the database
        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": database_name,
                    "Description": "Database for car rental data analysis",
                }
            )
            logger.info(f"Glue database '{database_name}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating Glue database '{database_name}': {e}")
            return False
    except Exception as e:
        logger.error(f"Error checking Glue database '{database_name}': {e}")
        return False


def create_glue_crawler(
    crawler_name: str, role: str, database_name: str, s3_target_path: str
) -> bool:
    """
    Create a Glue crawler if it doesn't exist.

    Args:
        crawler_name (str): Name of the crawler to create
        role (str): IAM role ARN for the crawler
        database_name (str): Name of the database to use
        s3_target_path (str): S3 path to crawl

    Returns:
        bool: True if crawler was created or already exists, False on error
    """
    glue_client = boto3.client("glue", region_name=AWS_REGION)

    try:
        # Check if crawler already exists
        glue_client.get_crawler(Name=crawler_name)
        logger.info(f"Glue crawler '{crawler_name}' already exists")
        return True
    except glue_client.exceptions.EntityNotFoundException:
        # Create the crawler
        try:
            glue_client.create_crawler(
                Name=crawler_name,
                Role=role,
                DatabaseName=database_name,
                Targets={"S3Targets": [{"Path": s3_target_path}]},
                SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "DELETE_FROM_DATABASE",
                },
                Configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}',
                RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
            )
            logger.info(f"Glue crawler '{crawler_name}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating Glue crawler '{crawler_name}': {e}")
            return False
    except Exception as e:
        logger.error(f"Error checking Glue crawler '{crawler_name}': {e}")
        return False


def start_crawler(crawler_name: str) -> bool:
    """
    Start a Glue crawler.

    Args:
        crawler_name (str): Name of the crawler to start

    Returns:
        bool: True if crawler was started, False on error
    """
    glue_client = boto3.client("glue", region_name=AWS_REGION)

    try:
        # Check if crawler is already running
        response = glue_client.get_crawler(Name=crawler_name)
        crawler_state = response["Crawler"]["State"]

        if crawler_state == "RUNNING":
            logger.info(f"Glue crawler '{crawler_name}' is already running")
            return True

        # Start the crawler
        glue_client.start_crawler(Name=crawler_name)
        logger.info(f"Glue crawler '{crawler_name}' started successfully")
        return True
    except Exception as e:
        logger.error(f"Error starting Glue crawler '{crawler_name}': {e}")
        return False


def wait_for_crawler_completion(crawler_name: str, timeout_seconds=300) -> bool:
    """
    Wait for a Glue crawler to complete.

    Args:
        crawler_name (str): Name of the crawler to wait for
        timeout_seconds (int): Maximum time to wait in seconds

    Returns:
        bool: True if crawler completed successfully, False otherwise
    """
    glue_client = boto3.client("glue", region_name=AWS_REGION)

    logger.info(f"Waiting for Glue crawler '{crawler_name}' to complete...")

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            response = glue_client.get_crawler(Name=crawler_name)
            crawler_state = response["Crawler"]["State"]

            if crawler_state == "READY":
                last_crawl = response["Crawler"].get("LastCrawl", {})
                status = last_crawl.get("Status")

                if status == "SUCCEEDED":
                    logger.info(f"Glue crawler '{crawler_name}' completed successfully")
                    return True
                elif status in ["FAILED", "CANCELLED"]:
                    logger.error(f"Glue crawler '{crawler_name}' {status.lower()}")
                    return False

            logger.info(f"Glue crawler '{crawler_name}' status: {crawler_state}")
            time.sleep(30)  # Wait for 30 seconds before checking again

        except Exception as e:
            logger.error(f"Error checking Glue crawler '{crawler_name}' status: {e}")
            return False

    logger.error(f"Timed out waiting for Glue crawler '{crawler_name}' to complete")
    return False


def main() -> None:
    """Main function to set up Glue crawlers."""
    logger.info("Starting Glue crawler setup")

    # Create Glue database
    if not create_glue_database(GLUE_CONFIG["database_name"]):
        logger.error("Failed to create Glue database. Exiting.")
        return

    # Create and start crawlers for each table
    for table_name, table_config in GLUE_CONFIG["tables"].items():
        crawler_name = f"{GLUE_CONFIG['crawler_name_prefix']}{table_name}"
        s3_target_path = table_config["location"]

        # Create crawler
        if not create_glue_crawler(
            crawler_name,
            GLUE_CONFIG["crawler_role"],
            GLUE_CONFIG["database_name"],
            s3_target_path,
        ):
            logger.error(f"Failed to create Glue crawler for {table_name}. Skipping.")
            continue

        # Start crawler
        if not start_crawler(crawler_name):
            logger.error(f"Failed to start Glue crawler for {table_name}. Skipping.")
            continue

        # Wait for crawler to complete
        wait_for_crawler_completion(crawler_name)

    logger.info("Glue crawler setup completed")


if __name__ == "__main__":
    main()
