"""
AWS Lambda Function to Start AWS Glue Crawlers (Step Functions Trigger)

This Lambda function is triggered by an AWS Step Functions workflow to start AWS Glue crawlers.
The function accepts a list of crawler names and their associated database, validates the input parameters,
checks if the crawlers are already running, and initiates them if they are not.

Key Features:
- Input validation for required parameters
- Checks crawler state before starting
- Handles multiple crawlers in a single invocation
- Error handling and logging
- Returns detailed execution status

Required Input Parameters:
- database: The name of the Glue database
- crawlers: List of crawler names to start

Returns:
- Dictionary containing execution status and results for each crawler
"""

import boto3  # Import boto3 module
import time
from typing import Dict, Any  # Import Dict and Any type hints
from utils.logging_config import configure_logger

# Remove dependency on mypy_boto3_glue
# Instead of using GlueClient type hint, use Any
GlueClient = Any  # Type alias for GlueClient

# Configure logger
logger = configure_logger(__name__)


def check_crawler_status(glue_client: GlueClient, crawler_name: str) -> bool:
    """
    Helper function to check crawler status.

    Args:
        glue_client (GlueClient): Boto3 Glue client instance
        crawler_name (str): Name of the crawler to check

    Returns:
        bool: True if crawler is not running, False if crawler is still running
    """
    try:
        response = glue_client.get_crawler(Name=crawler_name)
        crawler_state = response["Crawler"]["State"]

        if crawler_state == "RUNNING":
            logger.info(f"Crawler '{crawler_name}' is still running")
            return False

    except Exception as e:
        logger.error(f"Error checking crawler '{crawler_name}' status: {str(e)}")
    return True


def start_crawler(
    glue_client: GlueClient, crawler_name: str, results: Dict[str, str]
) -> None:
    """
    Helper function to start a crawler.

    Args:
        glue_client (GlueClient): Boto3 Glue client instance
        crawler_name (str): Name of the crawler to start
        results (Dict[str, str]): Dictionary to store crawler execution results

    Returns:
        None
    """
    try:
        response = glue_client.get_crawler(Name=crawler_name)
        crawler_state = response["Crawler"]["State"]

        if crawler_state == "RUNNING":
            logger.info(f"Crawler '{crawler_name}' is already running")
            results[crawler_name] = "ALREADY_RUNNING"
            return

        glue_client.start_crawler(Name=crawler_name)
        logger.info(f"Started crawler '{crawler_name}'")
        results[crawler_name] = "STARTED"

    except glue_client.exceptions.EntityNotFoundException:
        error_message = f"Crawler '{crawler_name}' not found"
        logger.error(error_message)
        results[crawler_name] = "NOT_FOUND"

    except Exception as e:
        error_message = f"Error starting crawler '{crawler_name}': {str(e)}"
        logger.error(error_message)
        results[crawler_name] = "ERROR"


def get_final_status(
    glue_client: GlueClient, crawler_name: str, results: Dict[str, str]
) -> None:
    """
    Helper function to get final crawler status.

    Args:
        glue_client (GlueClient): Boto3 Glue client instance
        crawler_name (str): Name of the crawler to check status
        results (Dict[str, str]): Dictionary to store crawler execution results

    Returns:
        None
    """
    try:
        response = glue_client.get_crawler(Name=crawler_name)
        last_crawl = response["Crawler"].get("LastCrawl", {})
        status = last_crawl.get("Status")

        results[crawler_name] = f"COMPLETED: {status}"
        logger.info(f"Crawler '{crawler_name}' completed with status: {status}")

    except Exception as e:
        logger.error(
            f"Error checking final status of crawler '{crawler_name}': {str(e)}"
        )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function handler to start AWS Glue crawlers.

    Args:
        event (Dict[str, Any]): Event data from Step Functions containing:
            - database (str): The name of the Glue database
            - crawlers (List[str]): List of crawler names to start
        context (Any): Lambda context object

    Returns:
        Dict[str, Any]: Result of the operation containing:
            - statusCode (int): HTTP status code
            - database (str): The name of the database processed
            - crawlers (Dict[str, str]): Status of each crawler
            - error (str, optional): Error message if operation failed
    """
    logger.info("Starting AWS Glue crawlers")

    database = event.get("database")
    crawlers = event.get("crawlers", [])

    if not database or not crawlers:
        error_message = "Missing required parameters: database and/or crawlers"
        logger.error(error_message)
        return {"statusCode": 400, "error": error_message}

    glue_client = boto3.client("glue")
    results: Dict[str, str] = {}

    # Start each crawler
    for crawler_name in crawlers:
        start_crawler(glue_client, crawler_name, results)

    # Wait for all crawlers to complete (with timeout)
    timeout_seconds = 900  # 15 minutes
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        all_completed = all(
            check_crawler_status(glue_client, name) for name in crawlers
        )
        if all_completed:
            break
        time.sleep(30)

    # Check final status of all crawlers
    for crawler_name in crawlers:
        get_final_status(glue_client, crawler_name, results)

    return {"statusCode": 200, "database": database, "crawlers": results}
