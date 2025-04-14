#!/usr/bin/env python3
"""
EMR Cluster Creation Script

This script creates an EMR cluster using the configuration defined in the project.
"""

import os
import json
import boto3
import sys
import time

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project configuration
from config.aws_config import EMR_CONFIG, AWS_REGION
from utils.logging_config import configure_logger

# Configure logger
logger = configure_logger(__name__)


def create_emr_cluster():
    """
    Create an EMR cluster using the configuration defined in config/emr_config.json.

    Returns:
        str: The cluster ID if successful, None otherwise
    """
    emr_client = boto3.client("emr", region_name=AWS_REGION)

    # Load EMR configuration from JSON file
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "emr_config.json",
    )

    with open(config_path, "r") as f:
        emr_config = json.load(f)

    try:
        response = emr_client.run_job_flow(**emr_config)
        cluster_id = response["JobFlowId"]
        logger.info(
            f"EMR cluster '{EMR_CONFIG['name']}' (ID: {cluster_id}) is being created"
        )
        return cluster_id
    except Exception as e:
        logger.error(f"Error creating EMR cluster: {e}")
        return None


def wait_for_cluster_creation(cluster_id):
    """
    Wait for the EMR cluster to be created and ready.

    Args:
        cluster_id (str): The ID of the cluster to wait for

    Returns:
        bool: True if the cluster is ready, False otherwise
    """
    emr_client = boto3.client("emr", region_name=AWS_REGION)

    logger.info(f"Waiting for EMR cluster {cluster_id} to be ready...")

    while True:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        status = response["Cluster"]["Status"]["State"]

        if status == "WAITING":
            logger.info(f"EMR cluster {cluster_id} is now ready")
            return True
        elif status in ["STARTING", "BOOTSTRAPPING", "RUNNING"]:
            logger.info(f"EMR cluster status: {status}")
            time.sleep(30)  # Wait for 30 seconds before checking again
        else:
            logger.error(f"EMR cluster creation failed. Status: {status}")
            if "StateChangeReason" in response["Cluster"]["Status"]:
                reason = response["Cluster"]["Status"]["StateChangeReason"]
                logger.error(f"Reason: {reason.get('Message', 'Unknown')}")
            return False


def main():
    """Main function to create an EMR cluster."""
    logger.info("Starting EMR cluster creation")

    cluster_id = create_emr_cluster()
    if not cluster_id:
        logger.error("Failed to create EMR cluster. Exiting.")
        return

    if wait_for_cluster_creation(cluster_id):
        logger.info(f"EMR cluster {cluster_id} is ready for use")

        # Save the cluster ID to a file for later use
        with open("emr_cluster_id.txt", "w") as f:
            f.write(cluster_id)
        logger.info(f"Cluster ID saved to emr_cluster_id.txt")
    else:
        logger.error("EMR cluster creation failed or timed out")


if __name__ == "__main__":
    main()
