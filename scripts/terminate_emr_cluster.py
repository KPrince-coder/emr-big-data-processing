#!/usr/bin/env python3
"""
Terminate EMR Cluster Script

This script terminates an EMR cluster.
"""

import os
import boto3
import sys
import time

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project configuration
from config.aws_config import AWS_REGION
from utils.logging_config import configure_logger

# Configure logger
logger = configure_logger(__name__)


def terminate_emr_cluster(cluster_id):
    """
    Terminate an EMR cluster.

    Args:
        cluster_id (str): The ID of the cluster to terminate

    Returns:
        bool: True if the cluster was terminated, False on error
    """
    emr_client = boto3.client("emr", region_name=AWS_REGION)

    try:
        # Check if the cluster exists and is not already terminated
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        state = response["Cluster"]["Status"]["State"]

        if state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
            logger.info(f"EMR cluster {cluster_id} is already terminated")
            return True

        # Terminate the cluster
        emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info(f"Termination request sent for EMR cluster {cluster_id}")
        return True
    except Exception as e:
        logger.error(f"Error terminating EMR cluster {cluster_id}: {e}")
        return False


def wait_for_cluster_termination(cluster_id, timeout_seconds=600):
    """
    Wait for an EMR cluster to be terminated.

    Args:
        cluster_id (str): The ID of the cluster to wait for
        timeout_seconds (int): Maximum time to wait in seconds

    Returns:
        bool: True if the cluster was terminated, False otherwise
    """
    emr_client = boto3.client("emr", region_name=AWS_REGION)

    logger.info(f"Waiting for EMR cluster {cluster_id} to terminate...")

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            response = emr_client.describe_cluster(ClusterId=cluster_id)
            state = response["Cluster"]["Status"]["State"]

            if state == "TERMINATED":
                logger.info(f"EMR cluster {cluster_id} has been terminated")
                return True
            elif state == "TERMINATED_WITH_ERRORS":
                logger.warning(f"EMR cluster {cluster_id} terminated with errors")
                return True
            else:
                logger.info(f"EMR cluster {cluster_id} is in state {state}")
                time.sleep(30)  # Wait for 30 seconds before checking again
        except Exception as e:
            logger.error(f"Error checking EMR cluster {cluster_id} status: {e}")
            return False

    logger.error(f"Timed out waiting for EMR cluster {cluster_id} to terminate")
    return False


def main():
    """Main function to terminate an EMR cluster."""
    # Check if the required arguments are provided
    if len(sys.argv) != 2:
        print("Usage: terminate_emr_cluster.py <cluster_id>")
        sys.exit(1)

    cluster_id = sys.argv[1]

    logger.info(f"Terminating EMR cluster {cluster_id}")

    if not terminate_emr_cluster(cluster_id):
        logger.error(f"Failed to terminate EMR cluster {cluster_id}")
        sys.exit(1)

    if not wait_for_cluster_termination(cluster_id):
        logger.warning(f"Could not confirm termination of EMR cluster {cluster_id}")

    logger.info(f"EMR cluster {cluster_id} termination process completed")


if __name__ == "__main__":
    main()
