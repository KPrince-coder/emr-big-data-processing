#!/usr/bin/env python3
"""
Execute AWS Step Functions Workflow

This script executes the Step Functions workflow for the data pipeline.
"""

import os
import json
import boto3
import sys
import time
import argparse

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project configuration
from config.aws_config import (
    STEP_FUNCTIONS_CONFIG,
    EMR_CONFIG,
    S3_CONFIG,
    GLUE_CONFIG,
    IAM_ROLES,
    AWS_REGION,
)
from utils.s3_path_utils import get_logs_path
from utils.logging_config import configure_logger

logger = configure_logger(__name__)


def get_state_machine_arn(state_machine_name: str) -> str | None:
    """
    Get the ARN of a Step Functions state machine by name.

    Args:
        state_machine_name (str): Name of the state machine

    Returns:
        str: The ARN of the state machine if found, None otherwise
    """
    sfn_client = boto3.client("stepfunctions", region_name=AWS_REGION)

    try:
        response = sfn_client.list_state_machines()
        for state_machine in response["stateMachines"]:
            if state_machine["name"] == state_machine_name:
                return state_machine["stateMachineArn"]

        logger.error(f"State machine '{state_machine_name}' not found")
        return None
    except Exception as e:
        logger.error(f"Error getting state machine ARN: {e}")
        return None


def start_execution(
    state_machine_arn: str, execution_name=None, input_data=None
) -> str | None:
    """
    Start an execution of a Step Functions state machine.

    Args:
        state_machine_arn (str): ARN of the state machine
        execution_name (str): Name for the execution (optional)
        input_data (dict): Input data for the execution (optional)

    Returns:
        str: The ARN of the execution if successful, None otherwise
    """
    sfn_client = boto3.client("stepfunctions", region_name=AWS_REGION)

    # Generate a default execution name if not provided
    if not execution_name:
        execution_name = f"Execution-{int(time.time())}"

    # Use default input if not provided
    if not input_data:
        input_data = {}

    try:
        response = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(input_data),
        )

        execution_arn = response["executionArn"]
        logger.info(f"Started execution '{execution_name}' (ARN: {execution_arn})")
        return execution_arn
    except Exception as e:
        logger.error(f"Error starting execution: {e}")
        return None


def wait_for_execution_completion(
    execution_arn: str, timeout_seconds: int = 3600
) -> bool:
    """
    Wait for a Step Functions execution to complete.

    Args:
        execution_arn (str): ARN of the execution to wait for
        timeout_seconds (int): Maximum time to wait in seconds

    Returns:
        bool: True if the execution completed successfully, False otherwise
    """
    sfn_client = boto3.client("stepfunctions", region_name=AWS_REGION)

    logger.info(f"Waiting for execution {execution_arn} to complete...")

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            response = sfn_client.describe_execution(executionArn=execution_arn)
            status = response["status"]

            if status == "SUCCEEDED":
                logger.info(f"Execution {execution_arn} completed successfully")
                return True
            elif status in ["RUNNING"]:
                logger.info(f"Execution {execution_arn} is {status}")
                time.sleep(30)  # Wait for 30 seconds before checking again
            else:
                logger.error(f"Execution {execution_arn} failed with status {status}")
                if "error" in response:
                    logger.error(f"Error: {response['error']}")
                if "cause" in response:
                    logger.error(f"Cause: {response['cause']}")
                return False
        except Exception as e:
            logger.error(f"Error checking execution status: {e}")
            return False

    logger.error(f"Timed out waiting for execution {execution_arn} to complete")
    return False


def main() -> None:
    """Main function to execute the Step Functions workflow."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Execute Step Functions workflow")
    parser.add_argument(
        "--environment",
        default="prod",
        choices=["dev", "test", "prod"],
        help="Environment to run the workflow in (default: prod)",
    )
    args = parser.parse_args()

    logger.info(
        f"Starting Step Functions workflow execution in {args.environment} environment"
    )

    # Get the state machine ARN
    state_machine_arn = get_state_machine_arn(
        STEP_FUNCTIONS_CONFIG["state_machine_name"]
    )
    if not state_machine_arn:
        logger.error("Failed to get state machine ARN. Exiting.")
        return

    # Prepare input data with configuration values
    input_data = {
        "emr_config": {
            "cluster_name": EMR_CONFIG["name"],
            "release_label": EMR_CONFIG["release_label"],
            "master_instance_type": EMR_CONFIG["master_instance_type"],
            "core_instance_type": EMR_CONFIG["core_instance_type"],
            "core_instance_count": EMR_CONFIG["core_instance_count"],
        },
        "s3_config": {
            "bucket_name": S3_CONFIG["bucket_name"],
            "log_uri": get_logs_path("emr", S3_CONFIG, S3_CONFIG["bucket_name"]),
        },
        "glue_config": {"database_name": GLUE_CONFIG["database_name"]},
        "iam_roles": {
            "emr_service_role": IAM_ROLES["emr_service_role"],
            "emr_ec2_instance_profile": IAM_ROLES["emr_ec2_instance_profile"],
        },
        "environment": args.environment,
    }

    # Start the execution with the configuration data
    execution_arn = start_execution(state_machine_arn, input_data=input_data)
    if not execution_arn:
        logger.error("Failed to start execution. Exiting.")
        return

    # Wait for the execution to complete
    if wait_for_execution_completion(execution_arn):
        logger.info("Step Functions workflow executed successfully")
    else:
        logger.error("Step Functions workflow execution failed")


if __name__ == "__main__":
    main()
