#!/usr/bin/env python3

"""
AWS Step Functions Deployment Script

This script deploys the Step Functions workflow for the data pipeline.
"""

import os

import boto3
from botocore.exceptions import ClientError


# Import project configuration
from config.aws_config import STEP_FUNCTIONS_CONFIG, AWS_REGION
from utils.logging_config import configure_logger


logger = configure_logger(__name__)


def ensure_log_group_exists(log_group_name: str) -> bool:
    """
    Ensure that a CloudWatch log group exists.

    Args:
        log_group_name (str): Name of the log group

    Returns:
        bool: True if the log group exists or was created, False otherwise
    """
    logs_client = boto3.client("logs", region_name=AWS_REGION)

    try:
        # Check if log group exists
        response = logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)
        for log_group in response.get("logGroups", []):
            if log_group["logGroupName"] == log_group_name:
                logger.info(f"Log group '{log_group_name}' already exists")
                return True

        # Create log group if it doesn't exist
        logs_client.create_log_group(logGroupName=log_group_name)
        logger.info(f"Created log group '{log_group_name}'")

        # Set retention policy (30 days)
        logs_client.put_retention_policy(
            logGroupName=log_group_name, retentionInDays=30
        )
        logger.info(f"Set retention policy for log group '{log_group_name}' to 30 days")

        return True
    except ClientError as e:
        logger.error(f"Error ensuring log group '{log_group_name}' exists: {e}")
        return False


def create_or_update_state_machine(
    state_machine_name: str, role_arn: str, definition: str
) -> str | None:
    """
    Create or update a Step Functions state machine.

    Args:
        state_machine_name (str): Name of the state machine
        role_arn (str): IAM role ARN for the state machine
        definition (str): State machine definition JSON

    Returns:
        str: The ARN of the state machine if successful, None otherwise
    """
    sfn_client = boto3.client("stepfunctions", region_name=AWS_REGION)

    # Check if state machine already exists
    try:
        response = sfn_client.list_state_machines()
        for state_machine in response["stateMachines"]:
            if state_machine["name"] == state_machine_name:
                # Update existing state machine
                logger.info(f"Updating existing state machine '{state_machine_name}'")
                response = sfn_client.update_state_machine(  # type: ignore
                    stateMachineArn=state_machine["stateMachineArn"],
                    definition=definition,
                    roleArn=role_arn,
                    loggingConfiguration={
                        "level": "ALL",
                        "includeExecutionData": True,
                        "destinations": [
                            {
                                "cloudWatchLogsLogGroup": {
                                    "logGroupArn": f"arn:aws:logs:{AWS_REGION}:{boto3.client('sts').get_caller_identity()['Account']}:log-group:/aws/vendedlogs/states/{state_machine_name}-logs:*"
                                }
                            }
                        ],
                    },
                )
                logger.info(
                    f"State machine '{state_machine_name}' updated successfully"
                )
                return state_machine["stateMachineArn"]

        # Create new state machine
        logger.info(f"Creating new state machine '{state_machine_name}'")
        response = sfn_client.create_state_machine(
            name=state_machine_name,
            definition=definition,
            roleArn=role_arn,
            type="STANDARD",
            loggingConfiguration={
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [
                    {
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn": f"arn:aws:logs:{AWS_REGION}:{boto3.client('sts').get_caller_identity()['Account']}:log-group:/aws/vendedlogs/states/{state_machine_name}-logs:*"
                        }
                    }
                ],
            },
        )
        logger.info(f"State machine '{state_machine_name}' created successfully")
        return response["stateMachineArn"]

    except Exception as e:
        logger.error(
            f"Error creating/updating state machine '{state_machine_name}': {e}"
        )
        return None


def main() -> None:
    """Main function to deploy the Step Functions workflow."""
    logger.info("Starting Step Functions workflow deployment")

    # Ensure the log group exists
    state_machine_name = STEP_FUNCTIONS_CONFIG["state_machine_name"]
    log_group_name = f"/aws/vendedlogs/states/{state_machine_name}-logs"
    if not ensure_log_group_exists(log_group_name):
        logger.warning(
            f"Failed to ensure log group '{log_group_name}' exists. Continuing anyway."
        )

    # Load the workflow definition
    workflow_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "step_functions",
        "data_pipeline_workflow.json",
    )

    with open(workflow_path, "r") as f:
        workflow_definition = f.read()

    # Create or update the state machine
    state_machine_arn = create_or_update_state_machine(
        STEP_FUNCTIONS_CONFIG["state_machine_name"],
        STEP_FUNCTIONS_CONFIG["role_arn"],
        workflow_definition,
    )

    if state_machine_arn:
        logger.info(
            f"Step Functions workflow deployed successfully: {state_machine_arn}"
        )
    else:
        logger.error("Failed to deploy Step Functions workflow")


if __name__ == "__main__":
    main()
