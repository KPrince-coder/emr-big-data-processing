#!/usr/bin/env python3
"""
AWS Lambda Function Deployment Script

This script handles the deployment of AWS Lambda functions, specifically for managing Glue crawlers. It performs the following tasks:

1. Creates a deployment package (ZIP) containing the Lambda function code
2. Configures Lambda function settings including runtime, memory, and timeout
3. Handles both creation of new Lambda functions and updates to existing ones
4. Manages IAM roles and permissions for Lambda execution
5. Provides error handling and logging during deployment

Requirements:
- AWS credentials configured
- Required Python packages: boto3
- Appropriate IAM permissions to create/update Lambda functions

Usage:
    python deploy_lambda_function.py [options]
"""

import os
import sys
import argparse
import zipfile
import tempfile
import time
import boto3
from botocore.exceptions import ClientError

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project configuration
from config.aws_config import AWS_REGION, IAM_ROLES
from utils.logging_config import configure_logger

# Configure logger
logger = configure_logger(__name__)

# Lambda function configuration
LAMBDA_CONFIG = {
    "function_name": "StartGlueCrawlers",
    "handler": "lambda_start_glue_crawlers_standalone.lambda_handler",
    "runtime": "python3.9",
    "timeout": 900,  # 15 minutes
    "memory_size": 128,
    "source_file": "scripts/lambda_start_glue_crawlers_standalone.py",
}


def create_lambda_deployment_package(source_file: str) -> bytes:
    """
    Create a deployment package (ZIP file) for the Lambda function.

    Args:
        source_file (str): Path to the Lambda function source file

    Returns:
        bytes: The deployment package as bytes
    """
    # Create a temporary file for the deployment package
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        # Create the deployment package
        with zipfile.ZipFile(temp_file, "w") as zip_file:
            # Add the Lambda function source file
            lambda_file_name = os.path.basename(source_file)
            zip_file.write(
                source_file,
                arcname=lambda_file_name,
            )

        temp_file_path = temp_file.name

    try:
        # Read the deployment package as bytes
        with open(temp_file_path, "rb") as f:
            deployment_package = f.read()

        logger.info("Created Lambda deployment package")
        return deployment_package
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


def deploy_lambda_function(
    function_name: str,
    handler: str,
    runtime: str,
    role_arn: str,
    deployment_package: bytes,
    timeout: int = 60,
    memory_size: int = 128,
    region=AWS_REGION,
    max_retries: int = 5,
    retry_delay: int = 10,
) -> bool:
    """
    Deploy a Lambda function.

    Args:
        function_name (str): Name of the Lambda function
        handler (str): Handler function (e.g., 'lambda_function.lambda_handler')
        runtime (str): Runtime (e.g., 'python3.9')
        role_arn (str): ARN of the IAM role for the Lambda function
        deployment_package (bytes): The deployment package as bytes
        timeout (int): Function timeout in seconds
        memory_size (int): Function memory size in MB
        region (str): AWS region

    Returns:
        bool: True if the function was deployed successfully, False otherwise
    """
    lambda_client = boto3.client("lambda", region_name=region)

    retries = 0
    while retries <= max_retries:
        try:
            # Check if the function already exists
            try:
                lambda_client.get_function(FunctionName=function_name)
                logger.info(
                    f"Lambda function '{function_name}' already exists, updating..."
                )

                # Update the function code
                lambda_client.update_function_code(
                    FunctionName=function_name,
                    ZipFile=deployment_package,
                )

                # Update the function configuration
                lambda_client.update_function_configuration(
                    FunctionName=function_name,
                    Handler=handler,
                    Role=role_arn,
                    Timeout=timeout,
                    MemorySize=memory_size,
                )

                logger.info(f"Lambda function '{function_name}' updated successfully")
                return True

            except lambda_client.exceptions.ResourceNotFoundException:
                # Function doesn't exist, create it
                logger.info(
                    f"Lambda function '{function_name}' doesn't exist, creating..."
                )

                lambda_client.create_function(
                    FunctionName=function_name,
                    Runtime=runtime,
                    Role=role_arn,
                    Handler=handler,
                    Code={"ZipFile": deployment_package},
                    Timeout=timeout,
                    MemorySize=memory_size,
                )

                logger.info(f"Lambda function '{function_name}' created successfully")
                return True

        except ClientError as e:
            if (
                e.response["Error"]["Code"] == "ResourceConflictException"
                and retries < max_retries
            ):
                # If there's a conflict (update in progress), wait and retry
                retries += 1
                wait_time = retry_delay * retries  # Exponential backoff
                logger.warning(
                    f"ResourceConflictException: Update in progress for function '{function_name}'. "
                    f"Retrying in {wait_time} seconds (attempt {retries}/{max_retries})..."
                )
                time.sleep(wait_time)
            else:
                # For other errors or if we've exhausted retries, log and return False
                logger.error(f"Error deploying Lambda function '{function_name}': {e}")
                return False

    # If we've exhausted all retries
    logger.error(
        f"Failed to deploy Lambda function '{function_name}' after {max_retries} retries"
    )
    return False


def main() -> None:
    """Main function to deploy the Lambda function."""
    parser = argparse.ArgumentParser(description="Deploy Lambda function")
    parser.add_argument(
        "--function-name",
        default=LAMBDA_CONFIG["function_name"],
        help=f"Name of the Lambda function (default: {LAMBDA_CONFIG['function_name']})",
    )
    parser.add_argument(
        "--handler",
        default=LAMBDA_CONFIG["handler"],
        help=f"Handler function (default: {LAMBDA_CONFIG['handler']})",
    )
    parser.add_argument(
        "--runtime",
        default=LAMBDA_CONFIG["runtime"],
        help=f"Runtime (default: {LAMBDA_CONFIG['runtime']})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=LAMBDA_CONFIG["timeout"],
        help=f"Function timeout in seconds (default: {LAMBDA_CONFIG['timeout']})",
    )
    parser.add_argument(
        "--memory-size",
        type=int,
        default=LAMBDA_CONFIG["memory_size"],
        help=f"Function memory size in MB (default: {LAMBDA_CONFIG['memory_size']})",
    )
    parser.add_argument(
        "--source-file",
        default=LAMBDA_CONFIG["source_file"],
        help=f"Path to the Lambda function source file (default: {LAMBDA_CONFIG['source_file']})",
    )
    parser.add_argument(
        "--region",
        default=AWS_REGION,
        help="AWS region (default: use AWS CLI configuration)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum number of retries for ResourceConflictException (default: 5)",
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=10,
        help="Initial delay between retries in seconds (default: 10)",
    )

    args = parser.parse_args()

    logger.info("Starting Lambda function deployment")

    # Create the deployment package
    deployment_package = create_lambda_deployment_package(args.source_file)

    # Get the IAM role ARN for the Lambda function
    role_arn = IAM_ROLES.get("lambda_execution_role_arn")
    if not role_arn:
        logger.error("Lambda execution role ARN not found in IAM_ROLES")
        sys.exit(1)

    # Deploy the Lambda function
    if deploy_lambda_function(
        args.function_name,
        args.handler,
        args.runtime,
        role_arn,
        deployment_package,
        args.timeout,
        args.memory_size,
        args.region,
        args.max_retries,
        args.retry_delay,
    ):
        logger.info("Lambda function deployment completed successfully")
    else:
        logger.error("Lambda function deployment failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
