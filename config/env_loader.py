"""
Environment Variable Loader

This module loads environment variables from a .env file and provides
utility functions to access them.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from the .env file (if it exists)
env_path = Path(__file__).parent.parent / ".env"  # Path to the .env file
load_dotenv(
    dotenv_path=env_path
) if env_path.exists() else None  # Load environment variables from the .env file (if it exists)


def get_env_var(var_name: str, default=None, required=False) -> str:
    """
    Get an environment variable.

    Args:
        var_name (str): Name of the environment variable
        default: Default value to return if the variable is not set
        required (bool): Whether the variable is required

    Returns:
        The value of the environment variable, or the default value

    Raises:
        ValueError: If the variable is required but not set
    """
    value = os.getenv(var_name, default)

    if required and value is None:
        logger.error(f"Required environment variable {var_name} is not set")
        raise ValueError(f"Required environment variable {var_name} is not set")

    return value


def get_aws_credentials() -> dict:
    """
    Get AWS credentials from environment variables.

    Returns:
        dict: AWS credentials
    """
    return {
        "aws_access_key_id": get_env_var("AWS_ACCESS_KEY_ID", required=True),
        "aws_secret_access_key": get_env_var("AWS_SECRET_ACCESS_KEY", required=True),
        "region_name": get_aws_region(),
    }


def get_s3_config() -> dict:
    """
    Get S3 configuration from environment variables.

    Returns:
        dict: S3 configuration
    """

    return {
        "bucket_name": get_aws_bucket(),
        "raw_data_prefix": "raw/",
        "processed_data_prefix": "processed/",
        "temp_data_prefix": "temp/",
        "scripts_prefix": "scripts/",
    }


def get_emr_config() -> dict:
    """
    Get EMR configuration from environment variables.

    Returns:
        dict: EMR configuration
    """

    return {
        "name": get_env_var("EMR_CLUSTER_NAME", default="Car-Rental-EMR-Cluster"),
        "log_uri": f"s3://{get_aws_bucket()}/logs/",
        "release_label": get_env_var("EMR_RELEASE_LABEL", default="emr-6.10.0"),
        "applications": ["Spark", "Hadoop", "Hive", "Livy"],
        "master_instance_type": get_env_var(
            "EMR_MASTER_INSTANCE_TYPE", default="m5.xlarge"
        ),
        "core_instance_type": get_env_var(
            "EMR_CORE_INSTANCE_TYPE", default="m5.xlarge"
        ),
        "core_instance_count": int(get_env_var("EMR_CORE_INSTANCE_COUNT", default="2")),
        "ec2_key_name": get_env_var("EMR_EC2_KEY_NAME", default="emr-key-pair"),
        "bootstrap_actions": [],
        "configurations": [
            {
                "Classification": "spark",
                "Properties": {"maximizeResourceAllocation": "true"},
            },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.executor.instances": "2",
                    "spark.executor.memory": "4g",
                    "spark.driver.memory": "4g",
                },
            },
        ],
    }


def get_glue_config() -> dict:
    """
    Get Glue configuration from environment variables.

    Returns:
        dict: Glue configuration
    """
    database_name = get_env_var("GLUE_DATABASE_NAME", default="car_rental_db")

    return {
        "database_name": database_name,
        "crawler_role": get_env_var(
            "GLUE_SERVICE_ROLE", default="AWSGlueServiceRole-CarRentalCrawler"
        ),
        "crawler_name_prefix": "car-rental-crawler-",
        "tables": {
            "vehicle_location_metrics": {
                "name": "vehicle_location_metrics",
                "location": f"s3://{get_aws_bucket()}/processed/vehicle_location_metrics/",
            },
            "user_transaction_analysis": {
                "name": "user_transaction_analysis",
                "location": f"s3://{get_aws_bucket()}/processed/user_transaction_analysis/",
            },
        },
    }


def get_step_functions_config() -> dict:
    """
    Get Step Functions configuration from environment variables.

    Returns:
        dict: Step Functions configuration
    """
    return {
        "state_machine_name": get_env_var(
            "STEP_FUNCTIONS_STATE_MACHINE_NAME", default="CarRentalDataPipeline"
        ),
        "role_arn": get_env_var("STEP_FUNCTIONS_ROLE_ARN", required=True),
    }


def get_iam_roles() -> dict:
    """
    Get IAM roles from environment variables.

    Returns:
        dict: IAM roles
    """
    return {
        "emr_service_role": get_env_var("EMR_SERVICE_ROLE", default="EMR_DefaultRole"),
        "emr_ec2_instance_profile": get_env_var(
            "EMR_EC2_INSTANCE_PROFILE", default="EMR_EC2_DefaultRole"
        ),
        "glue_service_role": get_env_var(
            "GLUE_SERVICE_ROLE", default="AWSGlueServiceRole-CarRentalCrawler"
        ),
        "step_functions_role": get_env_var(
            "STEP_FUNCTIONS_ROLE_ARN", default="StepFunctionsExecutionRole"
        ),
    }


def get_aws_region() -> str:
    """
    Get AWS region from environment variables.

    Returns:
        str: AWS region
    """
    return get_env_var("AWS_REGION", default="eu-west-1")


def get_aws_bucket() -> str:
    """
    Get AWS bucket name from environment variables.

    Returns:
        str: AWS bucket name
    """
    return get_env_var("S3_BUCKET_NAME", required=True)
