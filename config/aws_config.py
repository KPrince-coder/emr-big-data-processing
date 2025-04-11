"""
AWS Configuration for Big Data Processing with EMR Project

This module contains configuration settings for AWS services used in the project.
It loads sensitive configuration from environment variables.
"""

from config.env_loader import (
    get_s3_config,
    get_emr_config,
    get_glue_config,
    get_step_functions_config,
    get_iam_roles,
    get_aws_region,
)

# Load configurations from environment variables
S3_CONFIG = get_s3_config()
EMR_CONFIG = get_emr_config()
GLUE_CONFIG = get_glue_config()
STEP_FUNCTIONS_CONFIG = get_step_functions_config()
IAM_ROLES = get_iam_roles()
AWS_REGION = get_aws_region()
