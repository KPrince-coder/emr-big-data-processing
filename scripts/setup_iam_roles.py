#!/usr/bin/env python3
"""
IAM Roles and Permissions Setup Script

This script creates the necessary IAM roles and attaches appropriate policies
for working with S3, EMR, Glue, Step Functions, and other AWS services used in the project.

Usage:
    python setup_iam_roles.py [--region REGION]
"""

import argparse
import json
import os
import re
import sys
import time
import boto3
from botocore.exceptions import ClientError

from config.aws_config import AWS_REGION
from utils.logging_config import configure_logger

# Configure logger
logger = configure_logger(__name__)

# Define IAM role names and descriptions
IAM_ROLES = {
    "emr_service_role": {
        "name": "EMR_DefaultRole",
        "description": "Default role for Amazon EMR service",
        "service": "elasticmapreduce.amazonaws.com",
        "managed_policies": [
            "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
        ],
    },
    "emr_ec2_role": {
        "name": "EMR_EC2_DefaultRole",
        "description": "Default role for Amazon EMR EC2 instances",
        "service": "ec2.amazonaws.com",
        "managed_policies": [
            "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
        ],
    },
    "glue_service_role": {
        "name": "AWSGlueServiceRole-CarRentalCrawler",
        "description": "Role for AWS Glue crawlers",
        "service": "glue.amazonaws.com",
        "managed_policies": ["arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"],
        "custom_policies": [
            {
                "name": "GlueS3Access",
                "description": "Policy for Glue to access S3 buckets",
                "policy_document": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:DeleteObject",
                                "s3:ListBucket",
                            ],
                            "Resource": ["arn:aws:s3:::*/*", "arn:aws:s3:::*"],
                        }
                    ],
                },
            }
        ],
    },
    "step_functions_role": {
        "name": "StepFunctionsExecutionRole",
        "description": "Role for AWS Step Functions to execute workflows",
        "service": "states.amazonaws.com",
        "managed_policies": ["arn:aws:iam::aws:policy/service-role/AWSLambdaRole"],
        "custom_policies": [
            {
                "name": "StepFunctionsEMRAccess",
                "description": "Policy for Step Functions to manage EMR clusters",
                "policy_document": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "elasticmapreduce:AddJobFlowSteps",
                                "elasticmapreduce:AddTags",
                                "elasticmapreduce:CancelSteps",
                                "elasticmapreduce:CreateJobFlow",
                                "elasticmapreduce:DescribeCluster",
                                "elasticmapreduce:DescribeStep",
                                "elasticmapreduce:RunJobFlow",
                                "elasticmapreduce:SetTerminationProtection",
                                "elasticmapreduce:TerminateJobFlows",
                            ],
                            "Resource": "arn:aws:elasticmapreduce:*:*:cluster/*",
                        }
                    ],
                },
            },
            {
                "name": "StepFunctionsGlueAccess",
                "description": "Policy for Step Functions to start Glue crawlers",
                "policy_document": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["glue:StartCrawler", "glue:GetCrawler"],
                            "Resource": "arn:aws:glue:*:*:crawler/*",
                        }
                    ],
                },
            },
            {
                "name": "StepFunctionsS3Access",
                "description": "Policy for Step Functions to access S3",
                "policy_document": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                            "Resource": ["arn:aws:s3:::*/*", "arn:aws:s3:::*"],
                        }
                    ],
                },
            },
            {
                "name": "StepFunctionsLambdaAccess",
                "description": "Policy for Step Functions to invoke Lambda functions",
                "policy_document": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["lambda:InvokeFunction"],
                            "Resource": "arn:aws:lambda:*:*:function:*",
                        }
                    ],
                },
            },
            {
                "name": "StepFunctionsCloudWatchLogsAccess",
                "description": "Policy for Step Functions to write to CloudWatch Logs",
                "policy_document": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "logs:CreateLogDelivery",
                                "logs:GetLogDelivery",
                                "logs:UpdateLogDelivery",
                                "logs:DeleteLogDelivery",
                                "logs:ListLogDeliveries",
                                "logs:PutResourcePolicy",
                                "logs:DescribeResourcePolicies",
                                "logs:DescribeLogGroups",
                            ],
                            "Resource": "*",
                        }
                    ],
                },
            },
            {
                "name": "StepFunctionsPassRole",
                "description": "Policy for Step Functions to pass IAM roles to AWS services",
                "policy_document": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {"Effect": "Allow", "Action": "iam:PassRole", "Resource": "*"}
                    ],
                },
            },
        ],
    },
    "lambda_execution_role": {
        "name": "LambdaGlueCrawlerRole",
        "description": "Role for Lambda functions to start Glue crawlers",
        "service": "lambda.amazonaws.com",
        "managed_policies": [
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        ],
        "custom_policies": [
            {
                "name": "LambdaGlueAccess",
                "description": "Policy for Lambda to start Glue crawlers",
                "policy_document": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "glue:StartCrawler",
                                "glue:GetCrawler",
                                "glue:GetCrawlers",
                            ],
                            "Resource": "*",
                        }
                    ],
                },
            }
        ],
    },
}


def create_trust_relationship_policy(service: str) -> dict:
    """
    Create a trust relationship policy document for a service.

    Args:
        service (str): The AWS service (e.g., 'elasticmapreduce.amazonaws.com')

    Returns:
        dict: The trust relationship policy document
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": service},
                "Action": "sts:AssumeRole",
            }
        ],
    }


def create_iam_role(
    role_name: str, description: str, trust_relationship: dict, region=AWS_REGION
) -> str | None:
    """
    Create an IAM role if it doesn't exist.

    Args:
        role_name (str): Name of the role to create
        description (str): Description of the role
        trust_relationship (dict): Trust relationship policy document
        region (str): AWS region

    Returns:
        str: The ARN of the role if successful, None otherwise
    """
    iam_client = boto3.client("iam", region_name=region)

    try:
        # Check if role already exists
        response = iam_client.get_role(RoleName=role_name)
        logger.info(f"IAM role '{role_name}' already exists")
        return response["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            # Create the role
            try:
                response = iam_client.create_role(
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_relationship),
                    Description=description,
                    MaxSessionDuration=3600,
                )
                logger.info(f"IAM role '{role_name}' created successfully")

                # Wait for role to be available
                time.sleep(10)

                return response["Role"]["Arn"]
            except ClientError as e:
                logger.error(f"Error creating IAM role '{role_name}': {e}")
                return None
        else:
            logger.error(f"Error checking IAM role '{role_name}': {e}")
            return None


def attach_managed_policy(role_name: str, policy_arn: str, region=AWS_REGION) -> bool:
    """
    Attach a managed policy to an IAM role.

    Args:
        role_name (str): Name of the role
        policy_arn (str): ARN of the policy to attach
        region (str): AWS region

    Returns:
        bool: True if policy was attached, False otherwise
    """
    iam_client = boto3.client("iam", region_name=region)

    try:
        # Check if policy is already attached
        attached_policies = iam_client.list_attached_role_policies(RoleName=role_name)
        for policy in attached_policies["AttachedPolicies"]:
            if policy["PolicyArn"] == policy_arn:
                logger.info(
                    f"Policy '{policy_arn}' is already attached to role '{role_name}'"
                )
                return True

        # Attach the policy
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        logger.info(f"Policy '{policy_arn}' attached to role '{role_name}'")
        return True
    except ClientError as e:
        logger.error(
            f"Error attaching policy '{policy_arn}' to role '{role_name}': {e}"
        )
        return False


def create_custom_policy(
    policy_name: str, description: str, policy_document: dict, region=AWS_REGION
) -> str | None:
    """
    Create a custom IAM policy if it doesn't exist, or update it if it does.

    Args:
        policy_name (str): Name of the policy to create or update
        description (str): Description of the policy
        policy_document (dict): Policy document
        region (str): AWS region

    Returns:
        str: The ARN of the policy if successful, None otherwise
    """
    iam_client = boto3.client("iam", region_name=region)

    try:
        # List policies to check if it exists
        policy_arn = None
        paginator = iam_client.get_paginator("list_policies")
        for page in paginator.paginate(Scope="Local"):
            for policy in page["Policies"]:
                if policy["PolicyName"] == policy_name:
                    policy_arn = policy["Arn"]
                    logger.info(f"Custom policy '{policy_name}' already exists")

                    # Get the current policy version
                    policy_versions = iam_client.list_policy_versions(
                        PolicyArn=policy_arn
                    )

                    # Check if we need to delete the oldest version (AWS limits to 5 versions)
                    if len(policy_versions["Versions"]) >= 5:
                        # Find the oldest non-default version
                        oldest_version = None
                        oldest_date = None
                        for version in policy_versions["Versions"]:
                            if not version["IsDefaultVersion"]:
                                if (
                                    oldest_date is None
                                    or version["CreateDate"] < oldest_date
                                ):
                                    oldest_date = version["CreateDate"]
                                    oldest_version = version["VersionId"]

                        # Delete the oldest version if found
                        if oldest_version:
                            iam_client.delete_policy_version(
                                PolicyArn=policy_arn, VersionId=oldest_version
                            )
                            logger.info(
                                f"Deleted oldest version {oldest_version} of policy '{policy_name}'"
                            )

                    # Create a new version of the policy
                    iam_client.create_policy_version(
                        PolicyArn=policy_arn,
                        PolicyDocument=json.dumps(policy_document),
                        SetAsDefault=True,
                    )
                    logger.info(f"Updated policy '{policy_name}' with new permissions")
                    return policy_arn

        # If policy doesn't exist, create it
        if not policy_arn:
            response = iam_client.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document),
                Description=description,
            )
            logger.info(f"Custom policy '{policy_name}' created successfully")
            return response["Policy"]["Arn"]

        return policy_arn
    except ClientError as e:
        logger.error(f"Error creating/updating custom policy '{policy_name}': {e}")
        return None


def create_instance_profile(
    profile_name: str, role_name: str, region=AWS_REGION
) -> str | None:
    """
    Create an instance profile and add a role to it.

    Args:
        profile_name (str): Name of the instance profile to create
        role_name (str): Name of the role to add to the profile
        region (str): AWS region

    Returns:
        str: The ARN of the instance profile if successful, None otherwise
    """
    iam_client = boto3.client("iam", region_name=region)

    try:
        # Check if instance profile already exists
        try:
            response = iam_client.get_instance_profile(InstanceProfileName=profile_name)
            logger.info(f"Instance profile '{profile_name}' already exists")

            # Check if role is already added to the profile
            roles = response["InstanceProfile"]["Roles"]
            for role in roles:
                if role["RoleName"] == role_name:
                    logger.info(
                        f"Role '{role_name}' is already added to instance profile '{profile_name}'"
                    )
                    return response["InstanceProfile"]["Arn"]

            # Add role to the profile
            iam_client.add_role_to_instance_profile(
                InstanceProfileName=profile_name, RoleName=role_name
            )
            logger.info(
                f"Role '{role_name}' added to instance profile '{profile_name}'"
            )
            return response["InstanceProfile"]["Arn"]

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                # Create the instance profile
                response = iam_client.create_instance_profile(
                    InstanceProfileName=profile_name
                )
                logger.info(f"Instance profile '{profile_name}' created successfully")

                # Wait for instance profile to be available
                time.sleep(10)

                # Add role to the profile
                iam_client.add_role_to_instance_profile(
                    InstanceProfileName=profile_name, RoleName=role_name
                )
                logger.info(
                    f"Role '{role_name}' added to instance profile '{profile_name}'"
                )

                return response["InstanceProfile"]["Arn"]
            else:
                raise e
    except ClientError as e:
        logger.error(f"Error creating instance profile '{profile_name}': {e}")
        return None


def create_service_linked_role(service_name: str, region=AWS_REGION) -> bool:
    """
    Create a service-linked role for a specific AWS service.

    Args:
        service_name (str): The AWS service name (e.g., 'elasticmapreduce.amazonaws.com')
        region (str): AWS region

    Returns:
        bool: True if the role was created or already exists, False otherwise
    """
    iam_client = boto3.client("iam", region_name=region)

    try:
        # Check if the service-linked role already exists
        try:
            iam_client.get_role(
                RoleName=f"AWSServiceRoleFor{service_name.split('.')[0].capitalize()}"
            )
            logger.info(f"Service-linked role for {service_name} already exists")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise e

        # Create the service-linked role
        iam_client.create_service_linked_role(AWSServiceName=service_name)
        logger.info(f"Service-linked role for {service_name} created successfully")
        return True
    except ClientError as e:
        logger.error(f"Error creating service-linked role for {service_name}: {e}")
        return False


def setup_iam_roles(region=AWS_REGION) -> dict:
    """
    Set up all IAM roles and permissions.

    Args:
        region (str): AWS region

    Returns:
        dict: A dictionary mapping role keys to their ARNs
    """
    # Create service-linked role for EMR cleanup
    create_service_linked_role("elasticmapreduce.amazonaws.com", region)

    role_arns = {}

    for role_key, role_config in IAM_ROLES.items():
        role_name = role_config["name"]
        description = role_config["description"]
        service = role_config["service"]

        # Create trust relationship policy
        trust_relationship = create_trust_relationship_policy(service)

        # Create the role
        role_arn = create_iam_role(role_name, description, trust_relationship, region)
        if not role_arn:
            logger.error(
                f"Failed to create role '{role_name}'. Skipping policy attachments."
            )
            continue

        role_arns[role_key] = role_arn

        # Attach managed policies
        for policy_arn in role_config.get("managed_policies", []):
            if not attach_managed_policy(role_name, policy_arn, region):
                logger.warning(
                    f"Failed to attach managed policy '{policy_arn}' to role '{role_name}'"
                )

        # Create and attach custom policies
        for policy_config in role_config.get("custom_policies", []):
            policy_name = policy_config["name"]
            policy_description = policy_config["description"]
            policy_document = policy_config["policy_document"]

            policy_arn = create_custom_policy(
                policy_name, policy_description, policy_document, region
            )
            if policy_arn:
                if not attach_managed_policy(role_name, policy_arn, region):
                    logger.warning(
                        f"Failed to attach custom policy '{policy_name}' to role '{role_name}'"
                    )
            else:
                logger.warning(f"Failed to create custom policy '{policy_name}'")

        # Create instance profile for EC2 role
        if role_key == "emr_ec2_role":
            profile_arn = create_instance_profile(role_name, role_name, region)
            if not profile_arn:
                logger.warning(
                    f"Failed to create instance profile for role '{role_name}'"
                )

    return role_arns


def update_env_file(role_arns: dict, region: str) -> None:
    """
    Update the .env file with IAM role ARNs and other configuration.

    Args:
        role_arns (dict): Dictionary of role keys to ARNs
        region (str): AWS region
    """
    env_file_path = ".env"

    # Check if .env file exists
    if not os.path.exists(env_file_path):
        logger.warning(f".env file not found at {env_file_path}. Creating a new one.")
        with open(env_file_path, "w") as f:
            f.write("# AWS Configuration Environment Variables\n\n")

    # Read existing .env file
    with open(env_file_path, "r") as f:
        env_content = f.read()

    # Define sections and their variables
    sections = {
        "# AWS Credentials": {"AWS_REGION": region},
        "# IAM Roles": {
            "EMR_SERVICE_ROLE": IAM_ROLES["emr_service_role"]["name"],
            "EMR_EC2_INSTANCE_PROFILE": IAM_ROLES["emr_ec2_role"]["name"],
            "GLUE_SERVICE_ROLE": IAM_ROLES["glue_service_role"]["name"],
            "STEP_FUNCTIONS_ROLE_ARN": role_arns.get("step_functions_role", ""),
            "LAMBDA_EXECUTION_ROLE_ARN": role_arns.get("lambda_execution_role", ""),
        },
    }

    # Update or add variables in each section
    for section_header, variables in sections.items():
        # Check if section exists
        if section_header not in env_content:
            env_content += f"\n{section_header}\n"

        # Update or add each variable
        for var_name, var_value in variables.items():
            # Skip empty values
            if not var_value:
                continue

            # Create regex pattern to find existing variable
            pattern = re.compile(f"^{var_name}=.*$", re.MULTILINE)
            replacement = f"{var_name}={var_value}"

            # Check if variable exists and update it
            if pattern.search(env_content):
                env_content = pattern.sub(replacement, env_content)
            else:
                # Find the section and add the variable after it
                section_pos = env_content.find(section_header)
                if section_pos != -1:
                    section_end = env_content.find("\n\n", section_pos)
                    if section_end == -1:  # If no blank line after section
                        section_end = len(env_content)

                    # Insert the new variable at the end of the section
                    env_content = (
                        env_content[:section_end]
                        + f"\n{replacement}"
                        + env_content[section_end:]
                    )
                else:
                    # If section not found (shouldn't happen), append to end
                    env_content += f"{replacement}\n"

    # Write updated content back to .env file
    with open(env_file_path, "w") as f:
        f.write(env_content)

    logger.info(f"Updated .env file with IAM roles and ARNs at {env_file_path}")


def main() -> None:
    """Main function to set up IAM roles and permissions."""
    parser = argparse.ArgumentParser(description="Set up IAM roles and permissions")
    parser.add_argument(
        "--region",
        default=AWS_REGION,
        help="AWS region (default: use AWS CLI configuration)",
    )

    args = parser.parse_args()

    logger.info("Starting IAM roles and permissions setup")

    role_arns = setup_iam_roles(args.region)

    if role_arns:
        logger.info("IAM roles and permissions setup completed successfully")
        logger.info("Role ARNs:")
        for role_key, role_arn in role_arns.items():
            logger.info(f"  {role_key}: {role_arn}")

        # Save role ARNs to a file for reference
        with open("iam_roles.json", "w") as f:
            json.dump(role_arns, f, indent=2)
        logger.info("Role ARNs saved to iam_roles.json")

        # Update .env file with role ARNs
        update_env_file(role_arns, args.region)
    else:
        logger.error("Failed to set up IAM roles and permissions")
        sys.exit(1)


if __name__ == "__main__":
    main()
