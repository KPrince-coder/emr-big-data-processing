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
import logging
import sys
import time
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define IAM role names and descriptions
IAM_ROLES = {
    'emr_service_role': {
        'name': 'EMR_DefaultRole',
        'description': 'Default role for Amazon EMR service',
        'service': 'elasticmapreduce.amazonaws.com',
        'managed_policies': [
            'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
        ]
    },
    'emr_ec2_role': {
        'name': 'EMR_EC2_DefaultRole',
        'description': 'Default role for Amazon EMR EC2 instances',
        'service': 'ec2.amazonaws.com',
        'managed_policies': [
            'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role'
        ]
    },
    'glue_service_role': {
        'name': 'AWSGlueServiceRole-CarRentalCrawler',
        'description': 'Role for AWS Glue crawlers',
        'service': 'glue.amazonaws.com',
        'managed_policies': [
            'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        ],
        'custom_policies': [
            {
                'name': 'GlueS3Access',
                'description': 'Policy for Glue to access S3 buckets',
                'policy_document': {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': [
                                's3:GetObject',
                                's3:PutObject',
                                's3:DeleteObject',
                                's3:ListBucket'
                            ],
                            'Resource': [
                                'arn:aws:s3:::*/*',
                                'arn:aws:s3:::*'
                            ]
                        }
                    ]
                }
            }
        ]
    },
    'step_functions_role': {
        'name': 'StepFunctionsExecutionRole',
        'description': 'Role for AWS Step Functions to execute workflows',
        'service': 'states.amazonaws.com',
        'managed_policies': [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaRole'
        ],
        'custom_policies': [
            {
                'name': 'StepFunctionsEMRAccess',
                'description': 'Policy for Step Functions to manage EMR clusters',
                'policy_document': {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': [
                                'elasticmapreduce:AddJobFlowSteps',
                                'elasticmapreduce:CancelSteps',
                                'elasticmapreduce:CreateJobFlow',
                                'elasticmapreduce:DescribeCluster',
                                'elasticmapreduce:DescribeStep',
                                'elasticmapreduce:RunJobFlow',
                                'elasticmapreduce:SetTerminationProtection',
                                'elasticmapreduce:TerminateJobFlows'
                            ],
                            'Resource': 'arn:aws:elasticmapreduce:*:*:cluster/*'
                        }
                    ]
                }
            },
            {
                'name': 'StepFunctionsGlueAccess',
                'description': 'Policy for Step Functions to start Glue crawlers',
                'policy_document': {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': [
                                'glue:StartCrawler',
                                'glue:GetCrawler'
                            ],
                            'Resource': 'arn:aws:glue:*:*:crawler/*'
                        }
                    ]
                }
            },
            {
                'name': 'StepFunctionsS3Access',
                'description': 'Policy for Step Functions to access S3',
                'policy_document': {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': [
                                's3:GetObject',
                                's3:PutObject',
                                's3:ListBucket'
                            ],
                            'Resource': [
                                'arn:aws:s3:::*/*',
                                'arn:aws:s3:::*'
                            ]
                        }
                    ]
                }
            },
            {
                'name': 'StepFunctionsLambdaAccess',
                'description': 'Policy for Step Functions to invoke Lambda functions',
                'policy_document': {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': [
                                'lambda:InvokeFunction'
                            ],
                            'Resource': 'arn:aws:lambda:*:*:function:*'
                        }
                    ]
                }
            }
        ]
    },
    'lambda_execution_role': {
        'name': 'LambdaGlueCrawlerRole',
        'description': 'Role for Lambda functions to start Glue crawlers',
        'service': 'lambda.amazonaws.com',
        'managed_policies': [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        ],
        'custom_policies': [
            {
                'name': 'LambdaGlueAccess',
                'description': 'Policy for Lambda to start Glue crawlers',
                'policy_document': {
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Action': [
                                'glue:StartCrawler',
                                'glue:GetCrawler',
                                'glue:GetCrawlers'
                            ],
                            'Resource': '*'
                        }
                    ]
                }
            }
        ]
    }
}

def create_trust_relationship_policy(service):
    """
    Create a trust relationship policy document for a service.
    
    Args:
        service (str): The AWS service (e.g., 'elasticmapreduce.amazonaws.com')
        
    Returns:
        dict: The trust relationship policy document
    """
    return {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Effect': 'Allow',
                'Principal': {
                    'Service': service
                },
                'Action': 'sts:AssumeRole'
            }
        ]
    }

def create_iam_role(role_name, description, trust_relationship, region=None):
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
    iam_client = boto3.client('iam', region_name=region)
    
    try:
        # Check if role already exists
        response = iam_client.get_role(RoleName=role_name)
        logger.info(f"IAM role '{role_name}' already exists")
        return response['Role']['Arn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            # Create the role
            try:
                response = iam_client.create_role(
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_relationship),
                    Description=description,
                    MaxSessionDuration=3600
                )
                logger.info(f"IAM role '{role_name}' created successfully")
                
                # Wait for role to be available
                time.sleep(10)
                
                return response['Role']['Arn']
            except ClientError as e:
                logger.error(f"Error creating IAM role '{role_name}': {e}")
                return None
        else:
            logger.error(f"Error checking IAM role '{role_name}': {e}")
            return None

def attach_managed_policy(role_name, policy_arn, region=None):
    """
    Attach a managed policy to an IAM role.
    
    Args:
        role_name (str): Name of the role
        policy_arn (str): ARN of the policy to attach
        region (str): AWS region
        
    Returns:
        bool: True if policy was attached, False otherwise
    """
    iam_client = boto3.client('iam', region_name=region)
    
    try:
        # Check if policy is already attached
        attached_policies = iam_client.list_attached_role_policies(RoleName=role_name)
        for policy in attached_policies['AttachedPolicies']:
            if policy['PolicyArn'] == policy_arn:
                logger.info(f"Policy '{policy_arn}' is already attached to role '{role_name}'")
                return True
        
        # Attach the policy
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        logger.info(f"Policy '{policy_arn}' attached to role '{role_name}'")
        return True
    except ClientError as e:
        logger.error(f"Error attaching policy '{policy_arn}' to role '{role_name}': {e}")
        return False

def create_custom_policy(policy_name, description, policy_document, region=None):
    """
    Create a custom IAM policy if it doesn't exist.
    
    Args:
        policy_name (str): Name of the policy to create
        description (str): Description of the policy
        policy_document (dict): Policy document
        region (str): AWS region
        
    Returns:
        str: The ARN of the policy if successful, None otherwise
    """
    iam_client = boto3.client('iam', region_name=region)
    
    try:
        # List policies to check if it exists
        paginator = iam_client.get_paginator('list_policies')
        for page in paginator.paginate(Scope='Local'):
            for policy in page['Policies']:
                if policy['PolicyName'] == policy_name:
                    logger.info(f"Custom policy '{policy_name}' already exists")
                    return policy['Arn']
        
        # Create the policy
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description=description
        )
        logger.info(f"Custom policy '{policy_name}' created successfully")
        return response['Policy']['Arn']
    except ClientError as e:
        logger.error(f"Error creating custom policy '{policy_name}': {e}")
        return None

def create_instance_profile(profile_name, role_name, region=None):
    """
    Create an instance profile and add a role to it.
    
    Args:
        profile_name (str): Name of the instance profile to create
        role_name (str): Name of the role to add to the profile
        region (str): AWS region
        
    Returns:
        str: The ARN of the instance profile if successful, None otherwise
    """
    iam_client = boto3.client('iam', region_name=region)
    
    try:
        # Check if instance profile already exists
        try:
            response = iam_client.get_instance_profile(InstanceProfileName=profile_name)
            logger.info(f"Instance profile '{profile_name}' already exists")
            
            # Check if role is already added to the profile
            roles = response['InstanceProfile']['Roles']
            for role in roles:
                if role['RoleName'] == role_name:
                    logger.info(f"Role '{role_name}' is already added to instance profile '{profile_name}'")
                    return response['InstanceProfile']['Arn']
            
            # Add role to the profile
            iam_client.add_role_to_instance_profile(
                InstanceProfileName=profile_name,
                RoleName=role_name
            )
            logger.info(f"Role '{role_name}' added to instance profile '{profile_name}'")
            return response['InstanceProfile']['Arn']
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                # Create the instance profile
                response = iam_client.create_instance_profile(
                    InstanceProfileName=profile_name
                )
                logger.info(f"Instance profile '{profile_name}' created successfully")
                
                # Wait for instance profile to be available
                time.sleep(10)
                
                # Add role to the profile
                iam_client.add_role_to_instance_profile(
                    InstanceProfileName=profile_name,
                    RoleName=role_name
                )
                logger.info(f"Role '{role_name}' added to instance profile '{profile_name}'")
                
                return response['InstanceProfile']['Arn']
            else:
                raise e
    except ClientError as e:
        logger.error(f"Error creating instance profile '{profile_name}': {e}")
        return None

def setup_iam_roles(region=None):
    """
    Set up all IAM roles and permissions.
    
    Args:
        region (str): AWS region
        
    Returns:
        dict: A dictionary mapping role keys to their ARNs
    """
    role_arns = {}
    
    for role_key, role_config in IAM_ROLES.items():
        role_name = role_config['name']
        description = role_config['description']
        service = role_config['service']
        
        # Create trust relationship policy
        trust_relationship = create_trust_relationship_policy(service)
        
        # Create the role
        role_arn = create_iam_role(role_name, description, trust_relationship, region)
        if not role_arn:
            logger.error(f"Failed to create role '{role_name}'. Skipping policy attachments.")
            continue
        
        role_arns[role_key] = role_arn
        
        # Attach managed policies
        for policy_arn in role_config.get('managed_policies', []):
            if not attach_managed_policy(role_name, policy_arn, region):
                logger.warning(f"Failed to attach managed policy '{policy_arn}' to role '{role_name}'")
        
        # Create and attach custom policies
        for policy_config in role_config.get('custom_policies', []):
            policy_name = policy_config['name']
            policy_description = policy_config['description']
            policy_document = policy_config['policy_document']
            
            policy_arn = create_custom_policy(policy_name, policy_description, policy_document, region)
            if policy_arn:
                if not attach_managed_policy(role_name, policy_arn, region):
                    logger.warning(f"Failed to attach custom policy '{policy_name}' to role '{role_name}'")
            else:
                logger.warning(f"Failed to create custom policy '{policy_name}'")
        
        # Create instance profile for EC2 role
        if role_key == 'emr_ec2_role':
            profile_arn = create_instance_profile(role_name, role_name, region)
            if not profile_arn:
                logger.warning(f"Failed to create instance profile for role '{role_name}'")
    
    return role_arns

def main():
    """Main function to set up IAM roles and permissions."""
    parser = argparse.ArgumentParser(description='Set up IAM roles and permissions')
    parser.add_argument('--region', default=None, help='AWS region (default: use AWS CLI configuration)')
    
    args = parser.parse_args()
    
    logger.info("Starting IAM roles and permissions setup")
    
    role_arns = setup_iam_roles(args.region)
    
    if role_arns:
        logger.info("IAM roles and permissions setup completed successfully")
        logger.info("Role ARNs:")
        for role_key, role_arn in role_arns.items():
            logger.info(f"  {role_key}: {role_arn}")
        
        # Save role ARNs to a file for reference
        with open('iam_roles.json', 'w') as f:
            json.dump(role_arns, f, indent=2)
        logger.info("Role ARNs saved to iam_roles.json")
    else:
        logger.error("Failed to set up IAM roles and permissions")
        sys.exit(1)

if __name__ == "__main__":
    main()
