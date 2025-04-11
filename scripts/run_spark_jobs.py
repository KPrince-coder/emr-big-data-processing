#!/usr/bin/env python3
"""
Run Spark Jobs on EMR

This script runs the Spark jobs on an existing EMR cluster.
"""

import os
import boto3
import logging
import sys
import time

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project configuration
from config.aws_config import S3_CONFIG, AWS_REGION

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_step_to_emr(cluster_id, step_name, jar, args):
    """
    Add a step to an EMR cluster.
    
    Args:
        cluster_id (str): The ID of the EMR cluster
        step_name (str): The name of the step
        jar (str): The JAR file to use
        args (list): The arguments to pass to the JAR
        
    Returns:
        str: The step ID if successful, None otherwise
    """
    emr_client = boto3.client('emr', region_name=AWS_REGION)
    
    try:
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    'Name': step_name,
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': jar,
                        'Args': args
                    }
                }
            ]
        )
        
        step_id = response['StepIds'][0]
        logger.info(f"Added step '{step_name}' (ID: {step_id}) to EMR cluster {cluster_id}")
        return step_id
    except Exception as e:
        logger.error(f"Error adding step '{step_name}' to EMR cluster {cluster_id}: {e}")
        return None

def wait_for_step_completion(cluster_id, step_id):
    """
    Wait for an EMR step to complete.
    
    Args:
        cluster_id (str): The ID of the EMR cluster
        step_id (str): The ID of the step to wait for
        
    Returns:
        bool: True if the step completed successfully, False otherwise
    """
    emr_client = boto3.client('emr', region_name=AWS_REGION)
    
    logger.info(f"Waiting for step {step_id} to complete...")
    
    while True:
        response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = response['Step']['Status']['State']
        
        if state == 'COMPLETED':
            logger.info(f"Step {step_id} completed successfully")
            return True
        elif state in ['PENDING', 'RUNNING']:
            logger.info(f"Step {step_id} is {state}")
            time.sleep(30)  # Wait for 30 seconds before checking again
        else:
            logger.error(f"Step {step_id} failed with state {state}")
            if 'FailureDetails' in response['Step']['Status']:
                reason = response['Step']['Status']['FailureDetails'].get('Reason', 'Unknown')
                logger.error(f"Failure reason: {reason}")
            return False

def run_vehicle_location_metrics_job(cluster_id, s3_bucket, environment):
    """
    Run the vehicle and location performance metrics job on EMR.
    
    Args:
        cluster_id (str): The ID of the EMR cluster
        s3_bucket (str): The S3 bucket name
        environment (str): The environment ('dev' or 'prod')
        
    Returns:
        bool: True if the job completed successfully, False otherwise
    """
    step_name = "Vehicle Location Metrics Job"
    jar = "command-runner.jar"
    args = [
        "spark-submit",
        "--deploy-mode", "cluster",
        f"s3://{s3_bucket}/{S3_CONFIG['scripts_prefix']}job1_vehicle_location_metrics.py",
        s3_bucket,
        environment
    ]
    
    step_id = add_step_to_emr(cluster_id, step_name, jar, args)
    if not step_id:
        return False
    
    return wait_for_step_completion(cluster_id, step_id)

def run_user_transaction_analysis_job(cluster_id, s3_bucket, environment):
    """
    Run the user and transaction analysis job on EMR.
    
    Args:
        cluster_id (str): The ID of the EMR cluster
        s3_bucket (str): The S3 bucket name
        environment (str): The environment ('dev' or 'prod')
        
    Returns:
        bool: True if the job completed successfully, False otherwise
    """
    step_name = "User Transaction Analysis Job"
    jar = "command-runner.jar"
    args = [
        "spark-submit",
        "--deploy-mode", "cluster",
        f"s3://{s3_bucket}/{S3_CONFIG['scripts_prefix']}job2_user_transaction_analysis.py",
        s3_bucket,
        environment
    ]
    
    step_id = add_step_to_emr(cluster_id, step_name, jar, args)
    if not step_id:
        return False
    
    return wait_for_step_completion(cluster_id, step_id)

def main():
    """Main function to run Spark jobs on EMR."""
    # Check if the required arguments are provided
    if len(sys.argv) != 3:
        print("Usage: run_spark_jobs.py <cluster_id> <environment>")
        sys.exit(1)
    
    cluster_id = sys.argv[1]
    environment = sys.argv[2]  # 'dev' or 'prod'
    
    logger.info(f"Running Spark jobs on EMR cluster {cluster_id} in {environment} environment")
    
    # Run the vehicle and location performance metrics job
    logger.info("Running vehicle and location performance metrics job")
    if not run_vehicle_location_metrics_job(cluster_id, S3_CONFIG['bucket_name'], environment):
        logger.error("Vehicle and location performance metrics job failed")
        sys.exit(1)
    
    # Run the user and transaction analysis job
    logger.info("Running user and transaction analysis job")
    if not run_user_transaction_analysis_job(cluster_id, S3_CONFIG['bucket_name'], environment):
        logger.error("User and transaction analysis job failed")
        sys.exit(1)
    
    logger.info("All Spark jobs completed successfully")

if __name__ == "__main__":
    main()
