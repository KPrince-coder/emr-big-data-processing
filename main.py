#!/usr/bin/env python3
"""
Main Script for Car Rental Data Processing Pipeline

This script orchestrates the entire data processing pipeline:
1. Set up the AWS environment
2. Create an EMR cluster
3. Run Spark jobs on EMR
4. Set up Glue crawlers
5. Deploy Step Functions workflow

Usage:
    python main.py [--deploy-only] [--run-workflow]
"""

import os
import sys
import argparse
import logging
import importlib.util
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def import_script(script_path):
    """
    Import a Python script as a module.

    Args:
        script_path (str): Path to the script

    Returns:
        module: The imported module
    """
    script_name = os.path.basename(script_path).split(".")[0]
    spec = importlib.util.spec_from_file_location(script_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_pipeline(deploy_only=False, run_workflow=False):
    """
    Run the entire data processing pipeline.

    Args:
        deploy_only (bool): If True, only deploy the infrastructure without running jobs
        run_workflow (bool): If True, execute the Step Functions workflow
    """
    # Get the project root directory
    project_root = os.path.dirname(os.path.abspath(__file__))

    # Step 1: Set up the AWS environment
    logger.info("Step 1: Setting up AWS environment")
    setup_aws_env = import_script(
        os.path.join(project_root, "scripts", "setup_aws_environment.py")
    )
    setup_aws_env.main()

    if deploy_only:
        # Step 2: Deploy Step Functions workflow
        logger.info("Step 2: Deploying Step Functions workflow")
        deploy_step_functions = import_script(
            os.path.join(project_root, "scripts", "deploy_step_functions.py")
        )
        deploy_step_functions.main()

        logger.info("Deployment completed successfully")
        return

    # Step 2: Create an EMR cluster
    logger.info("Step 2: Creating EMR cluster")
    create_emr_cluster = import_script(
        os.path.join(project_root, "scripts", "create_emr_cluster.py")
    )
    create_emr_cluster.main()

    # Read the cluster ID from the file
    with open("emr_cluster_id.txt", "r") as f:
        cluster_id = f.read().strip()

    # Step 3: Upload Spark jobs to S3
    logger.info("Step 3: Uploading Spark jobs to S3")
    # This would be implemented in setup_aws_environment.py

    # Step 4: Run Spark jobs on EMR
    logger.info("Step 4: Running Spark jobs on EMR")
    run_spark_jobs = import_script(
        os.path.join(project_root, "scripts", "run_spark_jobs.py")
    )
    # Pass the cluster_id as an argument
    sys.argv = [sys.argv[0], cluster_id, "prod"]
    run_spark_jobs.main()

    # Step 5: Set up Glue crawlers
    logger.info("Step 5: Setting up Glue crawlers")
    setup_glue_crawlers = import_script(
        os.path.join(project_root, "scripts", "setup_glue_crawlers.py")
    )
    setup_glue_crawlers.main()

    # Step 6: Deploy Step Functions workflow
    logger.info("Step 6: Deploying Step Functions workflow")
    deploy_step_functions = import_script(
        os.path.join(project_root, "scripts", "deploy_step_functions.py")
    )
    deploy_step_functions.main()

    if run_workflow:
        # Step 7: Execute Step Functions workflow
        logger.info("Step 7: Executing Step Functions workflow")
        execute_step_functions = import_script(
            os.path.join(project_root, "scripts", "execute_step_functions.py")
        )
        execute_step_functions.main()

    logger.info("Pipeline execution completed successfully")


def main():
    """Main function to parse arguments and run the pipeline."""
    parser = argparse.ArgumentParser(description="Car Rental Data Processing Pipeline")
    parser.add_argument(
        "--deploy-only",
        action="store_true",
        help="Only deploy the infrastructure without running jobs",
    )
    parser.add_argument(
        "--run-workflow",
        action="store_true",
        help="Execute the Step Functions workflow",
    )

    args = parser.parse_args()

    run_pipeline(deploy_only=args.deploy_only, run_workflow=args.run_workflow)


if __name__ == "__main__":
    main()
