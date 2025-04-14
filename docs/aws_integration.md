# AWS Integration Guide

This document provides detailed information about the AWS integration components of the Big Data Processing with AWS EMR project.

## Table of Contents

- [AWS Integration Guide](#aws-integration-guide)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [AWS Services](#aws-services)
    - [Amazon S3](#amazon-s3)
    - [Amazon EMR](#amazon-emr)
    - [AWS Glue](#aws-glue)
    - [Amazon Athena](#amazon-athena)
    - [AWS Step Functions](#aws-step-functions)
    - [AWS Lambda](#aws-lambda)
  - [IAM Roles and Permissions](#iam-roles-and-permissions)
    - [EMR Service Role](#emr-service-role)
    - [EMR EC2 Instance Profile](#emr-ec2-instance-profile)
    - [Glue Service Role](#glue-service-role)
    - [Step Functions Execution Role](#step-functions-execution-role)
  - [Environment Setup](#environment-setup)
  - [Deployment Scripts](#deployment-scripts)
  - [Monitoring and Logging](#monitoring-and-logging)
    - [Logging](#logging)
    - [Monitoring](#monitoring)
  - [Cost Optimization](#cost-optimization)
  - [Security Considerations](#security-considerations)

## Overview

The project integrates with several AWS services to create a complete data processing pipeline. The integration is implemented using the AWS SDK for Python (boto3) and follows AWS best practices for security, cost optimization, and performance.

## AWS Services

### Amazon S3

Amazon S3 (Simple Storage Service) is used for storing both raw and processed data.

**Configuration:**

```python
S3_CONFIG = {
    'bucket_name': 'car-rental-data-lake',
    'raw_data_prefix': 'raw/',
    'processed_data_prefix': 'processed/',
    'temp_data_prefix': 'temp/',
    'scripts_prefix': 'scripts/',
}
```

**Data Organization:**

```markdown
s3://bucket-name/
├── raw/
│   ├── vehicles/
│   ├── users/
│   ├── locations/
│   └── rental_transactions/
├── processed/
│   ├── vehicle_location_metrics/
│   └── user_transaction_analysis/
├── temp/
│   └── athena_results/
├── scripts/
│   ├── job1_vehicle_location_metrics.py
│   └── job2_user_transaction_analysis.py
└── logs/
    └── emr/
```

**Implementation:**
The S3 integration is implemented in several scripts:

1. `scripts/setup_aws_environment.py` - General AWS environment setup
2. `scripts/setup_s3_bucket.py` - Dedicated script for S3 bucket setup and data loading

These scripts include functions for:

- Creating S3 buckets
- Creating folder structure
- Uploading data files
- Uploading Spark scripts

**S3 Bucket Setup Script:**

The `setup_s3_bucket.py` script provides a convenient way to create an S3 bucket and load data into it. It can be executed directly or through wrapper scripts:

```bash
# Using the Python script directly
python scripts/setup_s3_bucket.py --bucket-name your-bucket-name --region us-east-1

# Using the Bash wrapper script
bash scripts/setup_s3.sh --bucket-name your-bucket-name --region us-east-1

# Using the PowerShell wrapper script (Windows)
.\scripts\setup_s3.ps1 -BucketName your-bucket-name -Region us-east-1
```

The script performs the following actions:

1. Creates the S3 bucket if it doesn't exist
2. Creates the folder structure (raw, processed, temp, scripts, logs)
3. Uploads data files from the data directory to the appropriate subfolders
4. Uploads Spark scripts to the scripts folder

**Code Example:**

```python
def upload_file_to_s3(file_path, bucket_name, object_name=None):
    """
    Upload a file to an S3 bucket.

    Args:
        file_path (str): Path to the file to upload
        bucket_name (str): Name of the bucket to upload to
        object_name (str): S3 object name. If not specified, file_path is used

    Returns:
        bool: True if file was uploaded, False on error
    """
    if object_name is None:
        object_name = os.path.basename(file_path)

    s3_client = boto3.client('s3', region_name=AWS_REGION)

    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info(f"Uploaded '{file_path}' to '{bucket_name}/{object_name}'")
        return True
    except ClientError as e:
        logger.error(f"Error uploading '{file_path}' to '{bucket_name}/{object_name}': {e}")
        return False
```

### Amazon EMR

Amazon EMR (Elastic MapReduce) is used for processing the data using Apache Spark.

**Configuration:**

```python
EMR_CONFIG = {
    'name': 'Car-Rental-EMR-Cluster',
    'log_uri': 's3://bucket-name/logs/',
    'release_label': 'emr-6.10.0',
    'applications': ['Spark', 'Hadoop', 'Hive', 'Livy'],
    'master_instance_type': 'm5.xlarge',
    'core_instance_type': 'm5.xlarge',
    'core_instance_count': 2,
    'ec2_key_name': 'emr-key-pair',
    'bootstrap_actions': [],
    'configurations': [
        {
            'Classification': 'spark',
            'Properties': {
                'maximizeResourceAllocation': 'true'
            }
        },
        {
            'Classification': 'spark-defaults',
            'Properties': {
                'spark.dynamicAllocation.enabled': 'true',
                'spark.executor.instances': '2',
                'spark.executor.memory': '4g',
                'spark.driver.memory': '4g'
            }
        }
    ]
}
```

**Implementation:**
The EMR integration is implemented in `scripts/create_emr_cluster.py` and `scripts/run_spark_jobs.py` and includes functions for:

- Creating EMR clusters
- Running Spark jobs on EMR
- Monitoring job execution
- Terminating EMR clusters

**Code Example:**

```python
def create_emr_cluster():
    """
    Create an EMR cluster using the configuration defined in config/emr_config.json.

    Returns:
        str: The cluster ID if successful, None otherwise
    """
    emr_client = boto3.client('emr', region_name=AWS_REGION)

    # Load EMR configuration from JSON file
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config',
        'emr_config.json'
    )

    with open(config_path, 'r') as f:
        emr_config = json.load(f)

    try:
        response = emr_client.run_job_flow(**emr_config)
        cluster_id = response['JobFlowId']
        logger.info(f"EMR cluster '{EMR_CONFIG['name']}' (ID: {cluster_id}) is being created")
        return cluster_id
    except Exception as e:
        logger.error(f"Error creating EMR cluster: {e}")
        return None
```

### AWS Glue

AWS Glue is used for cataloging the processed data, making it available for querying with Athena.

**Configuration:**

```python
GLUE_CONFIG = {
    'database_name': 'car_rental_db',
    'crawler_role': 'AWSGlueServiceRole-CarRentalCrawler',
    'crawler_name_prefix': 'car-rental-crawler-',
    'tables': {
        'vehicle_location_metrics': {
            'name': 'vehicle_location_metrics',
            'location': 's3://bucket-name/processed/vehicle_location_metrics/'
        },
        'user_transaction_analysis': {
            'name': 'user_transaction_analysis',
            'location': 's3://bucket-name/processed/user_transaction_analysis/'
        }
    }
}
```

**Implementation:**
The Glue integration is implemented in `scripts/setup_glue_crawlers.py` and includes functions for:

- Creating Glue databases
- Creating Glue crawlers
- Running Glue crawlers
- Monitoring crawler execution

**Code Example:**

```python
def create_glue_crawler(crawler_name, role, database_name, s3_target_path):
    """
    Create a Glue crawler if it doesn't exist.

    Args:
        crawler_name (str): Name of the crawler to create
        role (str): IAM role ARN for the crawler
        database_name (str): Name of the database to use
        s3_target_path (str): S3 path to crawl

    Returns:
        bool: True if crawler was created or already exists, False on error
    """
    glue_client = boto3.client('glue', region_name=AWS_REGION)

    try:
        # Check if crawler already exists
        response = glue_client.get_crawler(Name=crawler_name)
        logger.info(f"Glue crawler '{crawler_name}' already exists")
        return True
    except glue_client.exceptions.EntityNotFoundException:
        # Create the crawler
        try:
            glue_client.create_crawler(
                Name=crawler_name,
                Role=role,
                DatabaseName=database_name,
                Targets={
                    'S3Targets': [
                        {
                            'Path': s3_target_path
                        }
                    ]
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DELETE_FROM_DATABASE'
                },
                Configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}',
                RecrawlPolicy={
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                }
            )
            logger.info(f"Glue crawler '{crawler_name}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating Glue crawler '{crawler_name}': {e}")
            return False
    except Exception as e:
        logger.error(f"Error checking Glue crawler '{crawler_name}': {e}")
        return False
```

### Amazon Athena

Amazon Athena is used for querying the processed data using SQL.

**Implementation:**
The Athena integration is demonstrated in `notebooks/athena_queries.ipynb` and includes functions for:

- Running SQL queries on Athena
- Retrieving query results
- Visualizing query results

**Code Example:**

```python
def run_athena_query(query, database, s3_output):
    """
    Run a query on Athena and return the results as a pandas DataFrame.

    Args:
        query (str): The SQL query to run
        database (str): The Athena database to query
        s3_output (str): The S3 location to store query results

    Returns:
        DataFrame: The query results as a pandas DataFrame
    """
    athena_client = boto3.client('athena', region_name=AWS_REGION)
    s3_client = boto3.client('s3', region_name=AWS_REGION)

    # Start the query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output
        }
    )

    query_execution_id = response['QueryExecutionId']
    print(f"Query execution ID: {query_execution_id}")

    # Wait for the query to complete
    state = 'RUNNING'
    while state in ['RUNNING', 'QUEUED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']

        if state in ['RUNNING', 'QUEUED']:
            print(f"Query is {state}. Waiting...")
            time.sleep(5)

    # Check if the query succeeded
    if state == 'SUCCEEDED':
        print("Query succeeded!")

        # Get the results
        result_file = f"{s3_output.rstrip('/')}/{query_execution_id}.csv"
        bucket_name = result_file.split('//')[1].split('/')[0]
        key = '/'.join(result_file.split('//')[1].split('/')[1:])

        # Download the results
        local_file = 'athena_query_result.csv'
        s3_client.download_file(bucket_name, key, local_file)

        # Load the results into a DataFrame
        df = pd.read_csv(local_file)

        # Clean up the local file
        os.remove(local_file)

        return df
    else:
        error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        print(f"Query failed: {error_message}")
        return None
```

**Sample Queries:**

```sql
-- Highest Revenue-Generating Locations
SELECT
    location_id,
    location_name,
    city,
    state,
    total_revenue
FROM
    location_metrics
ORDER BY
    total_revenue DESC
LIMIT 10;

-- Most Rented Vehicle Types
SELECT
    vehicle_type,
    total_rentals,
    total_revenue,
    avg_rental_amount,
    avg_rental_duration_hours
FROM
    vehicle_type_metrics
ORDER BY
    total_rentals DESC;

-- Top-Spending Users
SELECT
    user_id,
    first_name,
    last_name,
    total_spent,
    total_rentals,
    avg_rental_amount,
    spending_category
FROM
    user_metrics
WHERE
    spending_category = 'Top Spender'
ORDER BY
    total_spent DESC
LIMIT 20;
```

### AWS Step Functions

AWS Step Functions is used for orchestrating the entire data pipeline.

**Configuration:**

```python
STEP_FUNCTIONS_CONFIG = {
    'state_machine_name': 'CarRentalDataPipeline',
    'role_arn': 'arn:aws:iam::123456789012:role/StepFunctionsExecutionRole',
}
```

**Implementation:**
The Step Functions integration is implemented in `scripts/deploy_step_functions.py` and `scripts/execute_step_functions.py` and includes functions for:

- Creating Step Functions state machines
- Updating existing state machines
- Executing state machines
- Monitoring execution

**Workflow Definition:**
The workflow is defined in `step_functions/data_pipeline_workflow.json` and includes the following steps:

1. Create EMR Cluster
2. Run Vehicle Location Metrics Job
3. Run User Transaction Analysis Job
4. Start Glue Crawlers
5. Terminate EMR Cluster

**Code Example:**

```python
def create_or_update_state_machine(state_machine_name, role_arn, definition):
    """
    Create or update a Step Functions state machine.

    Args:
        state_machine_name (str): Name of the state machine
        role_arn (str): IAM role ARN for the state machine
        definition (str): State machine definition JSON

    Returns:
        str: The ARN of the state machine if successful, None otherwise
    """
    sfn_client = boto3.client('stepfunctions', region_name=AWS_REGION)

    # Check if state machine already exists
    try:
        response = sfn_client.list_state_machines()
        for state_machine in response['stateMachines']:
            if state_machine['name'] == state_machine_name:
                # Update existing state machine
                logger.info(f"Updating existing state machine '{state_machine_name}'")
                response = sfn_client.update_state_machine(
                    stateMachineArn=state_machine['stateMachineArn'],
                    definition=definition,
                    roleArn=role_arn
                )
                logger.info(f"State machine '{state_machine_name}' updated successfully")
                return state_machine['stateMachineArn']

        # Create new state machine
        logger.info(f"Creating new state machine '{state_machine_name}'")
        response = sfn_client.create_state_machine(
            name=state_machine_name,
            definition=definition,
            roleArn=role_arn,
            type='STANDARD',
            loggingConfiguration={
                'level': 'ALL',
                'includeExecutionData': True
            }
        )
        logger.info(f"State machine '{state_machine_name}' created successfully")
        return response['stateMachineArn']

    except Exception as e:
        logger.error(f"Error creating/updating state machine '{state_machine_name}': {e}")
        return None
```

### AWS Lambda

AWS Lambda is used for starting Glue crawlers as part of the Step Functions workflow.

**Implementation:**
The Lambda function is implemented in `scripts/lambda_start_glue_crawlers.py` and includes functions for:

- Starting Glue crawlers
- Monitoring crawler execution
- Returning results to Step Functions

**Code Example:**

```python
def lambda_handler(event, context):
    """
    Lambda function handler to start Glue crawlers.

    Args:
        event (dict): Event data from Step Functions
        context (object): Lambda context

    Returns:
        dict: Result of the operation
    """
    logger.info("Starting Glue crawlers")

    # Get parameters from the event
    database = event.get('database')
    crawlers = event.get('crawlers', [])

    if not database or not crawlers:
        error_message = "Missing required parameters: database and/or crawlers"
        logger.error(error_message)
        return {
            'statusCode': 400,
            'error': error_message
        }

    glue_client = boto3.client('glue')
    results = {}

    # Start each crawler
    for crawler_name in crawlers:
        try:
            # Check if crawler is already running
            response = glue_client.get_crawler(Name=crawler_name)
            crawler_state = response['Crawler']['State']

            if crawler_state == 'RUNNING':
                logger.info(f"Crawler '{crawler_name}' is already running")
                results[crawler_name] = 'ALREADY_RUNNING'
                continue

            # Start the crawler
            glue_client.start_crawler(Name=crawler_name)
            logger.info(f"Started crawler '{crawler_name}'")
            results[crawler_name] = 'STARTED'

        except glue_client.exceptions.EntityNotFoundException:
            error_message = f"Crawler '{crawler_name}' not found"
            logger.error(error_message)
            results[crawler_name] = 'NOT_FOUND'

        except Exception as e:
            error_message = f"Error starting crawler '{crawler_name}': {str(e)}"
            logger.error(error_message)
            results[crawler_name] = 'ERROR'

    # Wait for all crawlers to complete (with timeout)
    timeout_seconds = 900  # 15 minutes
    start_time = time.time()
    all_completed = False

    while not all_completed and time.time() - start_time < timeout_seconds:
        all_completed = True

        for crawler_name in crawlers:
            try:
                response = glue_client.get_crawler(Name=crawler_name)
                crawler_state = response['Crawler']['State']

                if crawler_state == 'RUNNING':
                    logger.info(f"Crawler '{crawler_name}' is still running")
                    all_completed = False
                    break

            except Exception as e:
                logger.error(f"Error checking crawler '{crawler_name}' status: {str(e)}")

        if not all_completed:
            time.sleep(30)  # Wait for 30 seconds before checking again

    # Check final status of all crawlers
    for crawler_name in crawlers:
        try:
            response = glue_client.get_crawler(Name=crawler_name)
            last_crawl = response['Crawler'].get('LastCrawl', {})
            status = last_crawl.get('Status')

            results[crawler_name] = f"COMPLETED: {status}"
            logger.info(f"Crawler '{crawler_name}' completed with status: {status}")

        except Exception as e:
            logger.error(f"Error checking final status of crawler '{crawler_name}': {str(e)}")

    return {
        'statusCode': 200,
        'database': database,
        'crawlers': results
    }
```

## IAM Roles and Permissions

The project requires several IAM roles with specific permissions. We provide a script to automate the creation of these roles and their associated permissions.

### IAM Setup Script

The `scripts/setup_iam_roles.py` script creates all the necessary IAM roles and attaches the appropriate policies. It can be executed directly or through wrapper scripts:

```bash
# Using the Python script directly
python scripts/setup_iam_roles.py --region us-east-1

# Using the Bash wrapper script
bash scripts/setup_iam_roles.sh --region us-east-1

# Using the PowerShell wrapper script (Windows)
.\scripts\setup_iam_roles.ps1 -Region us-east-1
```

The script performs the following actions:
1. Creates IAM roles if they don't exist
2. Attaches AWS managed policies to the roles
3. Creates custom policies for specific permissions
4. Sets up instance profiles for EC2 roles
5. Saves the role ARNs to a file for reference

### Required IAM Roles

The project requires the following IAM roles:

#### EMR Service Role

This role allows EMR to access other AWS services on your behalf.

**Role Name:** `EMR_DefaultRole`

**Required Permissions:**

- `elasticmapreduce:*`
- `s3:GetObject`
- `s3:PutObject`
- `s3:ListBucket`

#### EMR EC2 Instance Profile

This role allows EC2 instances in the EMR cluster to access other AWS services.

**Role Name:** `EMR_EC2_DefaultRole`

**Required Permissions:**

- `s3:GetObject`
- `s3:PutObject`
- `s3:ListBucket`
- `cloudwatch:PutMetricData`
- `dynamodb:*` (if using DynamoDB)

#### Glue Service Role

This role allows Glue to access S3 and other AWS services.

**Role Name:** `AWSGlueServiceRole-CarRentalCrawler`

**Required Permissions:**

- `glue:*`
- `s3:GetObject`
- `s3:PutObject`
- `s3:ListBucket`

#### Step Functions Execution Role

This role allows Step Functions to invoke other AWS services.

**Role Name:** `StepFunctionsExecutionRole`

**Required Permissions:**

- `lambda:InvokeFunction`
- `elasticmapreduce:*`
- `states:*`
- `events:*`

#### Lambda Execution Role

This role allows Lambda functions to execute and access other AWS services.

**Role Name:** `LambdaGlueCrawlerRole`

**Required Permissions:**

- `logs:CreateLogGroup`
- `logs:CreateLogStream`
- `logs:PutLogEvents`
- `glue:StartCrawler`
- `glue:GetCrawler`
- `glue:GetCrawlers`

## Environment Setup

The project uses environment variables for configuration, which are loaded from a `.env` file:

```bash
# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=us-east-1

# S3 Configuration
S3_BUCKET_NAME=car-rental-data-lake

# EMR Configuration
EMR_CLUSTER_NAME=Car-Rental-EMR-Cluster
EMR_RELEASE_LABEL=emr-6.10.0
EMR_EC2_KEY_NAME=emr-key-pair
EMR_MASTER_INSTANCE_TYPE=m5.xlarge
EMR_CORE_INSTANCE_TYPE=m5.xlarge
EMR_CORE_INSTANCE_COUNT=2

# IAM Roles
EMR_SERVICE_ROLE=EMR_DefaultRole
EMR_EC2_INSTANCE_PROFILE=EMR_EC2_DefaultRole
GLUE_SERVICE_ROLE=AWSGlueServiceRole-CarRentalCrawler
STEP_FUNCTIONS_ROLE_ARN=arn:aws:iam::123456789012:role/StepFunctionsExecutionRole

# Glue Configuration
GLUE_DATABASE_NAME=car_rental_db

# Step Functions Configuration
STEP_FUNCTIONS_STATE_MACHINE_NAME=CarRentalDataPipeline
```

The environment variables are loaded using the `python-dotenv` package and accessed through utility functions in `config/env_loader.py`.

## Deployment Scripts

The project includes several scripts for deploying and managing the AWS resources:

- `scripts/setup_iam_roles.py`: Sets up all required IAM roles and permissions
- `scripts/setup_aws_environment.py`: Sets up the S3 bucket and uploads data
- `scripts/create_emr_cluster.py`: Creates an EMR cluster
- `scripts/run_spark_jobs.py`: Runs Spark jobs on EMR
- `scripts/setup_glue_crawlers.py`: Sets up Glue crawlers
- `scripts/deploy_step_functions.py`: Deploys the Step Functions workflow
- `scripts/execute_step_functions.py`: Executes the Step Functions workflow
- `scripts/terminate_emr_cluster.py`: Terminates an EMR cluster

These scripts can be run individually or orchestrated through the main script:

```bash
python main.py
```

## Monitoring and Logging

The project includes comprehensive logging and monitoring:

### Logging

- **Python Logging**: All scripts use the Python logging module to log information, warnings, and errors
- **EMR Logs**: EMR logs are stored in S3 and can be viewed in the EMR console
- **Glue Logs**: Glue crawler logs can be viewed in the Glue console
- **Step Functions Logs**: Step Functions execution logs can be viewed in the Step Functions console

### Monitoring

- **EMR Monitoring**: EMR provides monitoring through CloudWatch metrics
- **Glue Monitoring**: Glue provides monitoring for crawler executions
- **Step Functions Monitoring**: Step Functions provides monitoring for workflow executions

## Cost Optimization

The project includes several cost optimization strategies:

1. **EMR Cluster Termination**: The EMR cluster is terminated after the jobs are completed
2. **Spot Instances**: The EMR cluster can be configured to use spot instances for cost savings
3. **Resource Sizing**: The EMR cluster is sized appropriately for the workload
4. **S3 Storage Classes**: The processed data can be stored in S3 with appropriate storage classes

## Security Considerations

The project follows AWS security best practices:

1. **IAM Roles**: Least privilege principle is applied to IAM roles
2. **Environment Variables**: Sensitive information is stored in environment variables
3. **S3 Encryption**: Data in S3 can be encrypted at rest
4. **VPC Configuration**: EMR clusters can be deployed in a VPC for network isolation
5. **Security Groups**: EMR clusters use security groups to control network access
