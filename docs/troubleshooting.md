# Troubleshooting Guide

This document provides solutions to common issues that may arise when working with the Big Data Processing with AWS EMR project.

## Table of Contents

1. [AWS Credentials Issues](#aws-credentials-issues)
2. [S3 Issues](#s3-issues)
3. [EMR Cluster Issues](#emr-cluster-issues)
4. [Spark Job Issues](#spark-job-issues)
5. [Glue Crawler Issues](#glue-crawler-issues)
6. [Athena Query Issues](#athena-query-issues)
7. [Step Functions Issues](#step-functions-issues)
8. [Environment Variables Issues](#environment-variables-issues)
9. [Python Dependencies Issues](#python-dependencies-issues)
10. [Performance Issues](#performance-issues)

## AWS Credentials Issues

### Issue: AWS credentials not found

**Symptoms:**

- Error message: `botocore.exceptions.NoCredentialsError: Unable to locate credentials`
- AWS API calls fail with authentication errors

**Solutions:**

1. Check if the AWS credentials are correctly set in the `.env` file:

   ```bash
   AWS_ACCESS_KEY_ID=your_access_key_here
   AWS_SECRET_ACCESS_KEY=your_secret_key_here
   AWS_REGION=us-east-1
   ```

2. Verify that the `.env` file is in the correct location (project root directory)

3. Check if the environment variables are being loaded correctly:

   ```python
   import os
   print(os.environ.get('AWS_ACCESS_KEY_ID'))
   print(os.environ.get('AWS_SECRET_ACCESS_KEY'))
   print(os.environ.get('AWS_REGION'))
   ```

4. Try configuring AWS credentials using the AWS CLI:

   ```bash
   aws configure
   ```

### Issue: Insufficient permissions

**Symptoms:**

- Error message: `An error occurred (AccessDenied) when calling the CreateBucket operation: Access Denied`
- AWS API calls fail with permission errors

**Solutions:**

1. Check if the IAM user has the necessary permissions for the AWS services being used

2. Verify the IAM policies attached to the user:

   ```bash
   aws iam list-attached-user-policies --user-name your-user-name
   ```

3. If using IAM roles, check if the roles have the necessary permissions:

   ```bash
   aws iam list-role-policies --role-name your-role-name
   ```

4. Consider using the AWS Policy Simulator to test permissions:
   <https://policysim.aws.amazon.com/>

## S3 Issues

### Issue: S3 bucket already exists

**Symptoms:**

- Error message: `botocore.exceptions.ClientError: An error occurred (BucketAlreadyExists) when calling the CreateBucket operation: The requested bucket name is not available.`

**Solutions:**

1. Use a different bucket name in the `.env` file:

   ```bash
   S3_BUCKET_NAME=your-unique-bucket-name
   ```

2. Check if the bucket already exists and is owned by you:

   ```bash
   aws s3 ls | grep your-bucket-name
   ```

3. If the bucket exists and is owned by you, modify the script to use the existing bucket instead of creating a new one

### Issue: Failed to upload files to S3

**Symptoms:**

- Error message: `botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the PutObject operation: Access Denied`
- Files are not appearing in the S3 bucket

**Solutions:**

1. Check if the IAM user has the necessary permissions for S3:
   - `s3:PutObject`
   - `s3:GetObject`
   - `s3:ListBucket`

2. Verify that the bucket exists and is accessible:

   ```bash
   aws s3 ls s3://your-bucket-name/
   ```

3. Check if the bucket policy allows the IAM user to upload files:

   ```bash
   aws s3api get-bucket-policy --bucket your-bucket-name
   ```

4. Try uploading a test file using the AWS CLI:

   ```bash
   echo "test" > test.txt
   aws s3 cp test.txt s3://your-bucket-name/
   ```

## EMR Cluster Issues

### Issue: EMR cluster creation fails

**Symptoms:**

- Error message: `botocore.exceptions.ClientError: An error occurred (ValidationException) when calling the RunJobFlow operation: ...`
- EMR cluster status shows as `TERMINATED_WITH_ERRORS`

**Solutions:**

1. Check the EMR cluster configuration in `config/emr_config.json`:
   - Verify that the instance types are valid
   - Verify that the EC2 key pair exists
   - Verify that the IAM roles exist

2. Check the EMR error message for specific issues:

   ```bash
   aws emr describe-cluster --cluster-id your-cluster-id
   ```

3. Verify that the IAM roles have the necessary permissions:
   - `EMR_DefaultRole` for the service role
   - `EMR_EC2_DefaultRole` for the EC2 instance profile

4. Check if the VPC and subnet configurations are valid:

   ```bash
   aws ec2 describe-subnets --subnet-ids your-subnet-id
   ```

5. Verify that the security groups allow the necessary traffic:

   ```bash
   aws ec2 describe-security-groups --group-ids your-security-group-id
   ```

### Issue: EMR cluster terminates unexpectedly

**Symptoms:**

- EMR cluster status changes from `RUNNING` to `TERMINATED` or `TERMINATED_WITH_ERRORS`
- Spark jobs fail with cluster termination errors

**Solutions:**

1. Check the EMR cluster logs in the S3 bucket:

   ```bash
   s3://your-bucket-name/logs/your-cluster-id/
   ```

2. Check the EMR cluster error message:

   ```bash
   aws emr describe-cluster --cluster-id your-cluster-id
   ```

3. Verify that the EC2 instances have sufficient resources:
   - Check if the instances are being terminated due to spot instance interruptions
   - Check if the instances are running out of memory or disk space

4. Check if the EMR cluster is being terminated by a step:

   ```bash
   aws emr list-steps --cluster-id your-cluster-id
   ```

5. Verify that the EMR cluster is not being terminated by a Step Functions workflow:

   ```bash
   aws stepfunctions list-executions --state-machine-arn your-state-machine-arn
   ```

## Spark Job Issues

### Issue: Spark job fails with errors

**Symptoms:**

- Error message in EMR step logs
- Spark job status shows as `FAILED`

**Solutions:**

1. Check the Spark job logs in the EMR cluster:

   ```markdown
   s3://your-bucket-name/logs/your-cluster-id/steps/your-step-id/
   ```

2. Check the Spark driver and executor logs:

   ```markdown
   s3://your-bucket-name/logs/your-cluster-id/containers/
   ```

3. Verify that the Spark job has the necessary resources:
   - Check if the job is running out of memory
   - Check if the job is running out of disk space

4. Check if the Spark job is accessing the correct data:
   - Verify that the S3 paths are correct
   - Verify that the data exists in S3

5. Try running the Spark job in client mode for interactive debugging:

   ```bash
   aws emr add-steps --cluster-id your-cluster-id --steps Type=CUSTOM_JAR,Name=SparkJob,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[spark-submit,--deploy-mode,client,s3://your-bucket-name/scripts/your-spark-job.py]
   ```

### Issue: Spark job runs slowly

**Symptoms:**

- Spark job takes a long time to complete
- Spark job progress is slow

**Solutions:**

1. Check the Spark job configuration:
   - Verify that the job is using the appropriate number of executors
   - Verify that the job is using the appropriate amount of memory

2. Check if the job is properly parallelized:
   - Verify that the data is properly partitioned
   - Verify that the job is using the appropriate number of partitions

3. Check if the job is using efficient transformations:
   - Avoid using `collect()` on large datasets
   - Use broadcast joins for small datasets
   - Use appropriate caching strategies

4. Consider increasing the EMR cluster size:
   - Increase the number of core nodes
   - Use larger instance types

5. Monitor the Spark UI for performance bottlenecks:
   - Check the stage details for skew
   - Check the executor details for resource utilization

## Glue Crawler Issues

### Issue: Glue crawler fails

**Symptoms:**

- Error message: `botocore.exceptions.ClientError: An error occurred (AccessDeniedException) when calling the StartCrawler operation: ...`
- Glue crawler status shows as `FAILED`

**Solutions:**

1. Check the Glue crawler logs in the AWS Glue console

2. Verify that the IAM role has the necessary permissions:
   - `glue:StartCrawler`
   - `glue:GetCrawler`
   - `glue:GetDatabase`
   - `glue:CreateTable`
   - `s3:GetObject`
   - `s3:ListBucket`

3. Check if the S3 path exists and contains data:

   ```bash
   aws s3 ls s3://your-bucket-name/processed/
   ```

4. Verify that the Glue database exists:

   ```bash
   aws glue get-database --name your-database-name
   ```

5. Try running the crawler manually from the AWS Glue console

### Issue: Glue crawler doesn't create tables

**Symptoms:**

- Glue crawler completes successfully but no tables are created
- Tables are missing columns or have incorrect data types

**Solutions:**

1. Check if the S3 path contains data in a supported format:
   - Parquet
   - ORC
   - JSON
   - CSV

2. Verify that the data files have the correct schema:
   - Check if the files have headers (for CSV)
   - Check if the files have the expected columns

3. Try running the crawler with different configuration:
   - Update the crawler to use a different classification
   - Update the crawler to use a different schema change policy

4. Check if the tables already exist and need to be updated:

   ```bash
   aws glue get-tables --database-name your-database-name
   ```

5. Try deleting the existing tables and running the crawler again:

   ```bash
   aws glue delete-table --database-name your-database-name --name your-table-name
   ```

## Athena Query Issues

### Issue: Athena query fails

**Symptoms:**

- Error message: `botocore.exceptions.ClientError: An error occurred (InvalidRequestException) when calling the StartQueryExecution operation: ...`
- Query status shows as `FAILED`

**Solutions:**

1. Check the Athena query error message in the AWS Athena console

2. Verify that the IAM user has the necessary permissions:
   - `athena:StartQueryExecution`
   - `athena:GetQueryExecution`
   - `athena:GetQueryResults`
   - `s3:PutObject` (for query results)
   - `s3:GetObject` (for query results)

3. Check if the Athena database and tables exist:

   ```bash
   aws athena list-databases --catalog-name AwsDataCatalog
   aws athena list-table-metadata --catalog-name AwsDataCatalog --database-name your-database-name
   ```

4. Verify that the S3 output location exists and is writable:

   ```bash
   aws s3 ls s3://your-bucket-name/temp/athena_results/
   ```

5. Try running a simple query to test the setup:

   ```sql
   SELECT 1
   ```

### Issue: Athena query returns unexpected results

**Symptoms:**

- Query results are missing data
- Query results have incorrect values
- Query results have unexpected data types

**Solutions:**

1. Check the table schema in the AWS Glue Data Catalog:

   ```bash
   aws glue get-table --database-name your-database-name --name your-table-name
   ```

2. Verify that the data files in S3 have the expected format and content:

   ```bash
   aws s3 ls s3://your-bucket-name/processed/your-table-name/
   ```

3. Try running a query to inspect the raw data:

   ```sql
   SELECT * FROM your_database_name.your_table_name LIMIT 10
   ```

4. Check if the table partitions are correctly defined:

   ```bash
   aws glue get-partitions --database-name your-database-name --table-name your-table-name
   ```

5. Consider re-running the Glue crawler to update the table schema:

   ```bash
   aws glue start-crawler --name your-crawler-name
   ```

## Step Functions Issues

### Issue: Step Functions workflow creation fails

**Symptoms:**

- Error message: `botocore.exceptions.ClientError: An error occurred (AccessDeniedException) when calling the CreateStateMachine operation: ...`

**Solutions:**

1. Verify that the IAM user has the necessary permissions:
   - `states:CreateStateMachine`
   - `states:UpdateStateMachine`
   - `states:DescribeStateMachine`

2. Check if the IAM role ARN is correct:

   ```bash
   aws iam get-role --role-name your-role-name
   ```

3. Verify that the IAM role has the necessary permissions:
   - `states:StartExecution`
   - `states:DescribeExecution`
   - `lambda:InvokeFunction`
   - `elasticmapreduce:*`

4. Check if the state machine definition is valid:
   - Verify that the JSON is well-formed
   - Verify that the state machine definition follows the Amazon States Language

5. Try creating a simple state machine to test the setup:

   ```json
   {
     "Comment": "A simple state machine",
     "StartAt": "HelloWorld",
     "States": {
       "HelloWorld": {
         "Type": "Pass",
         "Result": "Hello, World!",
         "End": true
       }
     }
   }
   ```

### Issue: Step Functions workflow execution fails

**Symptoms:**

- Error message in the Step Functions execution history
- Execution status shows as `FAILED`

**Solutions:**

1. Check the execution history in the AWS Step Functions console

2. Verify that the IAM role has the necessary permissions for the services being used:
   - `lambda:InvokeFunction` for Lambda functions
   - `elasticmapreduce:*` for EMR clusters
   - `states:*` for nested workflows

3. Check if the resources being accessed exist:
   - Verify that the EMR cluster exists
   - Verify that the Lambda function exists
   - Verify that the S3 bucket exists

4. Check if the input data is in the expected format:
   - Verify that the input JSON is well-formed
   - Verify that the input contains the expected fields

5. Try executing the workflow with a simplified input:

   ```bash
   aws stepfunctions start-execution --state-machine-arn your-state-machine-arn --input "{}"
   ```

## Environment Variables Issues

### Issue: Environment variables not loaded

**Symptoms:**

- Error message: `KeyError: 'AWS_ACCESS_KEY_ID'`
- Environment variables are `None` when accessed

**Solutions:**

1. Check if the `.env` file exists in the project root directory

2. Verify that the `.env` file has the correct format:

   ```bash
   AWS_ACCESS_KEY_ID=your_access_key_here
   AWS_SECRET_ACCESS_KEY=your_secret_key_here
   AWS_REGION=us-east-1
   ```

3. Check if the `python-dotenv` package is installed:

   ```bash
   pip install python-dotenv
   ```

4. Verify that the environment variables are being loaded correctly:

   ```python
   from dotenv import load_dotenv
   import os
   
   load_dotenv()
   print(os.environ.get('AWS_ACCESS_KEY_ID'))
   ```

5. Try loading the environment variables manually:

   ```python
   import os
   
   os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key_here'
   os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_key_here'
   os.environ['AWS_REGION'] = 'us-east-1'
   ```

### Issue: Environment variables have incorrect values

**Symptoms:**

- AWS API calls fail with authentication errors
- Resources are created with unexpected names or configurations

**Solutions:**

1. Check the values in the `.env` file:

   ```bash
   AWS_ACCESS_KEY_ID=your_access_key_here
   AWS_SECRET_ACCESS_KEY=your_secret_key_here
   AWS_REGION=us-east-1
   ```

2. Verify that the environment variables are not being overridden elsewhere:

   ```python
   import os
   print(os.environ.get('AWS_ACCESS_KEY_ID'))
   ```

3. Check if the environment variables are being loaded in the correct order:
   - `.env` file
   - System environment variables
   - Explicit assignments

4. Try printing all environment variables to identify conflicts:

   ```python
   import os
   for key, value in os.environ.items():
       if key.startswith('AWS_'):
           print(f"{key}={value}")
   ```

5. Consider using a different prefix for project-specific environment variables:

   ```bash
   PROJECT_AWS_ACCESS_KEY_ID=your_access_key_here
   PROJECT_AWS_SECRET_ACCESS_KEY=your_secret_key_here
   PROJECT_AWS_REGION=us-east-1
   ```

## Python Dependencies Issues

### Issue: Missing dependencies

**Symptoms:**

- Error message: `ModuleNotFoundError: No module named 'boto3'`
- Import errors when running scripts

**Solutions:**

1. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Verify that the dependencies are installed:

   ```bash
   pip list | grep boto3
   ```

3. Check if the dependencies are installed in the correct Python environment:

   ```bash
   which python
   python -c "import sys; print(sys.path)"
   ```

4. Try installing the dependencies manually:

   ```bash
   pip install boto3 pyspark pandas matplotlib seaborn jupyter notebook pyarrow python-dotenv
   ```

5. Consider using a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

### Issue: Version conflicts

**Symptoms:**

- Error message: `ImportError: cannot import name 'X' from 'Y'`
- Unexpected behavior when using libraries

**Solutions:**

1. Check the installed package versions:

   ```bash
   pip list
   ```

2. Verify that the package versions meet the requirements:

   ```bash
   pip install -r requirements.txt --upgrade
   ```

3. Check for conflicts between packages:

   ```bash
   pip check
   ```

4. Try installing specific versions of packages:

   ```bash
   pip install boto3==1.26.0 pyspark==3.3.1
   ```

5. Consider using a clean virtual environment:

   ```bash
   python -m venv venv_clean
   source venv_clean/bin/activate  # On Windows: venv_clean\Scripts\activate
   pip install -r requirements.txt
   ```

## Performance Issues

### Issue: Scripts run slowly

**Symptoms:**

- Scripts take a long time to complete
- High CPU or memory usage

**Solutions:**

1. Check if the AWS resources are properly sized:
   - EMR cluster instance types and count
   - Spark executor memory and cores

2. Verify that the data is properly partitioned:
   - Check if the data is skewed
   - Check if the partitioning strategy is appropriate

3. Monitor the resource usage:
   - Check the CPU and memory usage
   - Check the disk I/O
   - Check the network I/O

4. Consider optimizing the code:
   - Use more efficient algorithms
   - Reduce unnecessary data processing
   - Use appropriate caching strategies

5. Try running the scripts with profiling:

   ```python
   import cProfile
   cProfile.run('main()')
   ```

### Issue: High AWS costs

**Symptoms:**

- Unexpected charges on AWS bill
- Resources running longer than expected

**Solutions:**

1. Check if resources are being terminated properly:
   - EMR clusters
   - EC2 instances
   - Other AWS resources

2. Verify that the resource sizing is appropriate:
   - Use smaller instance types for development
   - Use spot instances for cost savings
   - Use appropriate storage classes for S3

3. Monitor the AWS costs:
   - Use AWS Cost Explorer
   - Set up billing alerts
   - Use AWS Budgets

4. Consider implementing cost optimization strategies:
   - Terminate resources when not in use
   - Use auto-scaling for variable workloads
   - Use reserved instances for predictable workloads

5. Try using AWS Cost Anomaly Detection:

   ```bash
   aws ce create-anomaly-monitor --anomaly-monitor '{"MonitorName":"EMRCostMonitor","MonitorType":"DIMENSIONAL","MonitorDimension":"SERVICE","DimensionalValueCount":1}'
   ```
