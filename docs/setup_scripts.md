# Setup Scripts Documentation

This document explains the purpose and usage of the setup scripts in the Big Data EMR project.

## Overview

The project includes several setup scripts that handle different aspects of the AWS environment setup:

1. **setup_s3_bucket.py**: Creates the S3 bucket and sets up the folder structure.
2. **setup_aws_environment.py**: Checks if the S3 bucket exists and uploads data files and scripts.
3. **setup_iam_roles.py**: Sets up the IAM roles and policies required for the project.

These scripts are designed to be run in sequence, with each script handling a specific aspect of the setup process.

## Script Details

### setup_s3_bucket.py

**Purpose**: Create the S3 bucket and set up the folder structure.

**Usage**:

```bash
python -m scripts.setup_s3_bucket [--bucket-name BUCKET_NAME] [--region REGION]
```

**Parameters**:

- `--bucket-name`: Name of the S3 bucket to create (default: value from .env file)
- `--region`: AWS region (default: value from AWS CLI configuration)

**Behavior**:

1. Creates the S3 bucket if it doesn't exist
2. Sets up the folder structure in the bucket based on the configuration in S3_CONFIG
3. Does NOT upload any data or scripts (this is handled by setup_aws_environment.py)

**Example**:

```bash
python -m scripts.setup_s3_bucket --bucket-name my-emr-bucket --region us-west-2
```

### setup_aws_environment.py

**Purpose**: Check if the S3 bucket exists and upload data files and scripts.

**Usage**:

```bash
python -m scripts.setup_aws_environment [--bucket-name BUCKET_NAME] [--region REGION]
```

**Parameters**:

- `--bucket-name`: Name of the S3 bucket (default: value from .env file)
- `--region`: AWS region (default: value from AWS CLI configuration)

**Behavior**:

1. Checks if the S3 bucket exists and is accessible
2. Uploads data files from the data directory to the appropriate folders in the bucket
3. Uploads Spark scripts from the spark directory to the scripts folder in the bucket
4. Does NOT create the bucket (this is handled by setup_s3_bucket.py)

**Example**:

```bash
python -m scripts.setup_aws_environment --bucket-name my-emr-bucket --region us-west-2
```

### setup_iam_roles.py

**Purpose**: Set up the IAM roles and policies required for the project.

**Usage**:

```bash
python -m scripts.setup_iam_roles [--region REGION]
```

**Parameters**:

- `--region`: AWS region (default: value from AWS CLI configuration)

**Behavior**:

1. Creates the IAM roles required for EMR, Glue, Step Functions, and Lambda
2. Sets up trust relationships for the roles
3. Attaches managed policies to the roles
4. Creates and attaches custom policies for S3 access
5. Creates instance profiles for the roles
6. Updates the .env file with the role information

**Example**:

```bash
python -m scripts.setup_iam_roles --region us-west-2
```

## Execution Order

The scripts should be executed in the following order:

1. **setup_s3_bucket.py**: Create the S3 bucket and folder structure
2. **setup_iam_roles.py**: Set up the IAM roles and policies
3. **setup_aws_environment.py**: Upload data files and scripts to the S3 bucket

This order ensures that each script has the necessary resources created by the previous scripts.

## S3 Utility Functions

The scripts use a shared set of utility functions for S3 operations, defined in `utils/s3_utils.py`. These functions include:

- `check_s3_bucket_exists`: Check if an S3 bucket exists and is accessible
- `create_s3_bucket`: Create an S3 bucket in the specified region
- `create_folder_structure`: Create the folder structure in the S3 bucket
- `upload_file_to_s3`: Upload a file to an S3 bucket
- `upload_data_files`: Upload data files to the S3 bucket
- `upload_spark_scripts`: Upload Spark scripts to the S3 bucket
- `upload_data_to_s3`: Upload all data files from a directory to an S3 bucket

These utility functions ensure consistent behavior across the scripts and reduce code duplication.

## Environment Variables

The scripts use environment variables for configuration, loaded from the `.env` file at the project root. Key environment variables include:

- `S3_BUCKET_NAME`: Name of the S3 bucket
- `AWS_REGION`: AWS region
- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key

See the [Environment Variables Documentation](environment_variables.md) for more details on how environment variables are managed in the project.

## Troubleshooting

If you encounter issues with the setup scripts:

1. **Check AWS Credentials**: Ensure your AWS credentials are correctly set up in the `.env` file or AWS CLI configuration.

2. **Check Bucket Name**: Ensure the S3 bucket name is unique and follows AWS naming conventions.

3. **Check Permissions**: Ensure your AWS user has the necessary permissions to create S3 buckets and IAM roles.

4. **Check Region**: Ensure the AWS region is correctly specified and supported.

5. **Check Logs**: The scripts include detailed logging to help diagnose issues.

## Conclusion

The setup scripts provide a streamlined way to set up the AWS environment for the Big Data EMR project. By separating the setup process into distinct scripts, each with a specific responsibility, the setup process is more maintainable and easier to understand.
