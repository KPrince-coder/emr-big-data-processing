"""S3 Path Utility

This module provides utility functions for handling S3 paths in the Big Data Processing with EMR project.
It includes functions for constructing S3 paths, validating S3 URIs, and managing S3 path structures
for different data types and processing stages.

Example:
    >>> from utils.s3_path_utils import get_raw_data_path, get_processed_data_path
    >>>
    >>> # Get path for raw data
    >>> users_path = get_raw_data_path('users', 'my-bucket')
    >>> print(users_path)  # Output: s3://my-bucket/raw/users/
    >>>
    >>> # Get path for processed data
    >>> metrics_path = get_processed_data_path('user_metrics', 'my-bucket')
    >>> print(metrics_path)  # Output: s3://my-bucket/processed/user_metrics/
"""

from typing import Dict


def normalize_s3_path(path: str) -> str:
    """
    Normalize an S3 path to ensure it ends with a slash.

    Args:
        path (str): The S3 path to normalize

    Returns:
        str: The normalized S3 path

    Example:
        >>> normalize_s3_path('s3://my-bucket/data')
        's3://my-bucket/data/'
        >>> normalize_s3_path('s3://my-bucket/data/')
        's3://my-bucket/data/'
    """
    if not path.endswith("/"):
        return f"{path}/"
    return path


def is_valid_s3_uri(uri: str) -> bool:
    """
    Check if a string is a valid S3 URI.

    Args:
        uri (str): The URI to check

    Returns:
        bool: True if the URI is a valid S3 URI, False otherwise

    Example:
        >>> is_valid_s3_uri('s3://my-bucket/data/')
        True
        >>> is_valid_s3_uri('file:///path/to/file')
        False
    """
    return uri.startswith("s3://")


def construct_s3_uri(bucket_name: str, key: str) -> str:
    """
    Construct an S3 URI from a bucket name and key.

    Args:
        bucket_name (str): The S3 bucket name
        key (str): The S3 object key (path within the bucket)

    Returns:
        str: The constructed S3 URI

    Example:
        >>> construct_s3_uri('my-bucket', 'data/users.csv')
        's3://my-bucket/data/users.csv'
    """
    # Remove leading slash from key if present
    if key.startswith("/"):
        key = key[1:]

    return f"s3://{bucket_name}/{key}"


def get_s3_bucket_and_key(s3_uri: str) -> tuple:
    """
    Extract the bucket name and key from an S3 URI.

    Args:
        s3_uri (str): The S3 URI

    Returns:
        tuple: (bucket_name, key)

    Raises:
        ValueError: If the URI is not a valid S3 URI

    Example:
        >>> get_s3_bucket_and_key('s3://my-bucket/data/users.csv')
        ('my-bucket', 'data/users.csv')
    """
    if not is_valid_s3_uri(s3_uri):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    # Remove 's3://' prefix
    path = s3_uri[5:]

    # Split into bucket and key
    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    return bucket, key


def get_raw_data_path(
    dataset_name: str, bucket_name: str, file_extension: str = ""
) -> str:
    """
    Get the S3 path for raw data.

    Args:
        dataset_name (str): The name of the dataset (e.g., 'users', 'vehicles')
        bucket_name (str): The S3 bucket name
        file_extension (str, optional): The file extension (e.g., '.csv')

    Returns:
        str: The S3 path for the raw data

    Example:
        >>> get_raw_data_path('users', 'my-bucket', '.csv')
        's3://my-bucket/raw/users.csv'
        >>> get_raw_data_path('users', 'my-bucket')
        's3://my-bucket/raw/users/'
    """
    if file_extension:
        if not file_extension.startswith("."):
            file_extension = f".{file_extension}"
        return f"s3://{bucket_name}/raw/{dataset_name}{file_extension}"
    else:
        return normalize_s3_path(f"s3://{bucket_name}/raw/{dataset_name}")


def get_processed_data_path(dataset_name: str, bucket_name: str) -> str:
    """
    Get the S3 path for processed data.

    Args:
        dataset_name (str): The name of the dataset (e.g., 'user_metrics', 'location_metrics')
        bucket_name (str): The S3 bucket name

    Returns:
        str: The S3 path for the processed data

    Example:
        >>> get_processed_data_path('user_metrics', 'my-bucket')
        's3://my-bucket/processed/user_metrics/'
    """
    return normalize_s3_path(f"s3://{bucket_name}/processed/{dataset_name}")


def get_temp_data_path(dataset_name: str, bucket_name: str) -> str:
    """
    Get the S3 path for temporary data.

    Args:
        dataset_name (str): The name of the dataset
        bucket_name (str): The S3 bucket name

    Returns:
        str: The S3 path for the temporary data

    Example:
        >>> get_temp_data_path('intermediate_results', 'my-bucket')
        's3://my-bucket/temp/intermediate_results/'
    """
    return normalize_s3_path(f"s3://{bucket_name}/temp/{dataset_name}")


def get_scripts_path(script_name: str, bucket_name: str) -> str:
    """
    Get the S3 path for scripts.

    Args:
        script_name (str): The name of the script (e.g., 'job1.py')
        bucket_name (str): The S3 bucket name

    Returns:
        str: The S3 path for the script

    Example:
        >>> get_scripts_path('job1.py', 'my-bucket')
        's3://my-bucket/scripts/job1.py'
    """
    return f"s3://{bucket_name}/scripts/{script_name}"


def get_logs_path(log_name: str, bucket_name: str) -> str:
    """
    Get the S3 path for logs.

    Args:
        log_name (str): The name of the log (e.g., 'emr', 'glue')
        bucket_name (str): The S3 bucket name

    Returns:
        str: The S3 path for the logs

    Example:
        >>> get_logs_path('emr', 'my-bucket')
        's3://my-bucket/logs/emr/'
    """
    return normalize_s3_path(f"s3://{bucket_name}/logs/{log_name}")


def get_data_file_paths(
    bucket_name: str, raw_data_prefix: str = "raw/"
) -> Dict[str, str]:
    """
    Get the S3 paths for all data files.

    Args:
        bucket_name (str): The S3 bucket name
        raw_data_prefix (str, optional): The prefix for raw data

    Returns:
        Dict[str, str]: A dictionary mapping dataset names to S3 paths

    Example:
        >>> paths = get_data_file_paths('my-bucket')
        >>> paths['users']
        's3://my-bucket/raw/users.csv'
    """
    # Ensure the raw_data_prefix ends with a slash
    raw_data_prefix = normalize_s3_path(raw_data_prefix)

    # Define the base S3 path
    base_path = f"s3://{bucket_name}/{raw_data_prefix}"

    # Define the data file paths
    return {
        "users": f"{base_path}users.csv",
        "vehicles": f"{base_path}vehicles.csv",
        "locations": f"{base_path}locations.csv",
        "rental_transactions": f"{base_path}rental_transactions.csv",
    }


def get_output_paths(
    bucket_name: str, processed_data_prefix: str = "processed/"
) -> Dict[str, str]:
    """
    Get the S3 paths for output data.

    Args:
        bucket_name (str): The S3 bucket name
        processed_data_prefix (str, optional): The prefix for processed data

    Returns:
        Dict[str, str]: A dictionary mapping output names to S3 paths

    Example:
        >>> paths = get_output_paths('my-bucket')
        >>> paths['location_metrics']
        's3://my-bucket/processed/vehicle_location_metrics/location_metrics/'
    """
    # Ensure the processed_data_prefix ends with a slash
    processed_data_prefix = normalize_s3_path(processed_data_prefix)

    # Define the base paths for each job
    vehicle_location_base = (
        f"s3://{bucket_name}/{processed_data_prefix}vehicle_location_metrics/"
    )
    user_transaction_base = (
        f"s3://{bucket_name}/{processed_data_prefix}user_transaction_analysis/"
    )

    # Define the output paths
    return {
        # Job 1 outputs
        "location_metrics": f"{vehicle_location_base}location_metrics/",
        "vehicle_type_metrics": f"{vehicle_location_base}vehicle_type_metrics/",
        "brand_metrics": f"{vehicle_location_base}brand_metrics/",
        # Job 2 outputs
        "daily_metrics": f"{user_transaction_base}daily_metrics/",
        "user_metrics": f"{user_transaction_base}user_metrics/",
        "hourly_metrics": f"{user_transaction_base}hourly_metrics/",
        "day_of_week_metrics": f"{user_transaction_base}day_of_week_metrics/",
    }
