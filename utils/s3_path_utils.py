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
    dataset_name: str,
    s3_config: dict,
    bucket_name: str = None,
    file_extension: str = "",
) -> str:
    """
    Get the S3 path for raw data using the folder structure from the config.

    Args:
        dataset_name (str): The name of the dataset (e.g., 'users', 'vehicles')
        bucket_name (str, optional): The S3 bucket name. If None, uses the bucket from S3_CONFIG.
        file_extension (str, optional): The file extension (e.g., '.csv')

    Returns:
        str: The S3 path for the raw data

    Example:
        >>> get_raw_data_path('users', 'my-bucket', '.csv')
        's3://my-bucket/raw/users.csv'
        >>> get_raw_data_path('users')
        's3://bucket-from-config/raw/users/'
    """
    # Use bucket from config if not provided
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]

    # Get the folder path from config if available
    folder_path = s3_config["folders"].get(dataset_name, None)

    if folder_path is None:
        # Fall back to default path construction if dataset not in config
        folder_path = f"{s3_config['raw_data_prefix']}{dataset_name}/"

    if file_extension:
        if not file_extension.startswith("."):
            file_extension = f".{file_extension}"
        # Remove trailing slash for files
        if folder_path.endswith("/"):
            folder_path = folder_path[:-1]
        return f"s3://{bucket_name}/{folder_path}{file_extension}"
    else:
        return normalize_s3_path(f"s3://{bucket_name}/{folder_path}")


def get_processed_data_path(
    dataset_name: str, s3_config: dict, bucket_name: str = None
) -> str:
    """
    Get the S3 path for processed data using the folder structure from the config.

    Args:
        dataset_name (str): The name of the dataset (e.g., 'user_metrics', 'location_metrics')
        bucket_name (str, optional): The S3 bucket name. If None, uses the bucket from S3_CONFIG.

    Returns:
        str: The S3 path for the processed data

    Example:
        >>> get_processed_data_path('user_metrics', 'my-bucket')
        's3://my-bucket/processed/user_metrics/'
        >>> get_processed_data_path('vehicle_location_metrics')
        's3://bucket-from-config/processed/vehicle_location_metrics/'
    """
    # Use bucket from config if not provided
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]

    # Check if the dataset is one of our known processed datasets
    if dataset_name in ["vehicle_location_metrics", "user_transaction_analysis"]:
        folder_path = s3_config["folders"].get(dataset_name, None)
        if folder_path is not None:
            return normalize_s3_path(f"s3://{bucket_name}/{folder_path}")

    # Fall back to default path construction
    return normalize_s3_path(
        f"s3://{bucket_name}/{s3_config['processed_data_prefix']}{dataset_name}"
    )


def get_temp_data_path(
    dataset_name: str, s3_config: dict, bucket_name: str = None
) -> str:
    """
    Get the S3 path for temporary data using the folder structure from the config.

    Args:
        dataset_name (str): The name of the dataset
        bucket_name (str, optional): The S3 bucket name. If None, uses the bucket from S3_CONFIG.

    Returns:
        str: The S3 path for the temporary data

    Example:
        >>> get_temp_data_path('intermediate_results', 'my-bucket')
        's3://my-bucket/temp/intermediate_results/'
        >>> get_temp_data_path('athena_results')
        's3://bucket-from-config/temp/athena_results/'
    """
    # Use bucket from config if not provided
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]

    # Check if the dataset is 'athena_results'
    if dataset_name == "athena_results":
        folder_path = s3_config["folders"].get("athena_results", None)
        if folder_path is not None:
            return normalize_s3_path(f"s3://{bucket_name}/{folder_path}")

    # Fall back to default path construction
    return normalize_s3_path(
        f"s3://{bucket_name}/{s3_config['temp_data_prefix']}{dataset_name}"
    )


def get_scripts_path(script_name: str, s3_config: dict, bucket_name: str = None) -> str:
    """
    Get the S3 path for scripts using the folder structure from the config.

    Args:
        script_name (str): The name of the script (e.g., 'job1.py')
        bucket_name (str, optional): The S3 bucket name. If None, uses the bucket from S3_CONFIG.

    Returns:
        str: The S3 path for the script

    Example:
        >>> get_scripts_path('job1.py', 'my-bucket')
        's3://my-bucket/scripts/job1.py'
        >>> get_scripts_path('job1.py')
        's3://bucket-from-config/scripts/job1.py'
    """
    # Use bucket from config if not provided
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]

    # Get scripts folder from config
    scripts_folder = s3_config["folders"].get("scripts", s3_config["scripts_prefix"])

    # Ensure the folder path ends with a slash
    if not scripts_folder.endswith("/"):
        scripts_folder += "/"

    return f"s3://{bucket_name}/{scripts_folder}{script_name}"


def get_logs_path(log_name: str, s3_config: dict, bucket_name: str = None) -> str:
    """
    Get the S3 path for logs using the folder structure from the config.

    Args:
        log_name (str): The name of the log (e.g., 'emr', 'glue')
        bucket_name (str, optional): The S3 bucket name. If None, uses the bucket from S3_CONFIG.

    Returns:
        str: The S3 path for the logs

    Example:
        >>> get_logs_path('emr', 'my-bucket')
        's3://my-bucket/logs/emr/'
        >>> get_logs_path('emr')
        's3://bucket-from-config/logs/emr/'
    """
    # Use bucket from config if not provided
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]

    # Check if log_name is 'emr' and we have a logs folder in config
    if log_name == "emr" and "logs" in s3_config["folders"]:
        return normalize_s3_path(f"s3://{bucket_name}/{s3_config['folders']['logs']}")

    # Fall back to default path construction
    return normalize_s3_path(f"s3://{bucket_name}/logs/{log_name}")


def get_data_file_paths(
    s3_config: dict, bucket_name: str = None, raw_data_prefix: str = None
) -> Dict[str, str]:
    """
    Get the S3 paths for all data files using the folder structure from the config.

    Args:
        bucket_name (str, optional): The S3 bucket name. If None, uses the bucket from S3_CONFIG.
        raw_data_prefix (str, optional): The prefix for raw data. If None, uses the prefix from S3_CONFIG.

    Returns:
        Dict[str, str]: A dictionary mapping dataset names to S3 paths

    Example:
        >>> paths = get_data_file_paths('my-bucket')
        >>> paths['users']
        's3://my-bucket/raw/users.csv'
        >>> paths = get_data_file_paths()
        >>> paths['vehicles']
        's3://bucket-from-config/raw/vehicles.csv'
    """
    # Use bucket from config if not provided
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]

    # Use raw_data_prefix from config if not provided
    if raw_data_prefix is None:
        raw_data_prefix = s3_config["raw_data_prefix"]

    # Ensure the raw_data_prefix ends with a slash
    raw_data_prefix = normalize_s3_path(raw_data_prefix)

    # Define the data file paths using the folder structure from config
    data_files = {}

    # Add paths for raw data files
    for dataset in ["users", "vehicles", "locations", "rental_transactions"]:
        # Get folder path from config if available
        folder_path = s3_config["folders"].get(dataset, f"{raw_data_prefix}{dataset}/")

        # Remove trailing slash for files
        if folder_path.endswith("/"):
            folder_path = folder_path[:-1]

        data_files[dataset] = f"s3://{bucket_name}/{folder_path}.csv"

    return data_files


def get_output_paths(
    s3_config: dict, bucket_name: str = None, processed_data_prefix: str = None
) -> Dict[str, str]:
    """
    Get the S3 paths for output data using the folder structure from the config.

    Args:
        bucket_name (str, optional): The S3 bucket name. If None, uses the bucket from S3_CONFIG.
        processed_data_prefix (str, optional): The prefix for processed data. If None, uses the prefix from S3_CONFIG.

    Returns:
        Dict[str, str]: A dictionary mapping output names to S3 paths

    Example:
        >>> paths = get_output_paths('my-bucket')
        >>> paths['location_metrics']
        's3://my-bucket/processed/vehicle_location_metrics/location_metrics/'
        >>> paths = get_output_paths()
        >>> paths['user_metrics']
        's3://bucket-from-config/processed/user_transaction_analysis/user_metrics/'
    """
    # Use bucket from config if not provided
    if bucket_name is None:
        bucket_name = s3_config["bucket_name"]

    # Use processed_data_prefix from config if not provided
    if processed_data_prefix is None:
        processed_data_prefix = s3_config["processed_data_prefix"]

    # Ensure the processed_data_prefix ends with a slash
    processed_data_prefix = normalize_s3_path(processed_data_prefix)

    # Get the base paths for each job from config if available
    vehicle_location_base = normalize_s3_path(
        f"s3://{bucket_name}/{s3_config['folders'].get('vehicle_location_metrics', f'{processed_data_prefix}vehicle_location_metrics/')}"
    )

    user_transaction_base = normalize_s3_path(
        f"s3://{bucket_name}/{s3_config['folders'].get('user_transaction_analysis', f'{processed_data_prefix}user_transaction_analysis/')}"
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
