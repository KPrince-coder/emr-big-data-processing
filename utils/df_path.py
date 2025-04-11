"""S3 Path Utility

This module provides utility functions for constructing S3 paths for data files.
It helps standardize the way S3 paths are constructed throughout the application,
making it easier to maintain consistent path structures.

Example:
    >>> from utils.df_path import df_path
    >>>
    >>> s3_base_path = "s3://my-bucket/data/"
    >>> users_path = df_path("users.csv", s3_base_path)
    >>> print(users_path)  # Output: s3://my-bucket/data/users.csv
"""


def df_path(file_name: str, s3_path: str) -> str:
    """
    Construct a complete S3 path by combining a base path with a file name.

    This function creates a properly formatted S3 path by appending a file name
    to a base S3 path. It ensures that the path is correctly formatted regardless
    of whether the base path ends with a slash or not.

    Args:
        file_name (str): The name of the file (e.g., "users.csv")
        s3_path (str): The base S3 path (e.g., "s3://my-bucket/data/")

    Returns:
        str: The complete S3 path to the file

    Example:
        >>> df_path("users.csv", "s3://my-bucket/data/")
        's3://my-bucket/data/users.csv'
    """
    # Ensure s3_path ends with a slash if it doesn't already
    if not s3_path.endswith("/"):
        s3_path = f"{s3_path}/"

    return f"{s3_path}{file_name}"
