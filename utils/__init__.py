"""Utilities Package

This package contains utility functions for the Big Data Processing with EMR project.
It provides helper functions for common tasks such as reading CSV files and
constructing S3 paths.

Available modules:
- read_csv_file: Functions for reading CSV files into Spark DataFrames
- df_path: Functions for constructing S3 paths
- s3_path_utils: Comprehensive utilities for handling S3 paths

Example using df_path:
    >>> from utils.read_csv_file import df
    >>> from utils.df_path import df_path
    >>> from pyspark.sql import SparkSession
    >>>
    >>> spark = SparkSession.builder.appName("MyApp").getOrCreate()
    >>> s3_base_path = "s3://my-bucket/data/"
    >>> file_path = df_path("users.csv", s3_base_path)
    >>> users_df = df(file_path, spark)
    >>> users_df.show(5)

Example using s3_path_utils:
    >>> from utils.read_csv_file import df
    >>> from utils.s3_path_utils import get_data_file_paths, get_output_paths
    >>> from pyspark.sql import SparkSession
    >>>
    >>> spark = SparkSession.builder.appName("MyApp").getOrCreate()
    >>> bucket_name = "my-bucket"
    >>>
    >>> # Get input data paths
    >>> data_paths = get_data_file_paths(bucket_name)
    >>> users_df = df(data_paths["users"], spark)
    >>>
    >>> # Get output paths
    >>> output_paths = get_output_paths(bucket_name)
    >>> users_df.write.mode("overwrite").parquet(output_paths["user_metrics"])
"""

from utils.read_csv_file import df
from utils.df_path import df_path
from utils.s3_path_utils import (
    get_data_file_paths,
    get_output_paths,
    get_raw_data_path,
    get_processed_data_path,
    get_temp_data_path,
    get_scripts_path,
    get_logs_path,
    normalize_s3_path,
    construct_s3_uri,
    get_s3_bucket_and_key,
    is_valid_s3_uri,
)

__all__ = [
    # read_csv_file exports
    "df",
    # df_path exports
    "df_path",
    # s3_path_utils exports
    "get_data_file_paths",
    "get_output_paths",
    "get_raw_data_path",
    "get_processed_data_path",
    "get_temp_data_path",
    "get_scripts_path",
    "get_logs_path",
    "normalize_s3_path",
    "construct_s3_uri",
    "get_s3_bucket_and_key",
    "is_valid_s3_uri",
]
