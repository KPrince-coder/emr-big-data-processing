"""Utilities Package

This package contains utility functions for the Big Data Processing with EMR project.
It provides helper functions for common tasks such as reading CSV files and
constructing S3 paths.

Available modules:
- read_csv_file: Functions for reading CSV files into Spark DataFrames
- df_path: Functions for constructing S3 paths

Example:
    >>> from utils.read_csv_file import df
    >>> from utils.df_path import df_path
    >>> from pyspark.sql import SparkSession
    >>> 
    >>> spark = SparkSession.builder.appName("MyApp").getOrCreate()
    >>> s3_base_path = "s3://my-bucket/data/"
    >>> file_path = df_path("users.csv", s3_base_path)
    >>> users_df = df(file_path, spark)
    >>> users_df.show(5)
"""

from utils.read_csv_file import df
from utils.df_path import df_path

__all__ = ['df', 'df_path']
