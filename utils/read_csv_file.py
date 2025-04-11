"""CSV File Reader Utility

This module provides utility functions for reading CSV files from S3 or local filesystem
using PySpark. It simplifies the process of loading data into Spark DataFrames with
appropriate schema inference and header handling.

Example:
    >>> from utils.read_csv_file import df
    >>> from pyspark.sql import SparkSession
    >>>
    >>> spark = SparkSession.builder.appName("MyApp").getOrCreate()
    >>> data_df = df("s3://my-bucket/data/users.csv", spark)
    >>> data_df.show(5)
"""

from pyspark.sql import DataFrame, SparkSession


def df(file_path: str, spark: SparkSession) -> DataFrame:
    """
    Load a CSV file into a Spark DataFrame from S3 or local filesystem.

    This function reads a CSV file with header and automatically infers the schema
    based on the data. It handles both local file paths and S3 URIs.

    Args:
        file_path (str): The file path or S3 URI to the CSV file
        spark (SparkSession): The active Spark session

    Returns:
        DataFrame: The loaded Spark DataFrame with inferred schema

    Raises:
        AnalysisException: If the file doesn't exist or cannot be accessed

    Note:
        This function assumes the CSV file has a header row and uses comma as delimiter.
        For more advanced CSV reading options, use spark.read.csv() directly with
        additional parameters.
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)
