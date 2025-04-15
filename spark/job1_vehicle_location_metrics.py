#!/usr/bin/env python3
"""
Spark Job 1: Vehicle and Location Performance Metrics

This PySpark job calculates key metrics by location and vehicle type:
- Revenue per location
- Total transactions per location
- Average, max, and min transaction amounts
- Unique vehicles used at each location
- Rental duration and revenue by vehicle type

Input: Raw data from S3
Output: Transformed data in Parquet format in S3
"""

import sys
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    min,
    countDistinct,
    round,
    unix_timestamp,
)

# Try to import utility functions for S3 path handling
try:
    from utils.read_csv_file import df
    from utils.s3_path_utils import get_data_file_paths, get_output_paths
except ImportError:
    # Define fallback functions if utils module is not available
    def df(path: str, spark: SparkSession, schema=None) -> DataFrame:
        """
        Read a CSV file into a Spark DataFrame.

        Args:
            path (str): Path to the CSV file
            spark (SparkSession): Active Spark session
            schema (StructType, optional): Schema for the DataFrame. Defaults to None.

        Returns:
            DataFrame: Spark DataFrame containing the CSV data
        """
        if schema:
            return spark.read.csv(path, header=True, schema=schema, inferSchema=False)
        else:
            return spark.read.csv(path, header=True, inferSchema=True)

    def get_data_file_paths(bucket_name: str, raw_data_prefix: str) -> dict:
        """
        Get the paths to the data files in S3.

        Args:
            bucket_name (str): Name of the S3 bucket
            raw_data_prefix (str): Prefix for raw data files in S3

        Returns:
            dict: Dictionary containing paths for different data files
        """
        return {
            "rental_transactions": f"s3://{bucket_name}/{raw_data_prefix}rental_transactions/",
            "vehicles": f"s3://{bucket_name}/{raw_data_prefix}vehicles/",
            "locations": f"s3://{bucket_name}/{raw_data_prefix}locations/",
            "users": f"s3://{bucket_name}/{raw_data_prefix}users/",
        }

    def get_output_paths(bucket_name: str, processed_data_prefix: str) -> dict:
        """
        Get the paths to the output directories in S3.

        Args:
            bucket_name (str): Name of the S3 bucket
            processed_data_prefix (str): Prefix for processed data in S3

        Returns:
            dict: Dictionary containing paths for different metric outputs
        """
        vehicle_location_base = (
            f"s3://{bucket_name}/{processed_data_prefix}vehicle_location_metrics/"
        )
        return {
            "location_metrics": f"{vehicle_location_base}location_metrics/",
            "vehicle_type_metrics": f"{vehicle_location_base}vehicle_type_metrics/",
            "brand_metrics": f"{vehicle_location_base}brand_metrics/",
        }


def create_spark_session() -> SparkSession:
    """
    Create and return a configured Spark session.

    Returns:
        SparkSession: Configured Spark session with snappy compression
    """
    return (
        SparkSession.builder.appName("Vehicle and Location Performance Metrics")
        .config("spark.sql.parquet.compression", "snappy")
        .getOrCreate()
    )


def load_data(
    spark: SparkSession, s3_bucket: str, raw_data_prefix: str
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Load the raw data from S3 into Spark DataFrames.

    Args:
        spark (SparkSession): Active Spark session
        s3_bucket (str): Name of the S3 bucket
        raw_data_prefix (str): Prefix for raw data in S3

    Returns:
        Tuple[DataFrame, DataFrame, DataFrame]: Tuple containing transactions, vehicles, and locations DataFrames
    """
    # Get the data file paths using the utility function
    data_paths = get_data_file_paths(s3_bucket, raw_data_prefix)

    # Load rental transactions data
    transactions_df = df(data_paths["rental_transactions"], spark)

    # Load vehicles data
    vehicles_df = df(data_paths["vehicles"], spark)

    # Load locations data
    locations_df = df(data_paths["locations"], spark)

    return transactions_df, vehicles_df, locations_df


def calculate_location_metrics(
    transactions_df: DataFrame, locations_df: DataFrame
) -> DataFrame:
    """
    Calculate comprehensive metrics by location.

    Args:
        transactions_df (DataFrame): DataFrame containing rental transactions
        locations_df (DataFrame): DataFrame containing location information

    Returns:
        DataFrame: Aggregated location metrics including revenue, transactions, and vehicle counts
    """
    # Calculate metrics by pickup location
    pickup_metrics = (
        transactions_df.groupBy("pickup_location")
        .agg(
            count("rental_id").alias("total_pickups"),
            sum("total_amount").alias("pickup_revenue"),
            round(avg("total_amount"), 2).alias(
                "avg_pickup_amount"
            ),  # Round to 2 decimal places for better readability
            max("total_amount").alias("max_pickup_amount"),
            min("total_amount").alias("min_pickup_amount"),
            countDistinct("vehicle_id").alias("unique_vehicles_picked_up"),
        )
        .withColumnRenamed("pickup_location", "location_id")
    )

    # Calculate metrics by dropoff location
    dropoff_metrics = (
        transactions_df.groupBy("dropoff_location")
        .agg(
            count("rental_id").alias("total_dropoffs"),
            sum("total_amount").alias("dropoff_revenue"),
            countDistinct("vehicle_id").alias("unique_vehicles_dropped_off"),
        )
        .withColumnRenamed("dropoff_location", "location_id")
    )

    # Join pickup and dropoff metrics with location information
    location_metrics = (
        pickup_metrics.join(dropoff_metrics, "location_id", "outer")
        .join(locations_df, "location_id", "inner")
        .select(
            "location_id",
            "location_name",
            "city",
            "state",
            col("total_pickups").alias("total_pickups"),
            col("total_dropoffs").alias("total_dropoffs"),
            (col("total_pickups") + col("total_dropoffs")).alias("total_transactions"),
            col("pickup_revenue").alias("pickup_revenue"),
            col("dropoff_revenue").alias("dropoff_revenue"),
            (col("pickup_revenue") + col("dropoff_revenue")).alias("total_revenue"),
            col("avg_pickup_amount").alias("avg_transaction_amount"),
            col("max_pickup_amount").alias("max_transaction_amount"),
            col("min_pickup_amount").alias("min_transaction_amount"),
            col("unique_vehicles_picked_up").alias("unique_vehicles_picked_up"),
            col("unique_vehicles_dropped_off").alias("unique_vehicles_dropped_off"),
            (
                col("unique_vehicles_picked_up") + col("unique_vehicles_dropped_off")
            ).alias("total_unique_vehicles"),
        )
    )

    return location_metrics


def calculate_vehicle_metrics(
    transactions_df: DataFrame, vehicles_df: DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """
    Calculate metrics by vehicle type and brand.

    Args:
        transactions_df (DataFrame): DataFrame containing rental transactions
        vehicles_df (DataFrame): DataFrame containing vehicle information

    Returns:
        Tuple[DataFrame, DataFrame]: Tuple containing vehicle type metrics and brand metrics DataFrames
    """
    # Calculate rental duration in hours from start and end timestamps
    transactions_with_duration = transactions_df.withColumn(
        "rental_duration_hours",
        round(
            (
                unix_timestamp(col("rental_end_time"))
                - unix_timestamp(col("rental_start_time"))
            )
            / 3600,
            2,
        ),
    )

    # Join transaction data with vehicle information
    transactions_with_vehicle_info = transactions_with_duration.join(
        vehicles_df.select("vehicle_id", "brand", "vehicle_type"), "vehicle_id", "inner"
    )

    # Calculate comprehensive metrics by vehicle type
    vehicle_type_metrics = transactions_with_vehicle_info.groupBy("vehicle_type").agg(
        count("rental_id").alias("total_rentals"),
        sum("total_amount").alias("total_revenue"),
        round(avg("total_amount"), 2).alias(
            "avg_rental_amount"
        ),  # Round to 2 decimal places for better readability
        max("total_amount").alias("max_rental_amount"),
        min("total_amount").alias("min_rental_amount"),
        round(avg("rental_duration_hours"), 2).alias(
            "avg_rental_duration_hours"
        ),  # Round to 2 decimal places for better readability
        max("rental_duration_hours").alias("max_rental_duration_hours"),
        min("rental_duration_hours").alias("min_rental_duration_hours"),
        countDistinct("vehicle_id").alias("unique_vehicles"),
    )

    # Calculate metrics by brand
    brand_metrics = transactions_with_vehicle_info.groupBy("brand").agg(
        count("rental_id").alias("total_rentals"),
        sum("total_amount").alias("total_revenue"),
        round(avg("total_amount"), 2).alias(
            "avg_rental_amount"
        ),  # Round to 2 decimal places for better readability
        countDistinct("vehicle_id").alias("unique_vehicles"),
    )

    return vehicle_type_metrics, brand_metrics


def main() -> None:
    """
    Main function to execute the Spark job for calculating vehicle and location metrics.

    Processes rental transaction data to generate metrics for locations and vehicles,
    then saves the results to S3 in Parquet format.
    """
    # Check if the required arguments are provided
    if len(sys.argv) != 3:
        print("Usage: job1_vehicle_location_metrics.py <s3_bucket> <environment>")
        sys.exit(1)

    s3_bucket = sys.argv[1]
    # environment = sys.argv[2]  # 'dev' or 'prod'

    # Set the data prefixes based on the environment
    raw_data_prefix = "raw/"
    processed_data_prefix = "processed/"

    # Create Spark session
    spark = create_spark_session()

    try:
        # Load data
        transactions_df, vehicles_df, locations_df = load_data(
            spark, s3_bucket, raw_data_prefix
        )

        # Calculate location metrics
        location_metrics = calculate_location_metrics(transactions_df, locations_df)

        # Calculate vehicle metrics
        vehicle_type_metrics, brand_metrics = calculate_vehicle_metrics(
            transactions_df, vehicles_df
        )

        # Get output paths using the utility function
        output_paths = get_output_paths(s3_bucket, processed_data_prefix)

        # Write the results to S3 in Parquet format
        location_metrics.write.mode("overwrite").parquet(
            output_paths["location_metrics"]
        )
        vehicle_type_metrics.write.mode("overwrite").parquet(
            output_paths["vehicle_type_metrics"]
        )
        brand_metrics.write.mode("overwrite").parquet(output_paths["brand_metrics"])

        print("Vehicle and location performance metrics job completed successfully")

    except Exception as e:
        print(f"Error in vehicle and location performance metrics job: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
