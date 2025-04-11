#!/usr/bin/env python3
"""
Spark Job 2: User and Transaction Analysis

This PySpark job analyzes user engagement and transaction trends:
- Total transactions per day
- Revenue per day
- User-specific spending and rental duration metrics
- Maximum and minimum transaction amounts

Input: Raw data from S3
Output: Transformed data in Parquet format in S3
"""

import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    min,
    countDistinct,
    hour,
    when,
    round,
    date_format,
    to_date,
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from utils.read_csv_file import df
from utils.s3_path_utils import get_data_file_paths, get_output_paths


def create_spark_session() -> SparkSession:
    """
    Create and return a Spark session.

    Returns:
        SparkSession: The created Spark session
    """
    return (
        SparkSession.builder.appName("User and Transaction Analysis")
        .config("spark.sql.parquet.compression", "snappy")
        .getOrCreate()
    )


def load_data(spark: SparkSession, s3_bucket: str, raw_data_prefix: str) -> tuple:
    """
    Load the raw data from S3.

    Args:
        spark (SparkSession): The Spark session
        s3_bucket (str): The S3 bucket name
        raw_data_prefix (str): The prefix for raw data in S3

    Returns:
        tuple: (transactions_df, users_df)
    """
    # Get the data file paths using the utility function
    data_paths = get_data_file_paths(s3_bucket, raw_data_prefix)

    # Load rental transactions data
    transactions_df = df(data_paths["rental_transactions"], spark)

    # Load users data
    users_df = df(data_paths["users"], spark)

    return transactions_df, users_df


def analyze_daily_transactions(transactions_df: DataFrame) -> DataFrame:
    """
    Analyze transactions by day.

    Args:
        transactions_df (DataFrame): The rental transactions DataFrame

    Returns:
        DataFrame: Daily transaction metrics DataFrame
    """
    # Extract date from rental_start_time
    transactions_with_date = transactions_df.withColumn(
        "rental_date", to_date(col("rental_start_time"))
    )

    # Calculate metrics by day
    daily_metrics = (
        transactions_with_date.groupBy("rental_date")
        .agg(
            count("rental_id").alias("total_transactions"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_amount"),
            max("total_amount").alias("max_transaction_amount"),
            min("total_amount").alias("min_transaction_amount"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("vehicle_id").alias("unique_vehicles"),
        )
        .orderBy("rental_date")
    )

    return daily_metrics


def analyze_user_transactions(
    transactions_df: DataFrame, users_df: DataFrame
) -> DataFrame:
    """
    Analyze transactions by user.

    Args:
        transactions_df (DataFrame): The rental transactions DataFrame
        users_df (DataFrame): The users DataFrame

    Returns:
        DataFrame: User transaction metrics DataFrame
    """
    # Add rental duration in hours
    transactions_with_duration = transactions_df.withColumn(
        "rental_duration_hours",
        round(
            hour(
                col("rental_end_time").cast("timestamp")
                - col("rental_start_time").cast("timestamp")
            ),
            2,
        ),
    )

    # Calculate metrics by user
    user_metrics = transactions_with_duration.groupBy("user_id").agg(
        count("rental_id").alias("total_rentals"),
        sum("total_amount").alias("total_spent"),
        avg("total_amount").alias("avg_rental_amount"),
        max("total_amount").alias("max_rental_amount"),
        min("total_amount").alias("min_rental_amount"),
        avg("rental_duration_hours").alias("avg_rental_duration_hours"),
        sum("rental_duration_hours").alias("total_rental_hours"),
        countDistinct("vehicle_id").alias("unique_vehicles_rented"),
    )

    # Join with users data
    user_metrics_with_info = user_metrics.join(
        users_df.select("user_id", "first_name", "last_name", "email", "is_active"),
        "user_id",
        "inner",
    )

    # Calculate user spending percentile
    window_spec = Window.orderBy(col("total_spent"))
    user_metrics_with_percentile = user_metrics_with_info.withColumn(
        "spending_percentile", F.percent_rank().over(window_spec) * 100
    )

    # Categorize users by spending
    user_metrics_categorized = user_metrics_with_percentile.withColumn(
        "spending_category",
        when(col("spending_percentile") >= 90, "Top Spender")
        .when(col("spending_percentile") >= 75, "High Spender")
        .when(col("spending_percentile") >= 50, "Medium Spender")
        .when(col("spending_percentile") >= 25, "Low Spender")
        .otherwise("Occasional Spender"),
    )

    return user_metrics_categorized


def analyze_transaction_patterns(transactions_df: DataFrame) -> tuple:
    """
    Analyze transaction patterns.

    Args:
        transactions_df (DataFrame): The rental transactions DataFrame

    Returns:
        DataFrame: Transaction pattern metrics DataFrame
    """
    # Extract hour of day from rental_start_time
    transactions_with_hour = transactions_df.withColumn(
        "hour_of_day", date_format(col("rental_start_time"), "HH").cast("int")
    )

    # Calculate metrics by hour of day
    hourly_metrics = (
        transactions_with_hour.groupBy("hour_of_day")
        .agg(
            count("rental_id").alias("total_transactions"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_amount"),
        )
        .orderBy("hour_of_day")
    )

    # Extract day of week from rental_start_time (1 = Sunday, 7 = Saturday)
    transactions_with_day = transactions_df.withColumn(
        "day_of_week", date_format(col("rental_start_time"), "u").cast("int")
    )

    # Calculate metrics by day of week
    day_of_week_metrics = (
        transactions_with_day.groupBy("day_of_week")
        .agg(
            count("rental_id").alias("total_transactions"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction_amount"),
        )
        .orderBy("day_of_week")
    )

    return hourly_metrics, day_of_week_metrics


def main():
    """Main function to execute the Spark job."""
    # Check if the required arguments are provided
    if len(sys.argv) != 3:
        print("Usage: job2_user_transaction_analysis.py <s3_bucket> <environment>")
        sys.exit(1)

    s3_bucket = sys.argv[1]
    environment = sys.argv[2]  # 'dev' or 'prod'

    # Set the data prefixes based on the environment
    raw_data_prefix = "raw/"
    processed_data_prefix = "processed/"

    # Create Spark session
    spark = create_spark_session()

    try:
        # Load data
        transactions_df, users_df = load_data(spark, s3_bucket, raw_data_prefix)

        # Analyze daily transactions
        daily_metrics = analyze_daily_transactions(transactions_df)

        # Analyze user transactions
        user_metrics = analyze_user_transactions(transactions_df, users_df)

        # Analyze transaction patterns
        hourly_metrics, day_of_week_metrics = analyze_transaction_patterns(
            transactions_df
        )

        # Get output paths using the utility function
        output_paths = get_output_paths(s3_bucket, processed_data_prefix)

        # Write the results to S3 in Parquet format
        daily_metrics.write.mode("overwrite").parquet(output_paths["daily_metrics"])
        user_metrics.write.mode("overwrite").parquet(output_paths["user_metrics"])
        hourly_metrics.write.mode("overwrite").parquet(output_paths["hourly_metrics"])
        day_of_week_metrics.write.mode("overwrite").parquet(
            output_paths["day_of_week_metrics"]
        )

        print("User and transaction analysis job completed successfully")

    except Exception as e:
        print(f"Error in user and transaction analysis job: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
