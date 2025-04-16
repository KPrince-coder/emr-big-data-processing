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
import traceback
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
    when,
    round,
    date_format,
    to_date,
    unix_timestamp,
    dayofweek,
    first,  # Import first function to get the first value in a group
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Set up logging
try:
    from utils.logging_config import configure_logger

    logger = configure_logger(__name__)
except ImportError:
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

# Try to import utility functions for S3 path handling
try:
    from utils.read_csv_file import df
    from utils.s3_path_utils import get_data_file_paths, get_output_paths
except ImportError:
    # Define fallback functions if utils module is not available
    def df(path: str, spark: SparkSession, schema=None) -> DataFrame:
        """Read a CSV file into a Spark DataFrame."""
        if schema:
            return spark.read.csv(path, header=True, schema=schema, inferSchema=False)
        else:
            return spark.read.csv(path, header=True, inferSchema=True)

    def get_data_file_paths(bucket_name: str, raw_data_prefix: str) -> dict:
        """Get the paths to the data files in S3."""
        return {
            "rental_transactions": f"s3://{bucket_name}/{raw_data_prefix}rental_transactions/rental_transactions.csv",
            "vehicles": f"s3://{bucket_name}/{raw_data_prefix}vehicles/vehicles.csv",
            "locations": f"s3://{bucket_name}/{raw_data_prefix}locations/locations.csv",
            "users": f"s3://{bucket_name}/{raw_data_prefix}users/users.csv",
        }

    def get_output_paths(
        s3_config: dict = None,
        bucket_name: str = None,
        processed_data_prefix: str = None,
    ) -> dict:
        """
        Get the paths to the output directories in S3.

        Args:
            s3_config (dict, optional): S3 configuration dictionary. Defaults to None.
            bucket_name (str, optional): Name of the S3 bucket. Defaults to None.
            processed_data_prefix (str, optional): Prefix for processed data files. Defaults to None.

        Returns:
            dict: Dictionary containing paths for different output metrics
        """
        # Handle the case when called with bucket_name and processed_data_prefix directly
        if (
            bucket_name is not None
            and processed_data_prefix is not None
            and s3_config is None
        ):
            # This is the case when called from the main function
            vehicle_location_base = (
                f"s3://{bucket_name}/{processed_data_prefix}vehicle_location_metrics/"
            )
            user_transaction_base = (
                f"s3://{bucket_name}/{processed_data_prefix}user_transaction_analysis/"
            )
        else:
            # This is the case when called with s3_config (matching the utils.s3_path_utils signature)
            if bucket_name is None and s3_config is not None:
                bucket_name = s3_config.get("bucket_name")

            if processed_data_prefix is None and s3_config is not None:
                processed_data_prefix = s3_config.get(
                    "processed_data_prefix", "processed/"
                )
            elif processed_data_prefix is None:
                processed_data_prefix = "processed/"

            # Ensure the processed_data_prefix ends with a slash
            if not processed_data_prefix.endswith("/"):
                processed_data_prefix += "/"

            # Define the base paths
            vehicle_location_base = (
                f"s3://{bucket_name}/{processed_data_prefix}vehicle_location_metrics/"
            )
            user_transaction_base = (
                f"s3://{bucket_name}/{processed_data_prefix}user_transaction_analysis/"
            )

        # Return the output paths dictionary
        return {
            # Job 1 outputs (included for completeness)
            "location_metrics": f"{vehicle_location_base}location_metrics/",
            "vehicle_type_metrics": f"{vehicle_location_base}vehicle_type_metrics/",
            "brand_metrics": f"{vehicle_location_base}brand_metrics/",
            # Job 2 outputs
            "daily_metrics": f"{user_transaction_base}daily_metrics/",
            "user_metrics": f"{user_transaction_base}user_metrics/",
            "hourly_metrics": f"{user_transaction_base}hourly_metrics/",
            "day_of_week_metrics": f"{user_transaction_base}day_of_week_metrics/",
        }


def create_spark_session() -> SparkSession:
    """
    Create and return a configured Spark session.

    Returns:
        SparkSession: Configured Spark session with snappy compression
    """
    return (
        SparkSession.builder.appName("User and Transaction Analysis")
        .config("spark.sql.parquet.compression", "snappy")
        .getOrCreate()
    )


def load_data(
    spark: SparkSession, s3_bucket: str, raw_data_prefix: str
) -> Tuple[DataFrame, DataFrame]:
    """
    Load the raw data from S3.

    Args:
        spark (SparkSession): The Spark session
        s3_bucket (str): The S3 bucket name
        raw_data_prefix (str): The prefix for raw data in S3

    Returns:
        Tuple[DataFrame, DataFrame]: (transactions_df, users_df)
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
            round(avg("total_amount"), 2).alias(
                "avg_transaction_amount"
            ),  # Round to 2 decimal places
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
            (
                unix_timestamp(col("rental_end_time"))
                - unix_timestamp(col("rental_start_time"))
            )
            / 3600,
            2,
        ),
    )

    # Calculate metrics by user
    user_metrics = transactions_with_duration.groupBy("user_id").agg(
        count("rental_id").alias("total_rentals"),
        sum("total_amount").alias("total_spent"),
        round(avg("total_amount"), 2).alias(
            "avg_rental_amount"
        ),  # Round to 2 decimal places
        max("total_amount").alias("max_rental_amount"),
        min("total_amount").alias("min_rental_amount"),
        round(avg("rental_duration_hours"), 2).alias(
            "avg_rental_duration_hours"
        ),  # Round to 2 decimal places
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
        "spending_percentile", round(F.percent_rank().over(window_spec) * 100, 2)
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


def analyze_transaction_patterns(
    transactions_df: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
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
            round(avg("total_amount"), 2).alias(
                "avg_transaction_amount"
            ),  # Round to 2 decimal places
        )
        .orderBy("hour_of_day")
    )

    # Extract day of week  from rental_start_time

    # Add day_of_week and adjust day_order for Monday (1) to Sunday (7)
    transactions_with_day = transactions_df.withColumn(
        "day_of_week",
        date_format(
            col("rental_start_time"), "EEEE"
        ),  # Extract day of the week (e.g., Monday)
    ).withColumn(
        "day_order",
        (dayofweek(col("rental_start_time")) + 5) % 7
        + 1,  # Add day order (Monday=1, Tuesday=2, ..., Sunday=7)
    )

    # Calculate metrics by day of week and sort by day_order
    day_of_week_metrics = (
        transactions_with_day.groupBy("day_of_week")  # Group data by day of the week
        .agg(
            count("rental_id").alias(
                "total_transactions"
            ),  # Calculate total transactions per day
            sum("total_amount").alias(
                "total_revenue"
            ),  # Calculate total revenue per day
            round(avg("total_amount"), 2).alias(
                "avg_transaction_amount"
            ),  # Calculate average transaction amount per day
            first("day_order").alias("day_order"),  # Include day order for sorting
        )
        .orderBy("day_order")  # Sort by day order (Monday to Sunday)
        .select(
            "day_of_week",
            "total_transactions",
            "total_revenue",
            "avg_transaction_amount",
        )  # Select desired columns for output
    )

    return hourly_metrics, day_of_week_metrics


def main() -> None:
    """Main function to execute the Spark job."""
    # Check if the required arguments are provided
    if len(sys.argv) != 3:
        logger.error(
            "Usage: job2_user_transaction_analysis.py <s3_bucket> <environment>"
        )
        sys.exit(1)

    s3_bucket = sys.argv[1]
    environment = sys.argv[2]  # 'dev' or 'prod'
    logger.info(
        f"Starting User Transaction Analysis job with bucket: {s3_bucket}, environment: {environment}"
    )

    # Set the data prefixes based on the environment
    if environment.lower() == "dev":
        raw_data_prefix = "dev/raw/"
        processed_data_prefix = "dev/processed/"
    else:  # 'prod' or any other value defaults to production
        raw_data_prefix = "raw/"
        processed_data_prefix = "processed/"

    logger.info(
        f"Using raw_data_prefix: {raw_data_prefix}, processed_data_prefix: {processed_data_prefix}"
    )

    # Create Spark session
    logger.info("Creating Spark session")
    spark = create_spark_session()
    logger.info(f"Spark session created: {spark.sparkContext.appName}")

    try:
        # Load data
        logger.info("Loading data from S3")
        transactions_df, users_df = load_data(spark, s3_bucket, raw_data_prefix)
        logger.info(
            f"Data loaded successfully. Transactions count: {transactions_df.count()}, Users count: {users_df.count()}"
        )

        # Analyze daily transactions
        logger.info("Analyzing daily transactions")
        daily_metrics = analyze_daily_transactions(transactions_df)
        logger.info(
            f"Daily metrics analysis complete. Row count: {daily_metrics.count()}"
        )

        # Analyze user transactions
        logger.info("Analyzing user transactions")
        user_metrics = analyze_user_transactions(transactions_df, users_df)
        logger.info(
            f"User metrics analysis complete. Row count: {user_metrics.count()}"
        )

        # Analyze transaction patterns
        logger.info("Analyzing transaction patterns")
        hourly_metrics, day_of_week_metrics = analyze_transaction_patterns(
            transactions_df
        )
        logger.info(
            f"Transaction patterns analysis complete. Hourly metrics count: {hourly_metrics.count()}, Day of week metrics count: {day_of_week_metrics.count()}"
        )

        # Get output paths using the utility function
        logger.info("Getting output paths")
        output_paths = get_output_paths(
            bucket_name=s3_bucket, processed_data_prefix=processed_data_prefix
        )
        logger.info(f"Output paths: {output_paths}")

        # Write the results to S3 in Parquet format
        logger.info(f"Writing daily metrics to {output_paths['daily_metrics']}")
        daily_metrics.write.mode("overwrite").parquet(output_paths["daily_metrics"])
        logger.info("Daily metrics written successfully")

        logger.info(f"Writing user metrics to {output_paths['user_metrics']}")
        user_metrics.write.mode("overwrite").parquet(output_paths["user_metrics"])
        logger.info("User metrics written successfully")

        logger.info(f"Writing hourly metrics to {output_paths['hourly_metrics']}")
        hourly_metrics.write.mode("overwrite").parquet(output_paths["hourly_metrics"])
        logger.info("Hourly metrics written successfully")

        logger.info(
            f"Writing day of week metrics to {output_paths['day_of_week_metrics']}"
        )
        day_of_week_metrics.write.mode("overwrite").parquet(
            output_paths["day_of_week_metrics"]
        )
        logger.info("Day of week metrics written successfully")

        logger.info("User and transaction analysis job completed successfully")

    except Exception as e:
        logger.error(f"Error in user and transaction analysis job: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        logger.info("Stopping Spark session")
        spark.stop()


if __name__ == "__main__":
    main()
