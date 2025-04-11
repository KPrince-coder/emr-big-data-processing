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
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, max, min, countDistinct, 
    datediff, hour, lit, when, round
)

def create_spark_session():
    """
    Create and return a Spark session.
    
    Returns:
        SparkSession: The created Spark session
    """
    return (SparkSession.builder
            .appName("Vehicle and Location Performance Metrics")
            .config("spark.sql.parquet.compression", "snappy")
            .getOrCreate())

def load_data(spark, s3_bucket, raw_data_prefix):
    """
    Load the raw data from S3.
    
    Args:
        spark (SparkSession): The Spark session
        s3_bucket (str): The S3 bucket name
        raw_data_prefix (str): The prefix for raw data in S3
        
    Returns:
        tuple: (transactions_df, vehicles_df, locations_df)
    """
    # Load rental transactions data
    transactions_df = (spark.read.option("header", "true")
                      .option("inferSchema", "true")
                      .csv(f"s3://{s3_bucket}/{raw_data_prefix}rental_transactions/"))
    
    # Load vehicles data
    vehicles_df = (spark.read.option("header", "true")
                  .option("inferSchema", "true")
                  .csv(f"s3://{s3_bucket}/{raw_data_prefix}vehicles/"))
    
    # Load locations data
    locations_df = (spark.read.option("header", "true")
                   .option("inferSchema", "true")
                   .csv(f"s3://{s3_bucket}/{raw_data_prefix}locations/"))
    
    return transactions_df, vehicles_df, locations_df

def calculate_location_metrics(transactions_df, locations_df):
    """
    Calculate metrics by location.
    
    Args:
        transactions_df (DataFrame): The rental transactions DataFrame
        locations_df (DataFrame): The locations DataFrame
        
    Returns:
        DataFrame: Location metrics DataFrame
    """
    # Calculate metrics by pickup location
    pickup_metrics = (transactions_df
        .groupBy("pickup_location")
        .agg(
            count("rental_id").alias("total_pickups"),
            sum("total_amount").alias("pickup_revenue"),
            avg("total_amount").alias("avg_pickup_amount"),
            max("total_amount").alias("max_pickup_amount"),
            min("total_amount").alias("min_pickup_amount"),
            countDistinct("vehicle_id").alias("unique_vehicles_picked_up")
        )
        .withColumnRenamed("pickup_location", "location_id"))
    
    # Calculate metrics by dropoff location
    dropoff_metrics = (transactions_df
        .groupBy("dropoff_location")
        .agg(
            count("rental_id").alias("total_dropoffs"),
            sum("total_amount").alias("dropoff_revenue"),
            countDistinct("vehicle_id").alias("unique_vehicles_dropped_off")
        )
        .withColumnRenamed("dropoff_location", "location_id"))
    
    # Join pickup and dropoff metrics
    location_metrics = (pickup_metrics
        .join(dropoff_metrics, "location_id", "outer")
        .join(locations_df, "location_id", "inner")
        .select(
            "location_id", "location_name", "city", "state",
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
            (col("unique_vehicles_picked_up") + col("unique_vehicles_dropped_off")).alias("total_unique_vehicles")
        ))
    
    return location_metrics

def calculate_vehicle_metrics(transactions_df, vehicles_df):
    """
    Calculate metrics by vehicle type.
    
    Args:
        transactions_df (DataFrame): The rental transactions DataFrame
        vehicles_df (DataFrame): The vehicles DataFrame
        
    Returns:
        DataFrame: Vehicle metrics DataFrame
    """
    # Add rental duration in hours
    transactions_with_duration = transactions_df.withColumn(
        "rental_duration_hours",
        round(hour(col("rental_end_time").cast("timestamp") - 
                  col("rental_start_time").cast("timestamp")), 2)
    )
    
    # Join with vehicles data
    transactions_with_vehicle_info = transactions_with_duration.join(
        vehicles_df.select("vehicle_id", "brand", "vehicle_type"),
        "vehicle_id",
        "inner"
    )
    
    # Calculate metrics by vehicle type
    vehicle_type_metrics = (transactions_with_vehicle_info
        .groupBy("vehicle_type")
        .agg(
            count("rental_id").alias("total_rentals"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_rental_amount"),
            max("total_amount").alias("max_rental_amount"),
            min("total_amount").alias("min_rental_amount"),
            avg("rental_duration_hours").alias("avg_rental_duration_hours"),
            max("rental_duration_hours").alias("max_rental_duration_hours"),
            min("rental_duration_hours").alias("min_rental_duration_hours"),
            countDistinct("vehicle_id").alias("unique_vehicles")
        ))
    
    # Calculate metrics by brand
    brand_metrics = (transactions_with_vehicle_info
        .groupBy("brand")
        .agg(
            count("rental_id").alias("total_rentals"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_rental_amount"),
            countDistinct("vehicle_id").alias("unique_vehicles")
        ))
    
    return vehicle_type_metrics, brand_metrics

def main():
    """Main function to execute the Spark job."""
    # Check if the required arguments are provided
    if len(sys.argv) != 3:
        print("Usage: job1_vehicle_location_metrics.py <s3_bucket> <environment>")
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
        transactions_df, vehicles_df, locations_df = load_data(spark, s3_bucket, raw_data_prefix)
        
        # Calculate location metrics
        location_metrics = calculate_location_metrics(transactions_df, locations_df)
        
        # Calculate vehicle metrics
        vehicle_type_metrics, brand_metrics = calculate_vehicle_metrics(transactions_df, vehicles_df)
        
        # Write the results to S3 in Parquet format
        location_metrics.write.mode("overwrite").parquet(
            f"s3://{s3_bucket}/{processed_data_prefix}vehicle_location_metrics/location_metrics/"
        )
        
        vehicle_type_metrics.write.mode("overwrite").parquet(
            f"s3://{s3_bucket}/{processed_data_prefix}vehicle_location_metrics/vehicle_type_metrics/"
        )
        
        brand_metrics.write.mode("overwrite").parquet(
            f"s3://{s3_bucket}/{processed_data_prefix}vehicle_location_metrics/brand_metrics/"
        )
        
        print("Vehicle and location performance metrics job completed successfully")
        
    except Exception as e:
        print(f"Error in vehicle and location performance metrics job: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
