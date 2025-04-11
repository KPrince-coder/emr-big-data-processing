# Data Processing Guide

This document provides detailed information about the data processing components of the Big Data Processing with AWS EMR project.

## Table of Contents

- [Data Processing Guide](#data-processing-guide)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Datasets](#datasets)
    - [Vehicles Dataset](#vehicles-dataset)
    - [Users Dataset](#users-dataset)
    - [Locations Dataset](#locations-dataset)
    - [Rental Transactions Dataset](#rental-transactions-dataset)
  - [Spark Jobs](#spark-jobs)
    - [Job 1: Vehicle and Location Performance Metrics](#job-1-vehicle-and-location-performance-metrics)
    - [Job 2: User and Transaction Analysis](#job-2-user-and-transaction-analysis)
  - [Data Transformation](#data-transformation)
  - [Output Formats](#output-formats)
  - [Performance Considerations](#performance-considerations)
  - [Testing and Validation](#testing-and-validation)

## Overview

The data processing component of the project uses Apache Spark on Amazon EMR to process large datasets from a car rental marketplace. The processing is divided into two main Spark jobs:

1. **Vehicle and Location Performance Metrics**: Calculates metrics related to vehicles and rental locations
2. **User and Transaction Analysis**: Analyzes user behavior and transaction patterns

The jobs read data from Amazon S3, process it using Spark transformations, and write the results back to S3 in Parquet format.

## Datasets

The project uses four datasets. For detailed information about these datasets, including sample data and complete schema, refer to the [Dataset Reference](../data/dataset_reference.md) file.

### Vehicles Dataset

Contains information about rental vehicles.

**Schema:**

```markdown
root
 |-- active: integer (nullable = true)
 |-- vehicle_license_number: string (nullable = true)
 |-- registration_name: string (nullable = true)
 |-- license_type: string (nullable = true)
 |-- expiration_date: string (nullable = true)
 |-- permit_license_number: string (nullable = true)
 |-- certification_date: date (nullable = true)
 |-- vehicle_year: integer (nullable = true)
 |-- base_telephone_number: string (nullable = true)
 |-- base_address: string (nullable = true)
 |-- vehicle_id: string (nullable = true)
 |-- last_update_timestamp: string (nullable = true)
 |-- brand: string (nullable = true)
 |-- vehicle_type: string (nullable = true)
```

**Key Fields:**

- `vehicle_id`: Unique identifier for the vehicle
- `brand`: Vehicle brand (e.g., Toyota, BMW)
- `vehicle_type`: Type of vehicle (e.g., basic, premium, high_end)

### Users Dataset

Contains user sign-up information.

**Schema:**

```markdown
root
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- phone_number: string (nullable = true)
 |-- driver_license_number: string (nullable = true)
 |-- driver_license_expiry: date (nullable = true)
 |-- creation_date: date (nullable = true)
 |-- is_active: integer (nullable = true)
```

**Key Fields:**

- `user_id`: Unique identifier for the user
- `creation_date`: Date when the user signed up
- `is_active`: Whether the user is active (1) or inactive (0)

### Locations Dataset

Contains master data for rental locations.

**Schema:**

```markdown
root
 |-- location_id: integer (nullable = true)
 |-- location_name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- zip_code: integer (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
```

**Key Fields:**

- `location_id`: Unique identifier for the location
- `city`: City where the location is situated
- `state`: State where the location is situated

### Rental Transactions Dataset

Contains records of vehicle rentals.

**Schema:**

```markdown
root
 |-- rental_id: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- vehicle_id: string (nullable = true)
 |-- rental_start_time: timestamp (nullable = true)
 |-- rental_end_time: timestamp (nullable = true)
 |-- pickup_location: integer (nullable = true)
 |-- dropoff_location: integer (nullable = true)
 |-- total_amount: double (nullable = true)
```

**Key Fields:**

- `rental_id`: Unique identifier for the rental transaction
- `user_id`: Reference to the user who rented the vehicle
- `vehicle_id`: Reference to the rented vehicle
- `rental_start_time`: When the rental started
- `rental_end_time`: When the rental ended
- `pickup_location`: Reference to the location where the vehicle was picked up
- `dropoff_location`: Reference to the location where the vehicle was dropped off
- `total_amount`: Total amount paid for the rental

## Spark Jobs

### Job 1: Vehicle and Location Performance Metrics

This job calculates metrics related to vehicles and rental locations.

**Input:**

- Vehicles Dataset
- Locations Dataset
- Rental Transactions Dataset

**Output:**

- Location Metrics
- Vehicle Type Metrics
- Brand Metrics

**Key Metrics:**

- Revenue per location
- Total transactions per location
- Average, max, and min transaction amounts
- Unique vehicles used at each location
- Rental duration and revenue by vehicle type

**Implementation:**
The job is implemented in `spark/job1_vehicle_location_metrics.py` and consists of the following steps:

1. **Load Data**: Load the datasets from S3
2. **Calculate Location Metrics**:
   - Group transactions by pickup and dropoff locations
   - Calculate metrics for each location
   - Join with the locations dataset to add location details
3. **Calculate Vehicle Metrics**:
   - Add rental duration to transactions
   - Join with the vehicles dataset to add vehicle details
   - Calculate metrics by vehicle type and brand
4. **Write Results**: Write the results to S3 in Parquet format

**Code Example:**

```python
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
```

### Job 2: User and Transaction Analysis

This job analyzes user behavior and transaction patterns.

**Input:**

- Users Dataset
- Rental Transactions Dataset

**Output:**

- Daily Transaction Metrics
- User Transaction Metrics
- Hourly Transaction Metrics
- Day of Week Transaction Metrics

**Key Metrics:**

- Total transactions per day
- Revenue per day
- User-specific spending and rental duration metrics
- Transaction patterns by hour of day
- Transaction patterns by day of week

**Implementation:**
The job is implemented in `spark/job2_user_transaction_analysis.py` and consists of the following steps:

1. **Load Data**: Load the datasets from S3
2. **Analyze Daily Transactions**:
   - Extract date from rental start time
   - Group transactions by date
   - Calculate metrics for each day
3. **Analyze User Transactions**:
   - Add rental duration to transactions
   - Group transactions by user
   - Calculate metrics for each user
   - Join with the users dataset to add user details
   - Calculate spending percentiles and categories
4. **Analyze Transaction Patterns**:
   - Extract hour of day and day of week from rental start time
   - Calculate metrics by hour and day
5. **Write Results**: Write the results to S3 in Parquet format

**Code Example:**

```python
def analyze_user_transactions(transactions_df, users_df):
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
        round(hour(col("rental_end_time").cast("timestamp") -
                  col("rental_start_time").cast("timestamp")), 2)
    )

    # Calculate metrics by user
    user_metrics = (transactions_with_duration
        .groupBy("user_id")
        .agg(
            count("rental_id").alias("total_rentals"),
            sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_rental_amount"),
            max("total_amount").alias("max_rental_amount"),
            min("total_amount").alias("min_rental_amount"),
            avg("rental_duration_hours").alias("avg_rental_duration_hours"),
            sum("rental_duration_hours").alias("total_rental_hours"),
            countDistinct("vehicle_id").alias("unique_vehicles_rented")
        ))

    # Join with users data
    user_metrics_with_info = user_metrics.join(
        users_df.select("user_id", "first_name", "last_name", "email", "is_active"),
        "user_id",
        "inner"
    )

    # Calculate user spending percentile
    window_spec = Window.orderBy(col("total_spent"))
    user_metrics_with_percentile = user_metrics_with_info.withColumn(
        "spending_percentile",
        F.percent_rank().over(window_spec) * 100
    )

    # Categorize users by spending
    user_metrics_categorized = user_metrics_with_percentile.withColumn(
        "spending_category",
        when(col("spending_percentile") >= 90, "Top Spender")
        .when(col("spending_percentile") >= 75, "High Spender")
        .when(col("spending_percentile") >= 50, "Medium Spender")
        .when(col("spending_percentile") >= 25, "Low Spender")
        .otherwise("Occasional Spender")
    )

    return user_metrics_categorized
```

## Data Transformation

The data processing involves several transformations:

1. **Filtering**: Remove invalid or irrelevant data
2. **Joining**: Combine data from different datasets
3. **Aggregation**: Calculate summary statistics
4. **Derivation**: Create new fields based on existing ones
5. **Categorization**: Assign categories based on values

Key transformations include:

- **Rental Duration**: Calculated as the difference between rental end time and start time
- **Total Transactions**: Sum of pickups and dropoffs at a location
- **Total Revenue**: Sum of revenue from all transactions
- **Spending Percentile**: Rank of users based on total spending
- **Spending Category**: Category assigned based on spending percentile

## Output Formats

The processed data is written to S3 in Parquet format, which offers several advantages:

- **Columnar Storage**: Efficient for analytical queries
- **Compression**: Reduces storage costs
- **Schema Preservation**: Maintains data types
- **Partitioning**: Supports efficient querying

The output is organized into the following structure:

```markdown
s3://bucket-name/processed/
├── vehicle_location_metrics/
│   ├── location_metrics/
│   ├── vehicle_type_metrics/
│   └── brand_metrics/
└── user_transaction_analysis/
    ├── daily_metrics/
    ├── user_metrics/
    ├── hourly_metrics/
    └── day_of_week_metrics/
```

## Performance Considerations

The Spark jobs are optimized for performance:

1. **Partitioning**: Data is partitioned appropriately to distribute the workload
2. **Caching**: Frequently used DataFrames are cached to avoid recomputation
3. **Broadcast Joins**: Small DataFrames are broadcast to avoid shuffling
4. **Column Pruning**: Only necessary columns are selected
5. **Predicate Pushdown**: Filters are pushed down to the data source

EMR cluster configuration:

- **Instance Types**: m5.xlarge for master and core nodes
- **Core Instance Count**: 2 (can be adjusted based on data size)
- **Spark Configuration**:
  - `spark.dynamicAllocation.enabled`: true
  - `spark.executor.instances`: 2
  - `spark.executor.memory`: 4g
  - `spark.driver.memory`: 4g

## Testing and Validation

The data processing is tested and validated using several approaches:

1. **Local Testing**:
   - Use the Jupyter notebooks for local testing
   - Use a small subset of the data for testing

2. **Data Quality Checks**:
   - Check for missing values
   - Validate calculated metrics
   - Ensure referential integrity

3. **Performance Testing**:
   - Test with different data sizes
   - Monitor execution time and resource usage

4. **Integration Testing**:
   - Test the complete pipeline on AWS
   - Validate the output data in Athena
