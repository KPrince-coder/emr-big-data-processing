# Utilities Package

This folder contains utility functions for the Big Data Processing with EMR project. These utilities provide helper functions for common tasks such as reading CSV files and constructing S3 paths.

## Available Modules

### read_csv_file.py

This module provides utility functions for reading CSV files from S3 or local filesystem using PySpark. It simplifies the process of loading data into Spark DataFrames with appropriate schema inference and header handling.

**Functions:**
- `df(file_path: str, spark: SparkSession) -> DataFrame`: Load a CSV file into a Spark DataFrame from S3 or local filesystem.

**Example:**
```python
from utils.read_csv_file import df
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
data_df = df("s3://my-bucket/data/users.csv", spark)
data_df.show(5)
```

### df_path.py

This module provides utility functions for constructing S3 paths for data files. It helps standardize the way S3 paths are constructed throughout the application, making it easier to maintain consistent path structures.

**Functions:**
- `df_path(file_name: str, s3_path: str) -> str`: Construct a complete S3 path by combining a base path with a file name.

**Example:**
```python
from utils.df_path import df_path

s3_base_path = "s3://my-bucket/data/"
users_path = df_path("users.csv", s3_base_path)
print(users_path)  # Output: s3://my-bucket/data/users.csv
```

### s3_path_utils.py

This module provides comprehensive utilities for handling S3 paths in the project. It includes functions for constructing S3 paths for different data types, validating S3 URIs, and managing S3 path structures.

**Key Functions:**
- `get_data_file_paths(bucket_name: str, raw_data_prefix: str = 'raw/') -> Dict[str, str]`: Get the S3 paths for all data files.
- `get_output_paths(bucket_name: str, processed_data_prefix: str = 'processed/') -> Dict[str, str]`: Get the S3 paths for output data.
- `get_raw_data_path(dataset_name: str, bucket_name: str, file_extension: str = '') -> str`: Get the S3 path for raw data.
- `get_processed_data_path(dataset_name: str, bucket_name: str) -> str`: Get the S3 path for processed data.
- `normalize_s3_path(path: str) -> str`: Normalize an S3 path to ensure it ends with a slash.
- `construct_s3_uri(bucket_name: str, key: str) -> str`: Construct an S3 URI from a bucket name and key.

**Example:**
```python
from utils.s3_path_utils import get_data_file_paths, get_output_paths
from utils.read_csv_file import df
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Get input data paths
bucket_name = "car-rental-data-lake"
data_paths = get_data_file_paths(bucket_name)

# Load data
users_df = df(data_paths["users"], spark)
vehicles_df = df(data_paths["vehicles"], spark)

# Get output paths
output_paths = get_output_paths(bucket_name)

# Write results
user_metrics.write.mode("overwrite").parquet(output_paths["user_metrics"])
```

## Usage in the Project

These utilities are used throughout the project to standardize the way data is loaded and S3 paths are constructed. By using these utilities, we ensure consistent behavior and reduce code duplication.

### Example Using df_path (Legacy Approach)

```python
from utils.read_csv_file import df
from utils.df_path import df_path
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Define base S3 path
s3_base_path = "s3://car-rental-data-lake/raw/"

# Construct file paths
users_path = df_path("users.csv", s3_base_path)
vehicles_path = df_path("vehicles.csv", s3_base_path)
locations_path = df_path("locations.csv", s3_base_path)
transactions_path = df_path("rental_transactions.csv", s3_base_path)

# Load data
users_df = df(users_path, spark)
vehicles_df = df(vehicles_path, spark)
locations_df = df(locations_path, spark)
transactions_df = df(transactions_path, spark)

# Process data...
```

### Example Using s3_path_utils (Recommended Approach)

```python
from utils.read_csv_file import df
from utils.s3_path_utils import get_data_file_paths, get_output_paths
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Get input data paths
bucket_name = "car-rental-data-lake"
data_paths = get_data_file_paths(bucket_name)

# Load data
users_df = df(data_paths["users"], spark)
vehicles_df = df(data_paths["vehicles"], spark)
locations_df = df(data_paths["locations"], spark)
transactions_df = df(data_paths["rental_transactions"], spark)

# Get output paths
output_paths = get_output_paths(bucket_name)

# Process data and write results
# ...

# Write results to S3
location_metrics.write.mode("overwrite").parquet(output_paths["location_metrics"])
user_metrics.write.mode("overwrite").parquet(output_paths["user_metrics"])
```
