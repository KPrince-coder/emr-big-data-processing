# Analysis and Visualization Guide

This document provides detailed information about the analysis and visualization components of the Big Data Processing with AWS EMR project.

## Table of Contents

1. [Overview](#overview)
2. [Data Exploration](#data-exploration)
3. [Athena Queries](#athena-queries)
4. [Visualization Techniques](#visualization-techniques)
5. [Business Insights](#business-insights)
6. [Advanced Analytics](#advanced-analytics)
7. [Extending the Analysis](#extending-the-analysis)

## Overview

The analysis and visualization component of the project focuses on extracting business insights from the processed data. It uses Amazon Athena for SQL-based analysis and Python libraries like Pandas, Matplotlib, and Seaborn for visualization.

The analysis is implemented in Jupyter notebooks:

- `notebooks/data_exploration.ipynb`: Explores the raw datasets
- `notebooks/athena_queries.ipynb`: Demonstrates how to query the processed data using Athena

## Data Exploration

The data exploration notebook (`notebooks/data_exploration.ipynb`) provides an initial analysis of the raw datasets to understand their structure and content.

### Loading the Data

```python
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Set plot style
plt.style.use('ggplot')
sns.set(style="whitegrid")

# Configure pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 20)
pd.set_option('display.width', 1000)

# Define the data directory
data_dir = '../data'

# Load the datasets
vehicles_df = pd.read_csv(os.path.join(data_dir, 'vehicles.csv'))
users_df = pd.read_csv(os.path.join(data_dir, 'users.csv'))
locations_df = pd.read_csv(os.path.join(data_dir, 'locations.csv'))
transactions_df = pd.read_csv(os.path.join(data_dir, 'rental_transactions.csv'))
```

### Exploring the Vehicles Dataset

```python
# Display the first few rows of the vehicles dataset
vehicles_df.head()

# Check for missing values
vehicles_df.isnull().sum()

# Analyze vehicle types
vehicle_type_counts = vehicles_df['vehicle_type'].value_counts()
print(vehicle_type_counts)

# Plot vehicle types
plt.figure(figsize=(10, 6))
vehicle_type_counts.plot(kind='bar')
plt.title('Vehicle Types Distribution')
plt.xlabel('Vehicle Type')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

### Exploring the Users Dataset

```python
# Display the first few rows of the users dataset
users_df.head()

# Check for missing values
users_df.isnull().sum()

# Analyze active users
active_users_count = users_df['is_active'].value_counts()
print(active_users_count)

# Plot active users
plt.figure(figsize=(8, 6))
active_users_count.plot(kind='pie', autopct='%1.1f%%')
plt.title('Active vs. Inactive Users')
plt.ylabel('')
plt.tight_layout()
plt.show()
```

### Exploring the Locations Dataset

```python
# Display the first few rows of the locations dataset
locations_df.head()

# Check for missing values
locations_df.isnull().sum()

# Analyze locations by state
locations_by_state = locations_df['state'].value_counts()
print(locations_by_state)

# Plot locations by state
plt.figure(figsize=(12, 6))
locations_by_state.plot(kind='bar')
plt.title('Locations by State')
plt.xlabel('State')
plt.ylabel('Number of Locations')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

### Exploring the Rental Transactions Dataset

```python
# Display the first few rows of the rental transactions dataset
transactions_df.head()

# Check for missing values
transactions_df.isnull().sum()

# Convert rental start and end times to datetime
transactions_df['rental_start_time'] = pd.to_datetime(transactions_df['rental_start_time'])
transactions_df['rental_end_time'] = pd.to_datetime(transactions_df['rental_end_time'])

# Calculate rental duration in hours
transactions_df['rental_duration_hours'] = (transactions_df['rental_end_time'] - transactions_df['rental_start_time']).dt.total_seconds() / 3600

# Display summary statistics for rental duration and total amount
print("Rental Duration (hours) Statistics:")
print(transactions_df['rental_duration_hours'].describe())
print("\nTotal Amount Statistics:")
print(transactions_df['total_amount'].describe())
```

### Cross-Dataset Analysis

```python
# Merge transactions with vehicles to analyze by vehicle type
transactions_vehicles = pd.merge(transactions_df, vehicles_df[['vehicle_id', 'brand', 'vehicle_type']], on='vehicle_id', how='inner')

# Analyze average transaction amount by vehicle type
avg_amount_by_vehicle_type = transactions_vehicles.groupby('vehicle_type')['total_amount'].mean().sort_values(ascending=False)
print(avg_amount_by_vehicle_type)

# Plot average transaction amount by vehicle type
plt.figure(figsize=(12, 6))
avg_amount_by_vehicle_type.plot(kind='bar')
plt.title('Average Transaction Amount by Vehicle Type')
plt.xlabel('Vehicle Type')
plt.ylabel('Average Amount ($)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

## Athena Queries

The Athena queries notebook (`notebooks/athena_queries.ipynb`) demonstrates how to query the processed data using Amazon Athena.

### Setting Up Athena Client

```python
def run_athena_query(query, database, s3_output):
    """
    Run a query on Athena and return the results as a pandas DataFrame.
    
    Args:
        query (str): The SQL query to run
        database (str): The Athena database to query
        s3_output (str): The S3 location to store query results
        
    Returns:
        DataFrame: The query results as a pandas DataFrame
    """
    athena_client = boto3.client('athena', region_name=AWS_REGION)
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    
    # Start the query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output
        }
    )
    
    query_execution_id = response['QueryExecutionId']
    print(f"Query execution ID: {query_execution_id}")
    
    # Wait for the query to complete
    state = 'RUNNING'
    while state in ['RUNNING', 'QUEUED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        
        if state in ['RUNNING', 'QUEUED']:
            print(f"Query is {state}. Waiting...")
            time.sleep(5)
    
    # Check if the query succeeded
    if state == 'SUCCEEDED':
        print("Query succeeded!")
        
        # Get the results
        result_file = f"{s3_output.rstrip('/')}/{query_execution_id}.csv"
        bucket_name = result_file.split('//')[1].split('/')[0]
        key = '/'.join(result_file.split('//')[1].split('/')[1:])
        
        # Download the results
        local_file = 'athena_query_result.csv'
        s3_client.download_file(bucket_name, key, local_file)
        
        # Load the results into a DataFrame
        df = pd.read_csv(local_file)
        
        # Clean up the local file
        os.remove(local_file)
        
        return df
    else:
        error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        print(f"Query failed: {error_message}")
        return None
```

### Query 1: Highest Revenue-Generating Locations

```python
query1 = """
SELECT 
    location_id, 
    location_name, 
    city, 
    state, 
    total_revenue
FROM 
    location_metrics
ORDER BY 
    total_revenue DESC
LIMIT 10
"""

top_revenue_locations = run_athena_query(query1, database_name, s3_output)
top_revenue_locations
```

Visualization:

```python
# Visualize top revenue-generating locations
plt.figure(figsize=(14, 6))
sns.barplot(x='location_name', y='total_revenue', data=top_revenue_locations)
plt.title('Top 10 Revenue-Generating Locations')
plt.xlabel('Location')
plt.ylabel('Total Revenue ($)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
```

### Query 2: Most Rented Vehicle Types

```python
query2 = """
SELECT 
    vehicle_type, 
    total_rentals, 
    total_revenue, 
    avg_rental_amount,
    avg_rental_duration_hours
FROM 
    vehicle_type_metrics
ORDER BY 
    total_rentals DESC
"""

vehicle_type_rentals = run_athena_query(query2, database_name, s3_output)
vehicle_type_rentals
```

Visualization:

```python
# Visualize vehicle type rentals
plt.figure(figsize=(14, 6))
sns.barplot(x='vehicle_type', y='total_rentals', data=vehicle_type_rentals)
plt.title('Total Rentals by Vehicle Type')
plt.xlabel('Vehicle Type')
plt.ylabel('Total Rentals')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Visualize average rental amount by vehicle type
plt.figure(figsize=(14, 6))
sns.barplot(x='vehicle_type', y='avg_rental_amount', data=vehicle_type_rentals)
plt.title('Average Rental Amount by Vehicle Type')
plt.xlabel('Vehicle Type')
plt.ylabel('Average Rental Amount ($)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

### Query 3: Top-Spending Users

```python
query3 = """
SELECT 
    user_id, 
    first_name, 
    last_name, 
    total_spent, 
    total_rentals, 
    avg_rental_amount,
    spending_category
FROM 
    user_metrics
WHERE 
    spending_category = 'Top Spender'
ORDER BY 
    total_spent DESC
LIMIT 20
"""

top_spending_users = run_athena_query(query3, database_name, s3_output)
top_spending_users
```

Visualization:

```python
# Visualize top spending users
plt.figure(figsize=(14, 6))
sns.barplot(x='user_id', y='total_spent', data=top_spending_users.head(10))
plt.title('Top 10 Spending Users')
plt.xlabel('User ID')
plt.ylabel('Total Spent ($)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
```

### Query 4: Daily Transaction Trends

```python
query4 = """
SELECT 
    rental_date, 
    total_transactions, 
    total_revenue, 
    avg_transaction_amount,
    unique_users
FROM 
    daily_metrics
ORDER BY 
    rental_date
"""

daily_trends = run_athena_query(query4, database_name, s3_output)
daily_trends
```

Visualization:

```python
# Convert rental_date to datetime
daily_trends['rental_date'] = pd.to_datetime(daily_trends['rental_date'])

# Visualize daily transaction trends
plt.figure(figsize=(14, 6))
plt.plot(daily_trends['rental_date'], daily_trends['total_transactions'], marker='o', linestyle='-')
plt.title('Daily Transaction Trends')
plt.xlabel('Date')
plt.ylabel('Total Transactions')
plt.grid(True)
plt.tight_layout()
plt.show()

# Visualize daily revenue trends
plt.figure(figsize=(14, 6))
plt.plot(daily_trends['rental_date'], daily_trends['total_revenue'], marker='o', linestyle='-', color='green')
plt.title('Daily Revenue Trends')
plt.xlabel('Date')
plt.ylabel('Total Revenue ($)')
plt.grid(True)
plt.tight_layout()
plt.show()
```

## Visualization Techniques

The project uses several visualization techniques to present the data:

### Bar Charts

Bar charts are used to compare categorical data:

```python
plt.figure(figsize=(14, 6))
sns.barplot(x='location_name', y='total_revenue', data=top_revenue_locations)
plt.title('Top 10 Revenue-Generating Locations')
plt.xlabel('Location')
plt.ylabel('Total Revenue ($)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
```

### Line Charts

Line charts are used to show trends over time:

```python
plt.figure(figsize=(14, 6))
plt.plot(daily_trends['rental_date'], daily_trends['total_transactions'], marker='o', linestyle='-')
plt.title('Daily Transaction Trends')
plt.xlabel('Date')
plt.ylabel('Total Transactions')
plt.grid(True)
plt.tight_layout()
plt.show()
```

### Pie Charts

Pie charts are used to show proportions:

```python
plt.figure(figsize=(8, 6))
active_users_count.plot(kind='pie', autopct='%1.1f%%')
plt.title('Active vs. Inactive Users')
plt.ylabel('')
plt.tight_layout()
plt.show()
```

### Histograms

Histograms are used to show the distribution of numerical data:

```python
plt.figure(figsize=(12, 6))
sns.histplot(transactions_df['total_amount'], bins=30, kde=True)
plt.title('Distribution of Rental Transaction Amounts')
plt.xlabel('Total Amount ($)')
plt.ylabel('Frequency')
plt.grid(True)
plt.tight_layout()
plt.show()
```

### Scatter Plots

Scatter plots are used to show the relationship between two numerical variables:

```python
plt.figure(figsize=(12, 6))
sns.scatterplot(x='rental_duration_hours', y='total_amount', data=transactions_df, alpha=0.5)
plt.title('Relationship Between Rental Duration and Total Amount')
plt.xlabel('Rental Duration (hours)')
plt.ylabel('Total Amount ($)')
plt.grid(True)
plt.tight_layout()
plt.show()
```

## Business Insights

The analysis provides several business insights:

### Revenue Analysis

- **Top Revenue-Generating Locations**: Identify the highest-performing rental locations by revenue
- **Revenue by Vehicle Type**: Analyze which vehicle types generate the most revenue
- **Revenue Trends**: Analyze revenue trends over time

### Customer Analysis

- **Top-Spending Users**: Identify the most valuable customers based on their spending
- **User Spending Categories**: Categorize users based on their spending patterns
- **User Activity**: Analyze user activity patterns

### Operational Analysis

- **Vehicle Utilization**: Analyze how vehicles are utilized across locations
- **Rental Duration**: Analyze rental duration patterns
- **Transaction Patterns**: Analyze transaction patterns by hour of day and day of week

### Geographic Analysis

- **Location Performance**: Analyze performance by location, city, and state
- **Geographic Distribution**: Analyze the geographic distribution of rentals

## Advanced Analytics

The project can be extended with advanced analytics techniques:

### Time Series Analysis

Time series analysis can be used to forecast future demand and revenue:

```python
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima.model import ARIMA

# Resample daily data to weekly
weekly_revenue = daily_trends.set_index('rental_date')['total_revenue'].resample('W').sum()

# Decompose the time series
decomposition = seasonal_decompose(weekly_revenue, model='multiplicative')

# Plot the decomposition
fig = decomposition.plot()
fig.set_size_inches(14, 10)
plt.tight_layout()
plt.show()

# Fit ARIMA model
model = ARIMA(weekly_revenue, order=(1, 1, 1))
model_fit = model.fit()

# Forecast future values
forecast = model_fit.forecast(steps=10)
```

### Clustering Analysis

Clustering analysis can be used to segment users or locations:

```python
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Prepare data for clustering
user_features = user_metrics[['total_rentals', 'total_spent', 'avg_rental_amount', 'avg_rental_duration_hours']]

# Standardize the features
scaler = StandardScaler()
user_features_scaled = scaler.fit_transform(user_features)

# Apply K-means clustering
kmeans = KMeans(n_clusters=4, random_state=42)
user_metrics['cluster'] = kmeans.fit_predict(user_features_scaled)

# Analyze clusters
cluster_analysis = user_metrics.groupby('cluster').agg({
    'total_rentals': 'mean',
    'total_spent': 'mean',
    'avg_rental_amount': 'mean',
    'avg_rental_duration_hours': 'mean',
    'user_id': 'count'
}).reset_index()

# Visualize clusters
plt.figure(figsize=(14, 8))
sns.scatterplot(x='total_spent', y='avg_rental_duration_hours', hue='cluster', data=user_metrics, palette='viridis')
plt.title('User Clusters by Total Spent and Average Rental Duration')
plt.xlabel('Total Spent ($)')
plt.ylabel('Average Rental Duration (hours)')
plt.tight_layout()
plt.show()
```

### Association Rule Mining

Association rule mining can be used to identify patterns in rental behavior:

```python
from mlxtend.frequent_patterns import apriori, association_rules

# Prepare data for association rule mining
user_vehicle_matrix = pd.crosstab(transactions_df['user_id'], transactions_df['vehicle_id'])
user_vehicle_matrix = (user_vehicle_matrix > 0).astype(int)

# Apply Apriori algorithm
frequent_itemsets = apriori(user_vehicle_matrix, min_support=0.01, use_colnames=True)

# Generate association rules
rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1.0)

# Display top rules by lift
top_rules = rules.sort_values('lift', ascending=False).head(10)
print(top_rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']])
```

## Extending the Analysis

The analysis can be extended in several ways:

### Additional Visualizations

- **Interactive Dashboards**: Create interactive dashboards using tools like Plotly or Dash
- **Geospatial Visualizations**: Visualize data on maps using libraries like Folium or Plotly
- **Network Visualizations**: Visualize relationships between entities using libraries like NetworkX

### Integration with Business Intelligence Tools

- **Amazon QuickSight**: Create dashboards and reports using Amazon QuickSight
- **Tableau**: Connect Tableau to Athena for advanced visualization
- **Power BI**: Connect Power BI to Athena for advanced visualization

### Real-Time Analytics

- **Streaming Data**: Process streaming data using Amazon Kinesis
- **Real-Time Dashboards**: Create real-time dashboards using tools like Grafana
- **Alerts and Notifications**: Set up alerts and notifications based on real-time metrics
