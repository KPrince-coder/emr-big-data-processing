# Big Data Processing with AWS EMR

This project demonstrates how to process big data using AWS EMR (Elastic MapReduce) with a car rental marketplace dataset. The project leverages Spark on EMR to process and transform raw data stored in Amazon S3, and integrates AWS Glue, Athena, and Step Functions to create a complete data pipeline.

## Project Structure

```markdown
.
├── README.md                 # Project documentation
├── CHANGELOG.md              # Track project progress
├── ROADMAP.md                # Project roadmap and technical explanations
├── config/                   # Configuration files
│   ├── aws_config.py         # AWS configuration settings
│   ├── env_loader.py         # Environment variable loader
│   └── emr_config.json       # EMR cluster configuration
├── .env                      # Environment variables (not in version control)
├── data/                     # Sample data (small versions for testing)
│   ├── vehicles/
│   ├── users/
│   ├── locations/
│   └── transactions/
├── notebooks/                # Jupyter notebooks for development and testing
│   ├── data_exploration.ipynb
│   ├── local_spark_testing.ipynb
│   └── athena_queries.ipynb
├── scripts/                  # Shell and Python scripts
│   ├── upload_data.py        # Script to upload data to S3
│   ├── create_emr_cluster.py # Script to create EMR cluster
│   └── setup_glue_crawlers.py # Script to set up Glue crawlers
├── spark/                    # Spark jobs
│   ├── job1_vehicle_location_metrics.py
│   └── job2_user_transaction_analysis.py
└── step_functions/           # Step Functions workflow definition
    └── data_pipeline_workflow.json
```

## Prerequisites

- AWS Account with appropriate permissions
- Python 3.7+
- AWS CLI configured
- Boto3 (AWS SDK for Python)
- PySpark (for local testing)

## Getting Started

1. Clone this repository
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Configure the environment variables:
   - Copy the `.env.example` file to `.env`
   - Edit the `.env` file with your AWS credentials and configuration
4. Run the data pipeline:
   ```
   python main.py
   ```

   Or to only deploy the infrastructure without running jobs:
   ```
   python main.py --deploy-only
   ```

   Or to execute the Step Functions workflow:
   ```
   python main.py --run-workflow
   ```

## Documentation

See the [ROADMAP.md](ROADMAP.md) file for detailed explanations of the technical components and project requirements.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
