#!/bin/bash
# Setup S3 Bucket and Load Data
# This script is a wrapper for the setup_s3_bucket.py Python script

# Default values
BUCKET_NAME=""
REGION=""
DATA_DIR="data"
SPARK_DIR="spark"

# Display usage information
function show_usage {
    echo "Usage: $0 --bucket-name <bucket-name> [--region <region>] [--data-dir <data-dir>] [--spark-dir <spark-dir>]"
    echo ""
    echo "Options:"
    echo "  --bucket-name <bucket-name>  Name of the S3 bucket to create (required)"
    echo "  --region <region>            AWS region (default: use AWS CLI configuration)"
    echo "  --data-dir <data-dir>        Directory containing data files (default: data)"
    echo "  --spark-dir <spark-dir>      Directory containing Spark scripts (default: spark)"
    echo ""
    echo "Example:"
    echo "  $0 --bucket-name my-car-rental-data --region us-east-1"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --bucket-name)
            BUCKET_NAME="$2"
            shift
            shift
            ;;
        --region)
            REGION="$2"
            shift
            shift
            ;;
        --data-dir)
            DATA_DIR="$2"
            shift
            shift
            ;;
        --spark-dir)
            SPARK_DIR="$2"
            shift
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Check if bucket name is provided
if [ -z "$BUCKET_NAME" ]; then
    echo "Error: Bucket name is required"
    show_usage
    exit 1
fi

# Build the command
CMD="python scripts/setup_s3_bucket.py --bucket-name $BUCKET_NAME"

if [ ! -z "$REGION" ]; then
    CMD="$CMD --region $REGION"
fi

if [ ! -z "$DATA_DIR" ]; then
    CMD="$CMD --data-dir $DATA_DIR"
fi

if [ ! -z "$SPARK_DIR" ]; then
    CMD="$CMD --spark-dir $SPARK_DIR"
fi

# Execute the command
echo "Executing: $CMD"
eval $CMD
