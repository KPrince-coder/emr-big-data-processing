#!/bin/bash
# Setup IAM Roles and Permissions
# This script is a wrapper for the setup_iam_roles.py Python script

# Default values
REGION=""

# Display usage information
function show_usage {
    echo "Usage: $0 [--region <region>]"
    echo ""
    echo "Options:"
    echo "  --region <region>  AWS region (default: use AWS CLI configuration)"
    echo ""
    echo "Example:"
    echo "  $0 --region us-east-1"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --region)
            REGION="$2"
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

# Build the command
CMD="python scripts/setup_iam_roles.py"

if [ ! -z "$REGION" ]; then
    CMD="$CMD --region $REGION"
fi

# Execute the command
echo "Executing: $CMD"
eval $CMD
