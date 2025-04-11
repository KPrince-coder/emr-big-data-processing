# Setup S3 Bucket and Load Data
# This PowerShell script is a wrapper for the setup_s3_bucket.py Python script

param (
    [Parameter(Mandatory=$true, HelpMessage="Name of the S3 bucket to create")]
    [string]$BucketName,
    
    [Parameter(Mandatory=$false, HelpMessage="AWS region")]
    [string]$Region = "",
    
    [Parameter(Mandatory=$false, HelpMessage="Directory containing data files")]
    [string]$DataDir = "data",
    
    [Parameter(Mandatory=$false, HelpMessage="Directory containing Spark scripts")]
    [string]$SparkDir = "spark"
)

# Build the command
$cmd = "python scripts/setup_s3_bucket.py --bucket-name $BucketName"

if ($Region -ne "") {
    $cmd += " --region $Region"
}

if ($DataDir -ne "") {
    $cmd += " --data-dir $DataDir"
}

if ($SparkDir -ne "") {
    $cmd += " --spark-dir $SparkDir"
}

# Execute the command
Write-Host "Executing: $cmd"
Invoke-Expression $cmd
