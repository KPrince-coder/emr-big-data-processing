# Setup IAM Roles and Permissions
# This PowerShell script is a wrapper for the setup_iam_roles.py Python script

param (
    [Parameter(Mandatory=$false, HelpMessage="AWS region")]
    [string]$Region = ""
)

# Build the command
$cmd = "python scripts/setup_iam_roles.py"

if ($Region -ne "") {
    $cmd += " --region $Region"
}

# Execute the command
Write-Host "Executing: $cmd"
Invoke-Expression $cmd
