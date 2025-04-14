# Environment Variables Management

This document explains how environment variables are managed in the Big Data EMR project, including the recent improvements to ensure environment variables are always up-to-date.

## Overview

The project uses environment variables to store configuration settings that may change between environments or contain sensitive information. These variables are stored in a `.env` file at the project root and are loaded into the application using the `python-dotenv` library.

## Environment Variables File (.env)

The `.env` file is located at the project root and contains key-value pairs in the format:

```python
KEY=value
```

Example:

```python
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
S3_BUCKET_NAME=emr-car-rental-data-lake
AWS_REGION=us-east-1
```

## Loading Environment Variables

Environment variables are loaded through the `config/env_loader.py` module, which provides several functions for accessing environment variables.

### Recent Improvements

The environment variable loading system was recently improved to ensure that scripts always use the latest values from the `.env` file, even if the file is modified after the application has started.

Key improvements include:

1. **Dynamic Reloading**: Environment variables can now be reloaded at runtime to ensure the latest values are used.
2. **Centralized Management**: All environment variable access goes through a single module, ensuring consistent behavior.
3. **Flexible Configuration**: Scripts can choose whether to use cached values or reload from the `.env` file.

## Key Functions

### `reload_env_vars()`

Reloads environment variables from the `.env` file, ensuring that any changes to the file are reflected in the application.

```python
def reload_env_vars():
    """
    Reload environment variables from the .env file.
    This ensures we always have the latest values.
    """
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        logger.debug(f"Loaded environment variables from {env_path}")
    else:
        logger.warning(f".env file not found at {env_path}")
```

### `get_env_var(var_name, default=None, required=False, reload=False)`

Gets the value of an environment variable, with options to specify a default value, mark the variable as required, and reload from the `.env` file.

Parameters:

- `var_name` (str): Name of the environment variable
- `default`: Default value to return if the variable is not set
- `required` (bool): Whether the variable is required
- `reload` (bool): Whether to reload environment variables before getting the value

Returns:

- The value of the environment variable, or the default value

Raises:

- `ValueError`: If the variable is required but not set

```python
def get_env_var(var_name: str, default=None, required=False, reload=False) -> str:
    """
    Get an environment variable.

    Args:
        var_name (str): Name of the environment variable
        default: Default value to return if the variable is not set
        required (bool): Whether the variable is required
        reload (bool): Whether to reload environment variables before getting the value

    Returns:
        The value of the environment variable, or the default value

    Raises:
        ValueError: If the variable is required but not set
    """
    # Optionally reload environment variables to ensure we have the latest values
    if reload:
        reload_env_vars()

    value = os.getenv(var_name, default)

    if required and value is None:
        logger.error(f"Required environment variable {var_name} is not set")
        raise ValueError(f"Required environment variable {var_name} is not set")

    return value
```

### `get_aws_bucket()`

Gets the AWS S3 bucket name from environment variables, always reloading from the `.env` file to ensure the latest value is used.

```python
def get_aws_bucket() -> str:
    """
    Get AWS bucket name from environment variables.
    Always reloads from .env file to ensure we have the latest value.

    Returns:
        str: AWS bucket name
    """
    return get_env_var("S3_BUCKET_NAME", required=True, reload=True)
```

## Usage Examples

### Basic Usage

```python
from config.env_loader import get_env_var

# Get an environment variable with a default value
region = get_env_var("AWS_REGION", default="us-east-1")

# Get a required environment variable
access_key = get_env_var("AWS_ACCESS_KEY_ID", required=True)
```

### Ensuring Latest Values

```python
from config.env_loader import get_env_var, reload_env_vars

# Reload all environment variables
reload_env_vars()

# Get an environment variable, ensuring it's the latest value
bucket_name = get_env_var("S3_BUCKET_NAME", reload=True)
```

### Getting AWS Configuration

```python
from config.env_loader import get_aws_bucket, get_aws_region

# Get the S3 bucket name (always reloads from .env)
bucket_name = get_aws_bucket()

# Get the AWS region
region = get_aws_region()
```

## Best Practices

1. **Use the Provided Functions**: Always use the functions in `env_loader.py` to access environment variables, rather than using `os.getenv()` directly.

2. **Reload When Necessary**: Use the `reload=True` parameter when you need to ensure you have the latest value from the `.env` file, especially for values that might change during development.

3. **Centralize Configuration**: Keep all environment variable access centralized in the `env_loader.py` module to ensure consistent behavior.

4. **Handle Missing Variables**: Use the `required=True` parameter for variables that must be set, and provide sensible defaults for optional variables.

5. **Protect Sensitive Information**: Never commit sensitive information (like API keys) to version control. Always use environment variables for such values.

## Troubleshooting

If you're experiencing issues with environment variables:

1. **Check the .env File**: Ensure the `.env` file exists at the project root and contains the expected variables.

2. **Verify Variable Names**: Make sure the variable names in your code match those in the `.env` file.

3. **Force Reload**: Use `reload_env_vars()` to force a reload of all environment variables.

4. **Enable Debug Logging**: Set the logging level to DEBUG to see more information about environment variable loading.

5. **Check for Typos**: Common issues include typos in variable names or values.

## Conclusion

The improved environment variable management system ensures that your application always uses the latest configuration values, making development and deployment more flexible and reliable. By centralizing environment variable access and providing options for dynamic reloading, the system helps prevent configuration-related issues and simplifies configuration management.
