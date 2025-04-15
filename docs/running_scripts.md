# Running Scripts in the Project

This document outlines different approaches for running scripts in this project, their advantages, and how they work.

## Methods for Running Scripts

There are several ways to run Python scripts in this project. Each method has its own advantages and use cases.

### 1. Using the Module Approach (`python -m`)

```bash
python -m path.to.script
```

#### How It Works

- Python treats the script as a module within the package structure
- Imports are resolved relative to the project root
- The script is executed in the context of the package

#### Advantages

- Maintains proper package structure
- Resolves import paths correctly
- Works consistently regardless of the current working directory
- Recommended for scripts that are part of a larger package

#### Example

```bash
# From project root
python -m scripts.setup_iam_roles
```

### 2. Adding Project Root to Python Path

```python
# At the top of your script
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

#### How It Works

- Manually adds the project root directory to the Python path
- Allows imports from the project root
- Script can be run directly with `python script.py`

#### Advantages

- Scripts can be run directly without module syntax
- Works when scripts need to import from other parts of the project
- Useful for standalone scripts that still need project imports

#### Example

```bash
# From any directory
python scripts/setup_iam_roles.py
```

### 3. Using Relative Imports with Package Structure

```python
# In a script within a package
from ..utils import helper
```

#### How It Works

- Uses Python's relative import syntax
- Requires running as a module (method 1)
- Follows Python package structure rules

#### Advantages

- Clean import statements
- Follows Python's intended package structure
- Maintainable in larger projects

### 4. Using Environment Variables for Path Resolution

```bash
# Set PYTHONPATH environment variable
export PYTHONPATH=/path/to/project:$PYTHONPATH
python scripts/script.py
```

#### How It Works

- Sets the PYTHONPATH environment variable to include the project root
- Python automatically searches these paths when resolving imports

#### Advantages

- Works across different scripts without code changes
- Can be set in shell profiles for persistence
- Useful for development environments

### 5. Installing the Project as a Development Package

```bash
# From project root
pip install -e .
```

#### How It Works

- Installs the project as an editable package
- Requires a proper setup.py file
- Makes the package importable from anywhere

#### Advantages

- Most "production-like" approach
- Clean imports without path manipulation
- Changes to code are immediately available
- Recommended for larger projects

## Best Practices

1. **Consistency**: Choose one approach and use it consistently throughout the project
2. **Documentation**: Document the chosen approach for other developers
3. **Virtual Environments**: Always use virtual environments to isolate dependencies
4. **Package Management**: Use UV (recommended) or pip for package management
5. **Testing**: Ensure scripts can be run in both development and production environments
6. **CI/CD**: Verify that your chosen method works in CI/CD pipelines

## Recommended Approach

For this project, we recommend using the **module approach** (Method 1) for running scripts as it provides the best balance of correctness and ease of use. This approach ensures proper package structure while avoiding path manipulation.

```bash
# Examples of recommended approach
python -m scripts.setup_iam_roles
python -m scripts.setup_s3_bucket --bucket-name your-bucket-name --region your-region
```

This approach works well with the project's structure and ensures consistent behavior across different environments.

## Package Management with UV

This project recommends using [UV](https://github.com/astral-sh/uv) as the Python package manager. UV is a fast, reliable Python package installer and resolver, built in Rust.

### Installing UV

```bash
curl -sSf https://install.astral.sh | sh
```

### Installing Dependencies with UV

```bash
# Install dependencies from requirements.txt
uv pip install -r requirements.txt

# Install a specific package
uv pip install package-name
```

### Advantages of UV

- Significantly faster than pip (10-100x faster)
- More reliable dependency resolution
- Compatible with pip's command-line interface
- Built-in virtual environment management
- Improved caching and parallelization
