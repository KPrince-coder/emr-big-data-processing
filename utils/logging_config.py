#!/usr/bin/env python3
"""
Logging Configuration Utility

This module provides a standardized logging configuration for all scripts in the project.
It ensures consistent log formatting and behavior across the application.
"""

import logging


def configure_logger(name=None) -> logging.Logger:
    """
    Configure and return a logger with standardized settings.

    Args:
        name (str, optional): The name for the logger. If None, returns the root logger.
            Defaults to None.

    Returns:
        logging.Logger: A configured logger instance
    """
    # Configure the basic logging settings
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Get the logger (either the named logger or root logger)
    logger = logging.getLogger(name)

    # Ensure all handlers use the same format
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    return logger


# For direct imports
logger = configure_logger(__name__)
