"""
This module sets up logging for the entire sync service.

Why do we need this?
--------------------
Every part of your project (Snowflake client, Elasticsearch client,
sync service, etc.) should log messages in a consistent format.

This file creates a simple function `setup_logging(level)` that:
  - sets the global log level (INFO, DEBUG, etc.)
  - formats logs with timestamps
  - prints logs to the console
"""

import logging


def setup_logging(level="INFO"):
    """
    Configure global logging settings.

    level: string like "DEBUG", "INFO", "WARNING", "ERROR"
    """

    # Convert string level to actual logging constant
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Configure the root logger
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Optional: print a startup message
    logging.getLogger("Logging").info(f"Logging initialized at level: {level}")
