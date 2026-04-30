"""
Main entrypoint for the Snowflake → Elasticsearch sync service.

This file:
  - loads config.yaml
  - sets up logging
  - creates the SyncService
  - supports command-line flags:
        --once         run one sync cycle and exit
        --reset-state  reset state.json to epoch
  - otherwise runs the sync loop forever
"""

import argparse
import yaml
import logging
from datetime import datetime

from services.sync_service import SyncService
from utils.logging import setup_logging


def load_config(path="config/config.yaml"):
    """
    Load the YAML configuration file.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)


def reset_state_file(path):
    """
    Reset state.json to the Unix epoch.
    """
    with open(path, "w") as f:
        f.write('{"last_timestamp": "1970-01-01T00:00:00"}\n')
    print(f"State file reset: {path}")


def main():
    # -----------------------------------------
    # Parse command-line arguments
    # -----------------------------------------
    parser = argparse.ArgumentParser(description="Snowflake → Elasticsearch Sync Service")
    parser.add_argument("--once", action="store_true", help="Run one sync cycle and exit")
    parser.add_argument("--reset-state", action="store_true", help="Reset state.json to epoch")
    args = parser.parse_args()

    # -----------------------------------------
    # Load config
    # -----------------------------------------
    config = load_config()

    # -----------------------------------------
    # Setup logging
    # -----------------------------------------
    setup_logging(config["service"]["log_level"])
    logger = logging.getLogger("Main")

    # -----------------------------------------
    # Handle --reset-state
    # -----------------------------------------
    if args.reset_state:
        reset_state_file(config["service"]["state_file"])
        return

    # -----------------------------------------
    # Create the sync service
    # -----------------------------------------
    service = SyncService(config)

    # -----------------------------------------
    # Run once or forever
    # -----------------------------------------
    if args.once:
        logger.info("Running a single sync cycle...")
        service.run_once()
    else:
        logger.info("Starting continuous sync loop...")
        service.run_forever()


if __name__ == "__main__":
    main()
