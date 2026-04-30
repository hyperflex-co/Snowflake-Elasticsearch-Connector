import time
import json
import logging
from datetime import datetime

from clients.snowflake_client import SnowflakeClient
from clients.elastic_client import ElasticClient
from utils.row_conversion import convert_row_to_doc
from utils.id_generation import generate_doc_id


class SyncService:
    """
    This class runs the full Snowflake → Elasticsearch sync process.
    It handles:
      - loading state (last sync timestamp)
      - connecting to Snowflake + Elasticsearch
      - fetching new/updated rows from Snowflake
      - converting rows into Elasticsearch documents
      - bulk indexing into Elasticsearch
      - updating the state file
      - repeating on a schedule
    """

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("SyncService")

        # Extract Snowflake settings
        sf_cfg = config["snowflake"]
        self.snowflake = SnowflakeClient(
            account=sf_cfg["account"],
            user=sf_cfg["user"],
            password=sf_cfg["password"],
            private_key=sf_cfg["private_key"],
            warehouse=sf_cfg["warehouse"],
            database=sf_cfg["database"],
            schema=sf_cfg["schema"],
            role=sf_cfg["role"],
            logger=self.logger,
        )

        # Extract Elasticsearch settings
        es_cfg = config["elasticsearch"]
        self.elasticsearch = ElasticClient(
            host=es_cfg["host"],
            api_key=es_cfg["api_key"],
            username=es_cfg["username"],
            password=es_cfg["password"],
            logger=self.logger,
        )

        # Service settings
        svc_cfg = config["service"]
        self.interval = svc_cfg["interval_seconds"]
        self.state_file = svc_cfg["state_file"]

        # Table + incremental column
        self.table = sf_cfg["table"]
        self.timestamp_column = sf_cfg["timestamp_column"]
        self.batch_size = sf_cfg["batch_size"]

        # Elasticsearch index
        self.index = es_cfg["index"]

    # ---------------------------------------------------------
    # State Management
    # ---------------------------------------------------------

    def load_state(self):
        """
        Load the last sync timestamp from state/state.json.
        If the file does not exist, start from 1970 (full sync).
        """
        try:
            with open(self.state_file, "r") as f:
                data = json.load(f)
                last_ts = datetime.fromisoformat(data["last_timestamp"])
                self.logger.info(f"Loaded last sync timestamp: {last_ts}")
                return last_ts
        except FileNotFoundError:
            self.logger.warning("State file not found. Starting full sync.")
            return datetime(1970, 1, 1)
        except Exception as e:
            self.logger.exception(f"Error loading state: {e}")
            return datetime(1970, 1, 1)

    def save_state(self, timestamp):
        """
        Save the new last sync timestamp to state/state.json.
        """
        try:
            with open(self.state_file, "w") as f:
                json.dump({"last_timestamp": timestamp.isoformat()}, f, indent=2)
            self.logger.info(f"Updated state timestamp to: {timestamp}")
        except Exception as e:
            self.logger.exception(f"Error saving state: {e}")

    # ---------------------------------------------------------
    # Sync Logic
    # ---------------------------------------------------------

    def run_once(self):
        """
        Run a single sync cycle:
          - connect to Snowflake + Elasticsearch
          - fetch new rows
          - index them
          - update state
        """
        self.logger.info("Starting sync cycle...")

        # Connect to Snowflake
        self.snowflake.connect()
        self.snowflake.ping()

        # Check Elasticsearch
        self.elasticsearch.ping()
        self.elasticsearch.ensure_index(self.index)

        # Load last sync timestamp
        last_ts = self.load_state()
        newest_ts = last_ts

        # Fetch new/updated rows from Snowflake
        self.logger.info(f"Fetching rows updated after {last_ts}")

        docs_to_index = []

        for row in self.snowflake.fetch_changes_since(
            table=self.table,
            timestamp_column=self.timestamp_column,
            last_timestamp=last_ts,
            batch_size=self.batch_size,
        ):
            # Convert row → Elasticsearch document
            doc = convert_row_to_doc(row)

            # Generate deterministic ID
            doc_id = generate_doc_id(self.table, row)

            docs_to_index.append({"_id": doc_id, "_source": doc})

            # Track newest timestamp seen
            row_ts = row[self.timestamp_column]
            if row_ts > newest_ts:
                newest_ts = row_ts

        # Index into Elasticsearch
        if docs_to_index:
            self.logger.info(f"Indexing {len(docs_to_index)} documents...")
            self.elasticsearch.bulk_index(self.index, docs_to_index)
        else:
            self.logger.info("No new documents to index.")

        # Update state
        self.save_state(newest_ts)

        # Close Snowflake connection
        self.snowflake.close()

        self.logger.info("Sync cycle complete.")

    # ---------------------------------------------------------
    # Periodic Loop
    # ---------------------------------------------------------

    def run_forever(self):
        """
        Run the sync loop forever.
        """
        self.logger.info("Starting continuous sync service...")

        while True:
            try:
                self.run_once()
            except Exception as e:
                self.logger.exception(f"Sync cycle failed: {e}")

            self.logger.info(f"Sleeping for {self.interval} seconds...")
            time.sleep(self.interval)
