import snowflake.connector
import logging
from datetime import datetime


class SnowflakeClient:
    """
    A simple wrapper around the Snowflake Python connector.
    This class handles:
      - connecting to Snowflake
      - running queries
      - fetching table metadata
      - fetching rows incrementally (using a timestamp column)
    """

    def __init__(
        self,
        account,
        user,
        password=None,
        private_key=None,
        warehouse=None,
        database=None,
        schema=None,
        role=None,
        logger=None,
    ):
        """
        Store connection settings. We do NOT connect yet.
        """
        self.account = account
        self.user = user
        self.password = password
        self.private_key = private_key
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role

        self.logger = logger or logging.getLogger("SnowflakeClient")
        self.connection = None

    def connect(self):
        """
        Create a Snowflake connection.
        This is synchronous because the Snowflake connector is synchronous.
        """
        try:
            params = {
                "account": self.account,
                "user": self.user,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
                "role": self.role,
            }

            # Choose password or private key authentication
            if self.private_key:
                with open(self.private_key, "rb") as key_file:
                    private_key_bytes = key_file.read()
                params["private_key"] = private_key_bytes
            else:
                params["password"] = self.password

            self.connection = snowflake.connector.connect(**params)
            self.logger.info("Connected to Snowflake successfully")

        except Exception as e:
            self.logger.exception(f"Error connecting to Snowflake: {e}")
            raise

    def close(self):
        """Close the Snowflake connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def ping(self):
        """Simple test query to confirm Snowflake is reachable."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            self.logger.info("Snowflake ping successful")
        except Exception as e:
            self.logger.exception(f"Snowflake ping failed: {e}")
            raise

    def fetch_all(self, query):
        """
        Run a query and return all rows.
        Useful for metadata queries.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return rows, columns
        except Exception as e:
            self.logger.exception(f"Query failed: {e}")
            raise

    def get_primary_key_columns(self, table):
        """
        Return the primary key columns for a table.
        """
        query = f"DESCRIBE TABLE {self.database}.{self.schema}.{table}"
        rows, _ = self.fetch_all(query)

        # Column 5 in DESCRIBE TABLE output indicates PK = 'Y'
        pk_columns = [row[0] for row in rows if row[5] == "Y"]
        return pk_columns

    def get_column_names(self, table):
        """
        Return all column names for a table.
        """
        query = f"SHOW COLUMNS IN TABLE {self.database}.{self.schema}.{table}"
        rows, _ = self.fetch_all(query)

        # Column 0 is the column name
        return [row[0] for row in rows]

    def fetch_changes_since(self, table, timestamp_column, last_timestamp, batch_size=5000):
        """
        Fetch rows where timestamp_column > last_timestamp.
        This is the core of incremental sync.

        Returns rows in batches (generator).
        """
        import datetime

        cursor = self.connection.cursor()

        # Convert timestamp to Snowflake-friendly string
        last_ts_str = last_timestamp.strftime("%Y-%m-%d %H:%M:%S")

        while True:
            query = f"""
                SELECT *
                FROM {self.database}.{self.schema}.{table}
                WHERE {timestamp_column} > '{last_ts_str}'
                ORDER BY {timestamp_column} ASC
                LIMIT {batch_size}
            """

            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                break

            columns = [col[0] for col in cursor.description]

            # Yield each row as a dict
            for row in rows:
                row_dict = dict(zip(columns, row))

                # Normalize DATE → DATETIME
                ts = row_dict[timestamp_column]
                if isinstance(ts, datetime.date) and not isinstance(ts, datetime.datetime):
                    ts = datetime.datetime.combine(ts, datetime.time.min)
                    row_dict[timestamp_column] = ts

                yield row_dict

            # Update last_ts_str to the newest row's timestamp
            newest_ts = row_dict[timestamp_column]
            last_ts_str = newest_ts.strftime("%Y-%m-%d %H:%M:%S")

    def fetch_full_table(self, table, batch_size=5000):
        """
        Fetch the entire table in batches.
        Useful for the first full sync.
        """
        cursor = self.connection.cursor()
        offset = 0

        while True:
            query = f"""
                SELECT *
                FROM {self.database}.{self.schema}.{table}
                LIMIT {batch_size} OFFSET {offset}
            """

            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                break

            columns = [col[0] for col in cursor.description]

            for row in rows:
                yield dict(zip(columns, row))

            offset += batch_size
