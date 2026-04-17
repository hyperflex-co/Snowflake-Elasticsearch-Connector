import snowflake.connector

class SnowflakeClient:
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
        fetch_size=1000,
        logger_=None,
    ):
        self.account = account
        self.user = user
        self.password = password
        self.private_key = private_key
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.fetch_size = fetch_size
        self.connection = None
        self._logger = logger_

    async def __aenter__(self):
        # create and return connection
        self.connection_params = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role
        }

        if self.private_key:
            self.connection_params["private_key"] = self.private_key
        else:
            self.connection_params["password"] = self.password
        
        self.connection = snowflake.connector.connect(**self.connection_params)

        return self

    async def __aexit__(self, exc_type, exc, tb):
        # close connection
        if self.connection:
            self.connection.close()
            self.connection = None

    async def ping(self):
        # test connection
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            self._logger.info("Successful connected to the Snowflake Server.")
        except Exception:
            self._logger.exception("Error while connecting to the Snowflake Server.")
            raise

    async def get_all_table_names(self):
        # return list of tables in the schema
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW TABLES IN SCHEMA {self.database}.{self.schema}")
            results = cursor.fetchall()

            table_names = [row[1] for row in results]
            return table_names
        except Exception:
            self._logger.exception("Error fetching table names from Snowflake.")
            raise


    async def get_column_names_for_table(self, table):
        # return list of column names for a table
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW COLUMNS IN TABLE {self.database}.{self.schema}.{table}")
            results = cursor.fetchall()

            column_names = [row[0] for row in results]
            return column_names
        except Exception:
            self._logger.exception("Error fetching column names from Snowflake.")
            raise
    
    async def get_column_types_for_table(self, table):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW COLUMNS IN TABLE {self.database}.{self.schema}.{table}")
            results = cursor.fetchall()

            column_types = {row[0]: row[1] for row in results}
            return column_types
        except Exception:
            self._logger.exception("Error fetching column types from Snowflake.")
            raise

    async def get_primary_key_column_names(self, table):
        # return list of primary key columns
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DESCRIBE TABLE {self.database}.{self.schema}.{table}")
            results = cursor.fetchall()

            pk_columns = [row[0] for row in results if row[5] == 'Y']
            return pk_columns
        except Exception:
            self._logger.exception("Error fetching primary key columns from Snowflake.")
            raise

    async def get_column_names_for_query(self, query):
        # return list of column names for a custom query
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            column_names = [col[0] for col in cursor.description]

            return column_names
        except Exception:
            self._logger.exception("Error fetching column names for query from Snowflake.")
            raise
    
    def _format_value(self, value):
        if value is None:
            return "NULL"
        if isinstance(value, str):
            return f"'{value.replace(chr(39), chr(39) + chr(39))}'"
        return str(value)
    
    async def yield_rows_for_table(self, table, primary_keys, batch_size=5000):
        try:
            cursor = self.connection.cursor()
            order_by = ", ".join(primary_keys)
            last_pk = None

            while True:
                if last_pk is None:
                    query = (
                        f"SELECT * FROM {self.database}.{self.schema}.{table} "
                        f"ORDER BY {order_by} "
                        f"LIMIT {batch_size}"
                    )
                else:
                    pk_cols = ", ".join(primary_keys)
                    pk_tuple = ", ".join(self.__format_value(v) for v in last_pk)
                    query = (
                        f"SELECT * FROM {self.database}.{self.schema}.{table} "
                        f"WHERE ({pk_cols}) > ({pk_tuple}) "
                        f"ORDER BY {order_by} "
                        f"LIMIT {batch_size}"
                    )
                
                cursor.execute(query)
                rows = cursor.fetchall()
                if not rows: break
                column_names = [col[0] for col in cursor.description]
                for row in rows: yield dict(zip(column_names, row))

                last_row = rows[-1]
                last_pk = tuple(last_row[column_names.index(pk)] for pk in primary_keys)
        except Exception:
            self._logger.exception("Error during PK-based pagination in Snowflake")
            raise
    
    async def yield_rows_for_query(self, query, primary_keys, batch_size=5000):
        try:
            cursor = self.connection.cursor()
            order_by = ", ".join(primary_keys)
            offset = 0

            while True:
                paginated_query = (
                    f"SELECT * FROM ({query}) AS sub "
                    f"ORDER BY {order_by} "
                    f"LIMIT {batch_size} OFFSET {offset}"
                )

                cursor.execute(paginated_query)
                rows = cursor.fetchall()
                if not rows:
                    break

                column_names = [col[0] for col in cursor.description]
                for row in rows:
                    yield dict(zip(column_names, row))

                offset += batch_size
        except Exception:
            self._logger.exception("Error during OFFSET-based pagination of query in Snowflake")
            raise

def row2doc(row, column_names, primary_key_columns, table, timestamp):
    # convert row → Elasticsearch document
    doc = {}

    for idx, col in enumerate(column_names):
        value = row[idx]

        if isinstance(value, (dict, list)):
            doc[col] = value
        else:
            doc[col] = value
    
    doc["_table"] = table
    doc["_pk"] = {pk: doc[pk] for pk in primary_key_columns}
    doc["_ingest_ts"] = timestamp
    return doc


def generate_id(table, row, primary_key_columns):
    # deterministic ID generation
    parts = [table]

    for pk in primary_key_columns:
        value = row[pk]
        parts.append(f"{pk}={value}")

    return "/".join(parts)
