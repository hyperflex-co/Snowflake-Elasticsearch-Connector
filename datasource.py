from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from .client import SnowflakeClient, row2doc, generate_id

class SnowflakeDataSource(BaseDataSource):
    """Snowflake Data Source"""

    name = "Snowflake"
    service_type = "snowflake"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        self.account = self.configuration["account"]
        self.user = self.configuration["user"]
        self.password = self.configuration["password"]
        self.private_key = self.configuration.get("private_key")
        self.warehouse = self.configuration["warehouse"]
        self.database = self.configuration["database"]
        self.schema = self.configuration["schema"]
        self.role = self.configuration["role"]
        self.tables = self.configuration["tables"]

        self.fetch_size = self.configuration.get("fetch_size", 5000)

        self.client = None
    
    @classmethod
    def get_default_configuration(cls):
        return {
            "account": {
                "label": "Account Identifier",
                "order": 1,
                "type": "str",
            },
            "user": {
                "label": "Username",
                "order": 2,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 3,
                "type": "str",
                "sensitive": True,
            },
            "private_key": {
                "label": "Private Key (optional)",
                "order": 4,
                "type": "str",
                "required": False,
            },
            "warehouse": {
                "label": "Warehouse",
                "order": 5,
                "type": "str",
            },
            "database": {
                "label": "Database",
                "order": 6,
                "type": "str",
            },
            "schema": {
                "label": "Schema",
                "order": 7,
                "type": "str",
            },
            "role": {
                "label": "Role",
                "order": 8,
                "type": "str",
            },
            "tables": {
                "label": "Comma-separated list of tables",
                "order": 9,
                "type": "list",
                "default_value": ["*"],
            },
            "fetch_size": {
                "label": "Rows fetched per request",
                "order": 10,
                "type": "int",
                "default_value": 5000,
                "required": False,
                "ui_restrictions": ["advanced"],
            },
        }
    
    async def validate_config(self):
        if not self.account:
            raise ConfigurableFieldValueError("Account identifier is required")
        if not self.user:
            raise ConfigurableFieldValueError("User is required")
        if not (self.password or self.private_key):
            raise ConfigurableFieldValueError("Either password or private key is required")
        if not self.database:
            raise ConfigurableFieldValueError("Database is required")
        if not self.schema:
            raise ConfigurableFieldValueError("Schema is required")
        
    async def ping(self):
        async with SnowflakeClient(
            account=self.account,
            user=self.user,
            password=self.password,
            private_key=self.private_key,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role,
            logger_=self._logger,
        ) as client:
            await client.ping()

    async def get_docs(self, filtering=None):
        async with SnowflakeClient(
            account=self.account,
            user=self.user,
            password=self.password,
            private_key=self.private_key,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role,
            logger_=self._logger,
        ) as client:
            
            if self.tables == "*" or self.tables == ["*"]:
                table_names = await client.get_all_table_names()
            else:
                table_names = self.tables

            for table in table_names:
                self._logger.info(f"Syncing table: {table}")

                pk_columns = await client.get_primary_key_column_names(table)
                column_names = await client.get_column_names_for_table(table)

                async for row in client.yield_rows_for_table(
                    table,
                    primary_keys=pk_columns,
                    batch_size=self.fetch_size,
                ):
                    doc = row2doc(
                        row=row,
                        column_names=column_names,
                        primary_key_columns=pk_columns,
                        table=table,
                        timestamp=self._generate_ingest_timestamp(),
                    )

                    doc_id = generate_id(table, row, pk_columns)

                    yield doc_id, doc