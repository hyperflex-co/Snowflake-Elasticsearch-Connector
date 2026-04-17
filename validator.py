from connectors_sdk.source import BaseAdvancedRulesValidator

class SnowflakeAdvancedRulesValidator(BaseAdvancedRulesValidator):
    service_type= "snowflake"

    async def validate(self, rules, datasource):
        validation = {
            "valid": True,
            "errors": [],
        }

        if not rules:
            return validation
        
        if "tables" in rules:
            for table in rules["tables"]:
                if not await self._table_exists(table, datasource):
                    validation["valid"]= False
                    validation["errors"].append(f"Table {table} does not exist in Snowflake.")

        if "fields" in rules:
            for table, fields in rules["fields"].items():
                for field in fields:
                    if not await self._column_exists(table, field, datasource):
                        validation["valid"]= False
                        validation["errors"].append(f"Column {field} does not exist in Table {table}.")

        return validation
    
    async def _table_exists(self, table, datasource):
        try:
            async with datasource.client_class(
                account = datasource.account,
                user = datasource.user,
                password = datasource.password,
                private_key = datasource.private_key,
                warehouse = datasource.warehouse,
                database = datasource.database,
                schema = datasource.schema,
                role = datasource.role,
                logger_ = datasource._logger,
            ) as client:
                tables = await client.get_all_table_names()
                return table in tables
        except Exception:
            return False
        
    async def _column_exists(self, table, column, datasource):
        try:
            async with datasource.client_class(
                account = datasource.account,
                user = datasource.user,
                password = datasource.password,
                private_key = datasource.private_key,
                warehouse = datasource.warehouse,
                database = datasource.database,
                schema = datasource.schema,
                role = datasource.role,
                logger_ = datasource._logger,
            ) as client:
                columns = await client.get_column_names_for_table(table)
                return column in columns
        except Exception:
            return False