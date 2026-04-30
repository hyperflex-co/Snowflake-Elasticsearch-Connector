"""
This module generates a deterministic Elasticsearch document ID
for each Snowflake row.

Why do we need this?
--------------------
Elasticsearch requires an ID for each document. If we generate the same ID
every time for the same row, Elasticsearch will UPDATE the document instead
of creating duplicates.

We build the ID using:
  - the table name
  - the primary key values from the row

Example ID:
  "ORDERS/ORDER_ID=12345"
"""

def generate_doc_id(table_name, row, primary_keys=None):
    """
    Create a stable document ID based on the table name and primary key values.

    table_name: name of the Snowflake table (string)
    row: a dictionary representing a Snowflake row
    primary_keys: list of PK column names (optional)

    If primary_keys is None, we fall back to ALL columns (not recommended).
    """

    # If primary keys are not provided, use all columns (fallback)
    if primary_keys is None:
        primary_keys = list(row.keys())

    # Build ID parts like: ["ORDERS", "ORDER_ID=12345", "LINE_ID=7"]
    parts = [table_name]

    for pk in primary_keys:
        value = row.get(pk)
        parts.append(f"{pk}={value}")

    # Join with "/" to form the final ID
    return "/".join(parts)
