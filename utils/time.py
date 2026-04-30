"""
This module converts a Snowflake row (a Python dict)
into a clean Elasticsearch document.

Why do we need this?
--------------------
Snowflake rows come in directly from the connector as Python dictionaries.
Elasticsearch expects documents to be JSON-serializable.

This file:
  - cleans values
  - converts timestamps to ISO strings
  - ensures all values are safe for Elasticsearch
  - adds metadata fields if needed
"""

from datetime import datetime


def convert_value(value):
    """
    Convert a single value into something Elasticsearch can store.

    Rules:
      - datetime → ISO string
      - None → None (ES stores it as null)
      - lists/dicts → keep as-is
      - everything else → keep as-is
    """
    if isinstance(value, datetime):
        return value.isoformat()

    # Lists and dicts are already JSON-safe
    if isinstance(value, (list, dict)):
        return value

    # Everything else (int, float, str, None) is fine
    return value


def convert_row_to_doc(row):
    """
    Convert an entire Snowflake row into an Elasticsearch document.

    Input:
      row = {
        "ID": 123,
        "NAME": "Alice",
        "UPDATED_AT": datetime(...),
        ...
      }

    Output:
      {
        "ID": 123,
        "NAME": "Alice",
        "UPDATED_AT": "2026-04-29T09:45:12.123456",
        ...
      }
    """

    doc = {}

    for key, value in row.items():
        doc[key] = convert_value(value)

    return doc
