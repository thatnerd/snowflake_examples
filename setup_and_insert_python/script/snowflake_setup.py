#!/usr/bin/env python3

"""Snowflake Resource Management Script.

This script performs operations in Snowflake based on the provided flags:
1. With --init: Creates a warehouse, a database, a schema, and a table.
2. With --cleanup: Deletes the created warehouse, database, schema, and table.

Usage:
  snowflake_setup.py [--account=<acc>] [--user=<usr>] [--password=<pwd>] [--warehouse=<wh>] [--database=<db>] [--schema=<sc>] [--table=<tb>] [--init] [--cleanup]
  snowflake_setup.py (-h | --help)

Options:
  -h --help                 Show this screen.
  --account=<acc>           Snowflake account name [default: replace_with_account_name].
  --user=<usr>              Snowflake user name [default: replace_with_username].
  --password=<pwd>          Snowflake password [default: replace_with_password].
  --warehouse=<wh>          Snowflake warehouse name [default: test_warehouse].
  --database=<db>           Snowflake database name [default: test_db].
  --schema=<sc>             Snowflake schema name [default: test_schema].
  --table=<tb>              Snowflake table name [default: test_table].
  --init                    Initialize the resources (warehouse, database, schema, table).
  --cleanup                 Clean up the resources (delete warehouse, database, schema, table).
"""

from typing import Dict
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from docopt import docopt


def create_warehouse(conn: SnowflakeConnection, warehouse_name: str) -> str:
    """Create a warehouse in Snowflake and return a success message."""
    cur = conn.cursor()
    cur.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
    cur.close()
    return f"Warehouse '{warehouse_name}' created or already present."

def create_database(conn: SnowflakeConnection, database_name: str) -> str:
    """Create a database in Snowflake and return a success message."""
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cur.close()
    return f"Database '{database_name}' created or already present."

def create_schema(conn: SnowflakeConnection, database_name: str, schema_name: str) -> str:
    """Create a schema in Snowflake and return a success message."""
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {database_name}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    cur.close()
    return f"Schema '{schema_name}' created or already present in database '{database_name}'."

def create_table(conn: SnowflakeConnection, database_name: str, schema_name: str, table_name: str) -> str:
    """Create a table in Snowflake and return a success message."""
    cur = conn.cursor()
    cur.execute(f"USE SCHEMA {database_name}.{schema_name}")
    cur.execute(f"CREATE OR REPLACE TABLE {table_name} (id INTEGER, data STRING)")
    cur.close()
    return f"Table '{table_name}' created or already present in schema '{schema_name}'."

def insert_data(conn: SnowflakeConnection, database_name: str, schema_name: str, table_name: str, warehouse_name: str) -> str:
    """Insert data into a table in Snowflake and return a success message."""
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {warehouse_name}")
    cur.execute(f"INSERT INTO {database_name}.{schema_name}.{table_name} (id, data) VALUES (1, 'Sample Data 1'), (2, 'Sample Data 2')")
    cur.close()
    return f"Data inserted into '{table_name}'."

def initialize_resources(conn: SnowflakeConnection, warehouse: str, database: str, schema: str, table: str) -> None:
    """Initialize Snowflake resources: warehouse, database, schema, and table."""
    print(create_warehouse(conn, warehouse))
    print(create_database(conn, database))
    print(create_schema(conn, database, schema))
    print(create_table(conn, database, schema, table))
    print(insert_data(conn, database, schema, table, warehouse))

def cleanup_resources(conn: SnowflakeConnection, warehouse: str, database: str, schema: str, table: str) -> str:
    """Clean up Snowflake resources: delete warehouse, database, schema, and table."""
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {database}.{schema}.{table}")
    cur.execute(f"DROP SCHEMA IF EXISTS {database}.{schema}")
    cur.execute(f"DROP DATABASE IF EXISTS {database}")
    cur.execute(f"DROP WAREHOUSE IF EXISTS {warehouse}")
    cur.close()
    return f"Clean-up completed: Table '{table}', Schema '{schema}', Database '{database}', Warehouse '{warehouse}' deleted (if present)."

def main(args: Dict[str, str]) -> None:
    # Snowflake connection credentials with default values
    ACCOUNT = args['--account']
    USER = args['--user']
    PASSWORD = args['--password']
    WAREHOUSE = args['--warehouse']
    DATABASE = args['--database']
    SCHEMA = args['--schema']
    TABLE = args['--table']

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        password=PASSWORD
    )

    try:
        if args['--init']:
            initialize_resources(conn, WAREHOUSE, DATABASE, SCHEMA, TABLE)

        # Always insert data
        print(insert_data(conn, DATABASE, SCHEMA, TABLE, WAREHOUSE))

        if args['--cleanup']:
            print(cleanup_resources(conn, WAREHOUSE, DATABASE, SCHEMA, TABLE))

    finally:
        # Close the connection
        conn.close()

if __name__ == "__main__":
    arguments = docopt(__doc__)
    main(arguments)
