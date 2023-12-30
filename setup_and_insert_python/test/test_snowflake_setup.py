import unittest
from unittest.mock import patch, MagicMock
import script.snowflake_setup as snowflake_setup_script

class TestSnowflakeSetup(unittest.TestCase):

    def setUp(self):
        # Mock the Snowflake connector
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

    def test_create_warehouse(self):
        warehouse_name = 'test_warehouse'
        snowflake_setup_script.create_warehouse(self.mock_conn, warehouse_name)
        self.mock_cursor.execute.assert_called_once_with(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")

    def test_create_database(self):
        database_name = 'test_database'
        snowflake_setup_script.create_database(self.mock_conn, database_name)
        self.mock_cursor.execute.assert_called_once_with(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    def test_create_schema(self):
        database_name = 'test_database'
        schema_name = 'test_schema'
        snowflake_setup_script.create_schema(self.mock_conn, database_name, schema_name)
        self.mock_cursor.execute.assert_called_with(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def test_create_table(self):
        schema_name = 'test_schema'
        database_name = 'test_database'
        table_name = 'test_table'
        snowflake_setup_script.create_table(self.mock_conn, database_name, schema_name, table_name)
        self.mock_cursor.execute.assert_called_with(f"CREATE OR REPLACE TABLE {table_name} (id INTEGER, data STRING)")

    def test_insert_data(self):
        database_name = 'test_database'
        schema_name = 'test_schema'
        table_name = 'test_table'
        warehouse_name = 'test_warehouse'
        snowflake_setup_script.insert_data(self.mock_conn, database_name, schema_name, table_name, warehouse_name)
        # Add assert calls based on actual implementation

    def test_cleanup_resources(self):
        warehouse_name = 'test_warehouse'
        database_name = 'test_database'
        schema_name = 'test_schema'
        table_name = 'test_table'
        snowflake_setup_script.cleanup_resources(self.mock_conn, warehouse_name, database_name, schema_name, table_name)
        calls = [
            unittest.mock.call(f"DROP TABLE IF EXISTS {database_name}.{schema_name}.{table_name}"),
            unittest.mock.call(f"DROP SCHEMA IF EXISTS {database_name}.{schema_name}"),
            unittest.mock.call(f"DROP DATABASE IF EXISTS {database_name}"),
            unittest.mock.call(f"DROP WAREHOUSE IF EXISTS {warehouse_name}")
        ]
        self.mock_cursor.execute.assert_has_calls(calls, any_order=False)

if __name__ == '__main__':
    unittest.main()
