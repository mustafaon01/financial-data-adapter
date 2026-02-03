"""Clickhouse connection conf"""

import clickhouse_connect
import logging
import os

logger = logging.getLogger(__name__)


class ClickHouseClient:
    """
    Client for managing ClickHouse database connections and operations.

    :param tenant_schema: The schema name of the tenant (used as database name).
    """

    def __init__(self, tenant_schema=None):
        self.host = os.getenv("CH_HOST", "clickhouse")
        self.user = os.getenv("CH_USER", "default")
        self.password = os.getenv("CH_PWD", "")
        self.port = 8123
        self.database = self.get_db_name(tenant_schema) if tenant_schema else "default"

    def get_db_name(self, tenant_schema):
        """
        Get the database name from tenant schema.

        :param tenant_schema: The schema name from Postgres.
        :return: The matching ClickHouse database name.
        """
        return f"dwh_{tenant_schema.lower()}"

    def get_client(self):
        """
        Create and return a new ClickHouse client instance.

        :return: A ClickHouse client object.
        """
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database,
        )

    def create_database(self, tenant_schema):
        """
        Create a new database for a tenant if it does not exist.

        :param tenant_schema: The name of the database to create.
        """
        db_name = self.get_db_name(tenant_schema)
        client = clickhouse_connect.get_client(
            host=self.host, port=self.port, username=self.user, password=self.password
        )
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            logger.info(f"Created ClickHouse database: {db_name}")
        except Exception as e:
            logger.error(f"Failed to create ClickHouse database {db_name}: {e}")
            raise

    def execute_query(self, query, params=None):
        """
        Execute a SQL query.

        :param query: The SQL query string.
        :param params: Optional parameters for the query.
        :return: The result of the query.
        """
        client = self.get_client()
        return client.query(query, params)

    def insert_data(self, table, data, column_names=None):
        """
        Insert data into a table.

        :param table: Target table name.
        :param data: List of data rows to insert.
        :param column_names: List of column names.
        """
        client = self.get_client()
        client.insert(table, data, column_names=column_names)

    def swap_tables(self, table_main, table_staging):
        """
        Replace the main table with the staging table.

        :param table_main: The name of the main table.
        :param table_staging: The name of the staging table.
        """
        client = self.get_client()
        try:
            exists = client.command(f"EXISTS TABLE {table_main}")
            if not exists:
                client.command(f"RENAME TABLE {table_staging} TO {table_main}")
            else:
                client.command(f"EXCHANGE TABLES {table_main} AND {table_staging}")
            logger.info(f"Swapped tables {table_main} <-> {table_staging}")
        except Exception as e:
            logger.error(f"Failed to swap tables: {e}")
            raise
