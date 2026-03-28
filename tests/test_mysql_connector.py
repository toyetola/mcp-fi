import pytest
from unittest.mock import MagicMock, patch
from connectors.mysql_connector import MySQLConnector


# ─────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────

@pytest.fixture
def mock_conn():
    with patch("mysql.connector.connect") as mock_connect:
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        yield mock_connect, mock_connection, mock_cursor


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

class TestMySQLConnector:

    # --- Initialization ---

    def test_connector_initializes_with_credentials(self):
        """Connector stores credentials and starts with no connection."""
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        assert connector.host == "localhost"
        assert connector.port == 3306
        assert connector.database == "mydb"
        assert connector.user == "root"
        assert connector.password == "secret"
        assert connector.conn is None

    def test_close_is_safe_without_connect(self):
        """close() should not crash if connect() was never called."""
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.close()

    # --- connect ---

    def test_connect_calls_mysql_connector(self, mock_conn):
        """connect() should call mysql.connector.connect with credentials."""
        mock_connect, mock_connection, _ = mock_conn
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        mock_connect.assert_called_once_with(
            host="localhost",
            port=3306,
            database="mydb",
            user="root",
            password="secret"
        )

    def test_connect_stores_connection(self, mock_conn):
        """After connect(), conn should no longer be None."""
        mock_connect, mock_connection, _ = mock_conn
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        assert connector.conn is not None

    # --- close ---

    def test_close_calls_connection_close(self, mock_conn):
        """close() should call .close() on the underlying connection."""
        mock_connect, mock_connection, _ = mock_conn
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        connector.close()
        mock_connection.close.assert_called_once()

    # --- query ---

    def test_query_returns_list_of_dicts(self, mock_conn):
        """query() should return results as a list of dicts."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        mock_cursor.__enter__.return_value = mock_cursor
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        results = connector.query("SELECT * FROM users")
        assert isinstance(results, list)
        assert all(isinstance(row, dict) for row in results)

    def test_query_executes_sql(self, mock_conn):
        """query() should call cursor.execute() with the provided SQL."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__.return_value = mock_cursor
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        connector.query("SELECT * FROM users")
        mock_cursor.execute.assert_called_once_with("SELECT * FROM users")

    def test_query_returns_empty_list_on_no_results(self, mock_conn):
        """query() should return [] when there are no matching rows."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__.return_value = mock_cursor
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        results = connector.query("SELECT * FROM users WHERE name = 'Nobody'")
        assert results == []

    # --- list_tables ---

    def test_list_tables_returns_table_names(self, mock_conn):
        """list_tables() should return a flat list of table name strings."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = [
            {"Tables_in_mydb": "users"},
            {"Tables_in_mydb": "products"}
        ]
        mock_cursor.__enter__.return_value = mock_cursor
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        tables = connector.list_tables()
        assert set(tables) == {"users", "products"}

    def test_list_tables_returns_list(self, mock_conn):
        """Return type should always be a list."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__.return_value = mock_cursor
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        tables = connector.list_tables()
        assert isinstance(tables, list)

    # --- describe_table ---

    def test_describe_table_returns_column_metadata(self, mock_conn):
        """describe_table() should return column names and types."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = [
            {"Field": "id",   "Type": "int"},
            {"Field": "name", "Type": "varchar(255)"},
        ]
        mock_cursor.__enter__.return_value = mock_cursor
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        schema = connector.describe_table("users")
        assert isinstance(schema, list)
        assert all(isinstance(col, dict) for col in schema)

    def test_describe_table_returns_list_of_dicts(self, mock_conn):
        """Each row in the schema should be a dict."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = [
            {"Field": "id", "Type": "int"}
        ]
        mock_cursor.__enter__.return_value = mock_cursor
        connector = MySQLConnector(
            host="localhost", port=3306,
            database="mydb", user="root", password="secret"
        )
        connector.connect()
        schema = connector.describe_table("users")
        assert isinstance(schema, list)
        assert all(isinstance(col, dict) for col in schema)