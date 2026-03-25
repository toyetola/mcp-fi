import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from connectors.postgres_connector import PostgresConnector

# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def make_mock_connection(rows=None, columns=None):
    """
    Builds a fake psycopg2 connection that returns
    whatever rows and columns you give it.

    This mimics the psycopg2 connection → cursor → execute → fetchall chain.
    """
    rows = rows or []
    columns = columns or []

    # Fake cursor
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    # description is what psycopg2 uses to expose column names
    # each entry is a tuple where index 0 is the column name
    mock_cursor.description = [(col,) for col in columns]
    # Make the cursor work as a context manager (with conn.cursor() as cur)
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    # Fake connection
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    return mock_conn, mock_cursor

# ─────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────

@pytest.fixture
def mock_conn():
    """
    Patches psycopg2.connect for the entire test.
    The connector never touches a real Postgres server.
    """
    with patch("psycopg2.connect") as mock_connect:
        mock_connection, mock_cursor = make_mock_connection()
        mock_connect.return_value = mock_connection
        yield mock_connect, mock_connection, mock_cursor


@pytest.fixture
def connected_postgres(mock_conn):
    """
    Gives a PostgresConnector that is already connected
    via the mocked psycopg2.connect.
    """
    mock_connect, mock_connection, mock_cursor = mock_conn
    connector = PostgresConnector("postgresql://fake:fake@localhost/fakedb")
    connector.connect()
    return connector, mock_connection, mock_cursor


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

class TestPostgresConnector:

    # --- Initialization ---

    def test_connector_initializes_with_dsn(self):
        """Connector stores dsn and starts with no connection."""
        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        assert connector.dsn == "postgresql://user:pass@localhost/db"
        assert connector.conn is None

    def test_close_is_safe_without_connect(self):
        """close() should not crash if connect() was never called."""
        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.close()

    # --- connect ---

    def test_connect_calls_psycopg2(self, mock_conn):
        """connect() should call psycopg2.connect with the dsn."""
        mock_connect, mock_connection, _ = mock_conn
        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        mock_connect.assert_called_once_with("postgresql://user:pass@localhost/db")

    def test_connect_stores_connection(self, mock_conn):
        """After connect(), conn should no longer be None."""
        mock_connect, mock_connection, _ = mock_conn
        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        assert connector.conn is not None

    # --- close ---

    def test_close_calls_connection_close(self, mock_conn):
        """close() should call .close() on the underlying connection."""
        mock_connect, mock_connection, _ = mock_conn
        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        connector.close()
        mock_connection.close.assert_called_once()

    # --- query ---

    def test_query_returns_list_of_dicts(self, mock_conn):
        """query() should return results as a list of dicts."""
        mock_connect, mock_connection, mock_cursor = mock_conn

        # Configure the fake cursor to return fake rows
        fake_rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_cursor.fetchall.return_value = fake_rows
        mock_cursor.__enter__.return_value = mock_cursor

        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        results = connector.query("SELECT * FROM users")

        assert isinstance(results, list)
        assert all(isinstance(row, dict) for row in results)

    def test_query_executes_sql(self, mock_conn):
        """query() should call cursor.execute() with the provided SQL."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__.return_value = mock_cursor

        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        connector.query("SELECT * FROM users")

        mock_cursor.execute.assert_called_once_with("SELECT * FROM users")

    def test_query_returns_empty_list_on_no_results(self, mock_conn):
        """query() should return [] when there are no matching rows."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__.return_value = mock_cursor

        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        results = connector.query("SELECT * FROM users WHERE name = 'Nobody'")

        assert results == []

    # --- list_tables ---

    def test_list_tables_returns_table_names(self, mock_conn):
        """list_tables() should return a flat list of table name strings."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = [
            {"table_name": "users"},
            {"table_name": "products"}
        ]
        mock_cursor.__enter__.return_value = mock_cursor

        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        tables = connector.list_tables()

        assert set(tables) == {"users", "products"}

    def test_list_tables_returns_list(self, mock_conn):
        """Return type should always be a list."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__.return_value = mock_cursor

        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        tables = connector.list_tables()

        assert isinstance(tables, list)

    # --- describe_table ---

    def test_describe_table_returns_column_metadata(self, mock_conn):
        """describe_table() should return column names and types."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = [
            {"column_name": "id",   "data_type": "integer"},
            {"column_name": "name", "data_type": "text"},
        ]
        mock_cursor.__enter__.return_value = mock_cursor

        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        schema = connector.describe_table("users")

        column_names = [col["column_name"] for col in schema]
        assert "id" in column_names
        assert "name" in column_names

    def test_describe_table_returns_list_of_dicts(self, mock_conn):
        """Each row in the schema should be a dict."""
        mock_connect, mock_connection, mock_cursor = mock_conn
        mock_cursor.fetchall.return_value = [
            {"column_name": "id", "data_type": "integer"}
        ]
        mock_cursor.__enter__.return_value = mock_cursor

        connector = PostgresConnector("postgresql://user:pass@localhost/db")
        connector.connect()
        schema = connector.describe_table("users")

        assert isinstance(schema, list)
        assert all(isinstance(col, dict) for col in schema)