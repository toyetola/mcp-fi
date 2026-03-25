import pytest
import sqlite3
import os
from connectors.sqlite_connector import SQLiteConnector

# ─────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────

@pytest.fixture
def sqlite_db(tmp_path):
    """
    Creates a temporary SQLite database with seed data for each test.
    tmp_path is a pytest built-in fixture that gives a fresh temp 
    directory for every test — no leftover files between runs.
    """
    db_path = tmp_path / "test.sqlite"
    
    # Seed the database directly using sqlite3
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            price REAL
        )
    """)
    conn.executemany(
        "INSERT INTO users (name, age) VALUES (?, ?)",
        [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    )
    conn.executemany(
        "INSERT INTO products (title, price) VALUES (?, ?)",
        [("Laptop", 999.99), ("Phone", 499.99)]
    )
    conn.commit()
    conn.close()

    # Yield the path to the test, then cleanup is automatic via tmp_path
    yield str(db_path)


@pytest.fixture
def connected_sqlite(sqlite_db):
    """
    Builds on sqlite_db fixture — gives a connected SQLiteConnector
    and ensures it's always closed after each test, even on failure.
    """
    connector = SQLiteConnector(sqlite_db)
    connector.connect()
    yield connector
    connector.close()


# ─────────────────────────────────────────
# SQLiteConnector Tests
# ─────────────────────────────────────────

class TestSQLiteConnector:

    # --- Initialization ---

    def test_connector_initializes_with_path(self, sqlite_db):
        """Connector stores db_path and starts with no connection."""
        connector = SQLiteConnector(sqlite_db)
        assert connector.db_path == sqlite_db
        assert connector.conn is None

    def test_connect_opens_connection(self, sqlite_db):
        """After connect(), conn should no longer be None."""
        connector = SQLiteConnector(sqlite_db)
        connector.connect()
        assert connector.conn is not None
        connector.close()

    def test_close_is_safe_without_connect(self, sqlite_db):
        """close() should not crash if connect() was never called."""
        connector = SQLiteConnector(sqlite_db)
        connector.close()  # Should not raise

    # --- list_tables ---

    def test_list_tables_returns_all_tables(self, connected_sqlite):
        """Should return all table names in the database."""
        tables = connected_sqlite.list_tables()
        assert set(tables) == {"users", "products"}

    def test_list_tables_returns_list(self, connected_sqlite):
        """Return type should always be a list."""
        tables = connected_sqlite.list_tables()
        assert isinstance(tables, list)

    # --- describe_table ---

    def test_describe_table_returns_columns(self, connected_sqlite):
        """Should return column metadata for the given table."""
        schema = connected_sqlite.describe_table("users")
        column_names = [col["name"] for col in schema]
        assert "id" in column_names
        assert "name" in column_names
        assert "age" in column_names

    def test_describe_table_returns_list_of_dicts(self, connected_sqlite):
        """Each row in the schema should be a dict."""
        schema = connected_sqlite.describe_table("users")
        assert isinstance(schema, list)
        assert all(isinstance(col, dict) for col in schema)

    # --- query ---

    def test_query_returns_all_rows(self, connected_sqlite):
        """SELECT * should return all seeded rows."""
        results = connected_sqlite.query("SELECT * FROM users")
        assert len(results) == 3

    def test_query_returns_list_of_dicts(self, connected_sqlite):
        """Each row should be a plain Python dict."""
        results = connected_sqlite.query("SELECT * FROM users")
        assert isinstance(results, list)
        assert all(isinstance(row, dict) for row in results)

    def test_query_returns_correct_data(self, connected_sqlite):
        """Data returned should match what was seeded."""
        results = connected_sqlite.query(
            "SELECT * FROM users WHERE name = 'Alice'"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 30

    def test_query_with_filter(self, connected_sqlite):
        """WHERE clause should correctly filter rows."""
        results = connected_sqlite.query(
            "SELECT * FROM users WHERE age > 28"
        )
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Charlie" in names
        assert "Bob" not in names

    def test_query_empty_result(self, connected_sqlite):
        """Query with no matches should return an empty list, not crash."""
        results = connected_sqlite.query(
            "SELECT * FROM users WHERE name = 'Nobody'"
        )
        assert results == []

    def test_query_invalid_sql_raises(self, connected_sqlite):
        """Malformed SQL should raise an exception."""
        with pytest.raises(Exception):
            connected_sqlite.query("THIS IS NOT SQL")