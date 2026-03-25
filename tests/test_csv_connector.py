import pytest
import csv
from connectors.csv_connector import CSVConnector


# ─────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────

@pytest.fixture
def csv_dir(tmp_path):
    users_file = tmp_path / "users.csv"
    with open(users_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "age"])
        writer.writeheader()
        writer.writerows([
            {"id": 1, "name": "Alice",   "age": 30},
            {"id": 2, "name": "Bob",     "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ])

    products_file = tmp_path / "products.csv"
    with open(products_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "title", "price"])
        writer.writeheader()
        writer.writerows([
            {"id": 1, "title": "Laptop", "price": 999.99},
            {"id": 2, "title": "Phone",  "price": 499.99},
        ])

    yield str(tmp_path)


@pytest.fixture
def connected_csv(csv_dir):
    connector = CSVConnector(csv_dir)
    connector.connect()
    yield connector
    connector.close()


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

class TestCSVConnector:

    # --- Initialization ---

    def test_connector_initializes_with_path(self, csv_dir):
        connector = CSVConnector(csv_dir)
        assert connector.data_dir == csv_dir
        assert connector.conn is None

    def test_connect_opens_connection(self, csv_dir):
        connector = CSVConnector(csv_dir)
        connector.connect()
        assert connector.conn is not None
        connector.close()

    def test_close_is_safe_without_connect(self, csv_dir):
        connector = CSVConnector(csv_dir)
        connector.close()

    # --- list_tables ---

    def test_list_tables_returns_all_csv_files(self, connected_csv):
        tables = connected_csv.list_tables()
        assert set(tables) == {"users", "products"}

    def test_list_tables_returns_list(self, connected_csv):
        tables = connected_csv.list_tables()
        assert isinstance(tables, list)

    def test_empty_directory_has_no_tables(self, tmp_path):
        connector = CSVConnector(str(tmp_path))
        connector.connect()
        tables = connector.list_tables()
        assert tables == []
        connector.close()

    # --- describe_table ---

    def test_describe_table_returns_columns(self, connected_csv):
        schema = connected_csv.describe_table("users")
        column_names = [col["column_name"] for col in schema]
        assert "id" in column_names
        assert "name" in column_names
        assert "age" in column_names

    def test_describe_table_returns_list_of_dicts(self, connected_csv):
        schema = connected_csv.describe_table("users")
        assert isinstance(schema, list)
        assert all(isinstance(col, dict) for col in schema)

    # --- query ---

    def test_query_returns_all_rows(self, connected_csv):
        results = connected_csv.query("SELECT * FROM users")
        assert len(results) == 3

    def test_query_returns_list_of_dicts(self, connected_csv):
        results = connected_csv.query("SELECT * FROM users")
        assert isinstance(results, list)
        assert all(isinstance(row, dict) for row in results)

    def test_query_returns_correct_data(self, connected_csv):
        results = connected_csv.query(
            "SELECT * FROM users WHERE name = 'Alice'"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 30

    def test_query_with_filter(self, connected_csv):
        results = connected_csv.query(
            "SELECT * FROM users WHERE age > 28"
        )
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Charlie" in names
        assert "Bob" not in names

    def test_query_empty_result(self, connected_csv):
        results = connected_csv.query(
            "SELECT * FROM users WHERE name = 'Nobody'"
        )
        assert results == []

    def test_query_invalid_sql_raises(self, connected_csv):
        with pytest.raises(Exception):
            connected_csv.query("THIS IS NOT SQL")

    def test_query_across_tables(self, connected_csv):
        results = connected_csv.query("""
            SELECT u.name, u.age
            FROM users u
            WHERE u.id IN (SELECT id FROM products)
        """)
        assert isinstance(results, list)