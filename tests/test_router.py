import pytest
import json
from unittest.mock import patch, MagicMock
from connectors.router import get_connector
from connectors.sqlite_connector import SQLiteConnector
from connectors.csv_connector import CSVConnector
from connectors.postgres_connector import PostgresConnector


# ─────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────

@pytest.fixture
def config_file(tmp_path):
    """
    Writes a real config.json to a temp directory.
    Each test gets its own isolated config file.
    """
    config = {
        "sources": {
            "my_sqlite_db": {
                "type": "sqlite",
                "path": "data/mydb.sqlite"
            },
            "my_csv_data": {
                "type": "csv",
                "data_dir": "data/"
            },
            "my_postgres": {
                "type": "postgres",
                "dsn": "postgresql://user:pass@localhost/mydb"
            }
        }
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    return str(config_path)


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

class TestRouter:

    # --- correct connector types returned ---

    def test_returns_sqlite_connector(self, config_file):
        """Should return a SQLiteConnector for sqlite type sources."""
        with patch.object(SQLiteConnector, "connect"):
            connector = get_connector("my_sqlite_db", config_file)
            assert isinstance(connector, SQLiteConnector)

    def test_returns_csv_connector(self, config_file):
        """Should return a CSVConnector for csv type sources."""
        with patch.object(CSVConnector, "connect"):
            connector = get_connector("my_csv_data", config_file)
            assert isinstance(connector, CSVConnector)

    def test_returns_postgres_connector(self, config_file):
        """Should return a PostgresConnector for postgres type sources."""
        with patch.object(PostgresConnector, "connect"):
            connector = get_connector("my_postgres", config_file)
            assert isinstance(connector, PostgresConnector)

    # --- correct config values passed to connectors ---

    def test_sqlite_connector_receives_correct_path(self, config_file):
        """SQLiteConnector should be initialized with the path from config."""
        with patch.object(SQLiteConnector, "connect"):
            connector = get_connector("my_sqlite_db", config_file)
            assert connector.db_path == "data/mydb.sqlite"

    def test_csv_connector_receives_correct_data_dir(self, config_file):
        """CSVConnector should be initialized with the data_dir from config."""
        with patch.object(CSVConnector, "connect"):
            connector = get_connector("my_csv_data", config_file)
            assert connector.data_dir == "data/"

    def test_postgres_connector_receives_correct_dsn(self, config_file):
        """PostgresConnector should be initialized with the dsn from config."""
        with patch.object(PostgresConnector, "connect"):
            connector = get_connector("my_postgres", config_file)
            assert connector.dsn == "postgresql://user:pass@localhost/mydb"

    # --- connect() is always called ---

    def test_connect_is_called_on_sqlite(self, config_file):
        """get_connector should always call connect() before returning."""
        with patch.object(SQLiteConnector, "connect") as mock_connect:
            get_connector("my_sqlite_db", config_file)
            mock_connect.assert_called_once()

    def test_connect_is_called_on_csv(self, config_file):
        with patch.object(CSVConnector, "connect") as mock_connect:
            get_connector("my_csv_data", config_file)
            mock_connect.assert_called_once()

    def test_connect_is_called_on_postgres(self, config_file):
        with patch.object(PostgresConnector, "connect") as mock_connect:
            get_connector("my_postgres", config_file)
            mock_connect.assert_called_once()

    # --- error handling ---

    def test_raises_on_unknown_source_name(self, config_file):
        """Should raise KeyError when source name is not in config."""
        with pytest.raises(KeyError, match="nonexistent_source"):
            get_connector("nonexistent_source", config_file)

    def test_raises_on_unknown_connector_type(self, tmp_path):
        """Should raise ValueError when connector type is not supported."""
        bad_config = {
            "sources": {
                "bad_source": {"type": "mongodb", "uri": "mongodb://localhost"}
            }
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(bad_config))

        with pytest.raises(ValueError, match="mongodb"):
            get_connector("bad_source", str(config_path))

    def test_error_message_lists_available_sources(self, config_file):
        """KeyError message should tell the user what sources are available."""
        with pytest.raises(KeyError) as exc_info:
            get_connector("wrong_name", config_file)
        print(exc_info)