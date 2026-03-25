from .sqlite_connector import SQLiteConnector
from .csv_connector import CSVConnector
from .postgres_connector import PostgresConnector
import json
import os

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_CONFIG = os.path.join(_PROJECT_ROOT, "config.json")

def get_connector(source_name: str, config_path=_DEFAULT_CONFIG):
    with open(config_path) as f:
        config = json.load(f)
    source = config["sources"][source_name]

    if source["type"] == "sqlite":
        c = SQLiteConnector(source["path"])
    elif source["type"] == "csv":
        c = CSVConnector(source["data_dir"])
    elif source["type"] == "postgres":
        c = PostgresConnector(source["dsn"])
    else:
        raise ValueError(f"Unknown source type: {source['type']}")

    c.connect()
    return c