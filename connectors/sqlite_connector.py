import sqlite3
from .base_connector import BaseConnector

class SQLiteConnector(BaseConnector):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def query(self, sql: str) -> list[dict]:
        cursor = self.conn.execute(sql)
        return [dict(row) for row in cursor.fetchall()]

    def list_tables(self) -> list[str]:
        rows = self.query("SELECT name FROM sqlite_master WHERE type='table'")
        return [r["name"] for r in rows]

    def describe_table(self, table: str) -> list[dict]:
        return self.query(f"PRAGMA table_info({table})")

    def close(self):
        if self.conn:
            self.conn.close()