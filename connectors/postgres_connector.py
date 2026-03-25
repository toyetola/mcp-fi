import psycopg2
import psycopg2.extras
from .base_connector import BaseConnector


class PostgresConnector(BaseConnector):
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.dsn)

    def query(self, sql: str) -> list[dict]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]

    def list_tables(self) -> list[str]:
        rows = self.query("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        return [r["table_name"] for r in rows]

    def describe_table(self, table: str) -> list[dict]:
        return self.query(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
        """)

    def close(self):
        if self.conn:
            self.conn.close()