import mysql.connector
import mysql.connector.cursor
from .base_connector import BaseConnector


class MySQLConnector(BaseConnector):
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def connect(self):
        self.conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def query(self, sql: str) -> list[dict]:
        with self.conn.cursor(dictionary=True) as cur:
            cur.execute(sql)
            return cur.fetchall()

    def list_tables(self) -> list[str]:
        rows = self.query("SHOW TABLES")
        # SHOW TABLES returns a dict with one key like {"Tables_in_mydb": "users"}
        return [list(row.values())[0] for row in rows]

    def describe_table(self, table: str) -> list[dict]:
        return self.query(f"DESCRIBE {table}")

    def close(self):
        if self.conn:
            self.conn.close()