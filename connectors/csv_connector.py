import duckdb
import glob
import os
from .base_connector import BaseConnector


class CSVConnector(BaseConnector):
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.conn = None

    def connect(self):
        self.conn = duckdb.connect()
        self._register_csv_files()

    def _register_csv_files(self):
        """
        Scans data_dir for CSV files and registers each one
        as a DuckDB view named after the file. For example:
        data/orders.csv becomes queryable as SELECT * FROM orders
        """
        pattern = os.path.join(self.data_dir, "*.csv")
        csv_files = glob.glob(pattern)

        if not csv_files:
            return

        for path in csv_files:
            name = os.path.splitext(os.path.basename(path))[0]
            self.conn.execute(
                f'CREATE VIEW "{name}" AS SELECT * FROM read_csv_auto(\'{path}\')'
            )

    def query(self, sql: str) -> list[dict]:
        result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def list_tables(self) -> list[str]:
        result = self.conn.execute("SHOW TABLES")
        return [row[0] for row in result.fetchall()]

    def describe_table(self, table: str) -> list[dict]:
        result = self.conn.execute(f'DESCRIBE "{table}"')
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def close(self):
        if self.conn:
            self.conn.close()