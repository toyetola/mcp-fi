from abc import ABC, abstractmethod

class BaseConnector(ABC):
    @abstractmethod
    def connect(self): pass

    @abstractmethod
    def query(self, sql: str) -> list[dict]: pass

    @abstractmethod
    def list_tables(self) -> list[str]: pass

    @abstractmethod
    def describe_table(self, table: str) -> list[dict]: pass

    @abstractmethod
    def close(self): pass