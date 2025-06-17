from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd
import pyodbc


@dataclass
class AzureSQLConfig:
    server: str
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    driver: str = "{ODBC Driver 17 for SQL Server}"


class AzureSQLConnector:
    def __init__(self, config: AzureSQLConfig):
        self.config = config
        self.conn = self._create_connection()

    def _create_connection(self):
        if self.config.user and self.config.password:
            conn_str = (
                f"DRIVER={self.config.driver};"
                f"SERVER={self.config.server};"
                f"DATABASE={self.config.database};"
                f"UID={self.config.user};"
                f"PWD={self.config.password}"
            )
        else:
            conn_str = (
                f"DRIVER={self.config.driver};"
                f"SERVER={self.config.server};"
                f"DATABASE={self.config.database};"
                "Authentication=ActiveDirectoryMsi"
            )
        return pyodbc.connect(conn_str)

    def query(self, sql: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        return pd.read_sql(sql, self.conn, params=params)
