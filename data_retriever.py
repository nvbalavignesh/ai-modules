from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd

from azure_sql_connector import AzureSQLConfig, AzureSQLConnector
from adls_delta_connector import ADLSConfig, ADLSDeltaConnector


@dataclass
class RetrieverConfig:
    """Configuration for the DataRetriever."""

    source_type: str  # "sql" or "adls"
    sql_config: Optional[AzureSQLConfig] = None
    adls_config: Optional[ADLSConfig] = None


class DataRetriever:
    """Retrieve data from SQL or ADLS based on configuration."""

    def __init__(self, config: RetrieverConfig):
        self.config = config
        stype = config.source_type.lower()
        if stype == "sql":
            if not config.sql_config:
                raise ValueError("sql_config must be provided when source_type is 'sql'")
            self.client = AzureSQLConnector(config.sql_config)
        elif stype == "adls":
            if not config.adls_config:
                raise ValueError("adls_config must be provided when source_type is 'adls'")
            self.client = ADLSDeltaConnector(config.adls_config)
        else:
            raise ValueError(f"Unsupported source_type: {config.source_type}")

    def retrieve(self, *args, **kwargs) -> pd.DataFrame:
        """Retrieve data using the underlying connector."""
        if isinstance(self.client, AzureSQLConnector):
            return self.client.query(*args, **kwargs)
        else:
            return self.client.read()
