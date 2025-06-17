from dataclasses import dataclass
from typing import Optional

import pandas as pd
from azure.identity import ManagedIdentityCredential, ClientSecretCredential
import adlfs
try:
    from deltalake import DeltaTable
except ImportError:  # optional dependency
    DeltaTable = None


@dataclass
class ADLSConfig:
    account_name: str
    filesystem: str
    path: str
    file_format: str = "delta"  # 'delta' or 'parquet'
    connection_type: str = "mi"  # 'mi' or 'spn'
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class ADLSDeltaConnector:
    def __init__(self, config: ADLSConfig):
        self.config = config
        self.fs = self._create_filesystem()

    def _create_filesystem(self):
        if self.config.connection_type.lower() == "mi":
            credential = ManagedIdentityCredential()
        elif self.config.connection_type.lower() == "spn":
            if not all([self.config.tenant_id, self.config.client_id, self.config.client_secret]):
                raise ValueError("SPN requires tenant_id, client_id, and client_secret")
            credential = ClientSecretCredential(
                tenant_id=self.config.tenant_id,
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
            )
        else:
            raise ValueError(f"Unsupported connection type: {self.config.connection_type}")

        return adlfs.AzureBlobFileSystem(account_name=self.config.account_name, credential=credential)

    def read(self) -> pd.DataFrame:
        abfs_path = f"abfs://{self.config.filesystem}/{self.config.path}"
        file_format = self.config.file_format.lower()

        if file_format == "delta":
            if DeltaTable is None:
                raise ImportError("deltalake package is required for reading delta format")
            dt = DeltaTable(abfs_path, filesystem=self.fs)
            return dt.to_pandas()
        elif file_format == "parquet":
            with self.fs.open(abfs_path, "rb") as f:
                return pd.read_parquet(f)
        else:
            raise ValueError(f"Unsupported file format: {self.config.file_format}")
