# ADLS Gen2 Delta Connector

This module provides a simple connector for reading data from Azure Data Lake Storage Gen2 that is stored in either Delta Lake or Parquet format. The connector supports authentication using Managed Identity or a Service Principal.

## Installation

Install the required dependencies:

```bash
pip install adlfs deltalake azure-identity pandas
```

## Usage

```python
from adls_delta_connector import ADLSConfig, ADLSDeltaConnector

config = ADLSConfig(
    account_name="<storage-account>",
    filesystem="<filesystem>",  # e.g. "my-container"
    path="path/to/table",  # path to the delta table or parquet file
    file_format="delta",  # or "parquet"
    connection_type="mi"  # or "spn"
)

# For service principal authentication
# config = ADLSConfig(
#     account_name="<storage-account>",
#     filesystem="<filesystem>",
#     path="path/to/table",
#     file_format="delta",
#     connection_type="spn",
#     tenant_id="<tenant>",
#     client_id="<client-id>",
#     client_secret="<secret>"
# )

connector = ADLSDeltaConnector(config)

df = connector.read()
print(df.head())
```

This will return a `pandas.DataFrame` containing the data stored in the specified Delta Lake table or Parquet file.
