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

# Azure SQL Connector

This connector allows querying data from Azure SQL Database using `pyodbc`.

## Installation

Install the required dependencies:

```bash
pip install pyodbc pandas
```

## Usage

```python
from azure_sql_connector import AzureSQLConfig, AzureSQLConnector

config = AzureSQLConfig(
    server="<server>.database.windows.net",
    database="<database>",
    user="<username>",  # omit for Managed Identity
    password="<password>",  # omit for Managed Identity
)

connector = AzureSQLConnector(config)

df = connector.query("SELECT * FROM mytable")
print(df.head())
```

# Data Retriever

The `DataRetriever` class provides a single interface for retrieving data from either ADLS or Azure SQL based on a configuration.

```python
from data_retriever import DataRetriever, RetrieverConfig
from adls_delta_connector import ADLSConfig
from azure_sql_connector import AzureSQLConfig

# Example for ADLS
adls_cfg = ADLSConfig(
    account_name="<storage-account>",
    filesystem="<filesystem>",
    path="path/to/table",
)
retriever = DataRetriever(RetrieverConfig(source_type="adls", adls_config=adls_cfg))

adls_df = retriever.retrieve()

# Example for Azure SQL
sql_cfg = AzureSQLConfig(
    server="<server>.database.windows.net",
    database="<database>",
)
retriever = DataRetriever(RetrieverConfig(source_type="sql", sql_config=sql_cfg))

sql_df = retriever.retrieve("SELECT * FROM mytable")
```

This will return a `pandas.DataFrame` with the results from the chosen source.

