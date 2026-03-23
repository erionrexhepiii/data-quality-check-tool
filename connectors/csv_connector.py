"""CSV and Parquet file connector."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from connectors.base import BaseConnector
from core.exceptions import ConnectionError
from core.models import ColumnInfo, ConnectorType


class CSVConnector(BaseConnector):
    """Connector for local CSV and Parquet files.

    Config expects:
        - file_path: str — path to the CSV or Parquet file, OR a directory of files.
        - delimiter: str — CSV delimiter (default: ",")
        - encoding: str — file encoding (default: "utf-8")
    """

    def __init__(self) -> None:
        super().__init__()
        self._file_path: Optional[Path] = None
        self._dataframes: dict[str, pd.DataFrame] = {}
        self._config: dict[str, Any] = {}

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.CSV

    @property
    def display_name(self) -> str:
        return "CSV / Parquet File"

    def connect(self, config: dict[str, Any]) -> None:
        file_path = config.get("file_path")
        if not file_path:
            raise ConnectionError("file_path is required for CSV connector.")

        path = Path(file_path)
        if not path.exists():
            raise ConnectionError(f"Path does not exist: {file_path}")

        self._file_path = path
        self._config = config
        self._dataframes = {}

        # Pre-load file(s)
        if path.is_file():
            table_name = path.stem
            self._dataframes[table_name] = self._read_file(path)
        elif path.is_dir():
            for f in path.iterdir():
                if f.suffix.lower() in (".csv", ".tsv", ".parquet", ".parq"):
                    self._dataframes[f.stem] = self._read_file(f)
            if not self._dataframes:
                raise ConnectionError(
                    f"No CSV or Parquet files found in directory: {file_path}"
                )
        else:
            raise ConnectionError(f"Path is not a file or directory: {file_path}")

        self._connected = True

    def _read_file(self, path: Path) -> pd.DataFrame:
        """Read a single file into a DataFrame."""
        suffix = path.suffix.lower()
        try:
            if suffix in (".parquet", ".parq"):
                return pd.read_parquet(path)
            else:
                return pd.read_csv(
                    path,
                    delimiter=self._config.get("delimiter", ","),
                    encoding=self._config.get("encoding", "utf-8"),
                )
        except Exception as e:
            raise ConnectionError(f"Failed to read {path.name}: {e}")

    def disconnect(self) -> None:
        self._dataframes = {}
        self._file_path = None
        self._connected = False

    def test_connection(self) -> bool:
        return self._connected and bool(self._dataframes)

    def list_schemas(self) -> list[str]:
        return ["default"]

    def list_tables(self, schema: Optional[str] = None) -> list[str]:
        return sorted(self._dataframes.keys())

    def get_columns(self, table: str, schema: Optional[str] = None) -> list[ColumnInfo]:
        df = self._get_df(table)
        return [
            ColumnInfo(
                name=col,
                dtype=str(df[col].dtype),
                nullable=df[col].isna().any(),
                position=i,
            )
            for i, col in enumerate(df.columns)
        ]

    def fetch_dataframe(
        self,
        query: str,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """For CSV connector, 'query' is treated as a table name."""
        df = self._get_df(query)
        if limit is not None:
            return df.head(limit)
        return df.copy()

    def fetch_table(
        self,
        table: str,
        schema: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        return self.fetch_dataframe(table, limit=limit)

    def get_row_count(self, table: str, schema: Optional[str] = None) -> int:
        return len(self._get_df(table))

    def _get_df(self, table: str) -> pd.DataFrame:
        if table not in self._dataframes:
            available = ", ".join(sorted(self._dataframes.keys()))
            raise ConnectionError(
                f"Table '{table}' not found. Available: {available}"
            )
        return self._dataframes[table]

    def get_config_fields(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "file_path",
                "label": "File or Directory Path",
                "type": "text",
                "required": True,
                "help": "Path to a CSV/Parquet file or a directory containing them.",
            },
            {
                "name": "delimiter",
                "label": "Delimiter",
                "type": "text",
                "required": False,
                "default": ",",
                "help": "Column delimiter for CSV files.",
            },
            {
                "name": "encoding",
                "label": "Encoding",
                "type": "text",
                "required": False,
                "default": "utf-8",
                "help": "File encoding (e.g. utf-8, latin-1).",
            },
        ]
