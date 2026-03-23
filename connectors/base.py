"""Abstract base class for all database/file connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

import pandas as pd

from core.models import ColumnInfo, ConnectorType


class BaseConnector(ABC):
    """Abstract interface that all DQC connectors must implement.

    To add a new data source:
    1. Create a new file in connectors/ inheriting from BaseConnector.
    2. Implement all abstract methods.
    3. Register it in connectors/factory.py.
    """

    def __init__(self) -> None:
        self._connected = False

    @property
    @abstractmethod
    def connector_type(self) -> ConnectorType:
        """Return the ConnectorType enum for this connector."""
        ...

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name shown in the UI."""
        ...

    @property
    def is_connected(self) -> bool:
        return self._connected

    @abstractmethod
    def connect(self, config: dict[str, Any]) -> None:
        """Establish connection using the provided configuration.

        Raises:
            core.exceptions.ConnectionError: If the connection fails.
        """
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection and release resources."""
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        """Verify the connection is alive. Returns True on success."""
        ...

    @abstractmethod
    def list_schemas(self) -> list[str]:
        """Return available schemas/databases."""
        ...

    @abstractmethod
    def list_tables(self, schema: Optional[str] = None) -> list[str]:
        """Return table names within the given schema."""
        ...

    @abstractmethod
    def get_columns(self, table: str, schema: Optional[str] = None) -> list[ColumnInfo]:
        """Return column metadata for the specified table."""
        ...

    @abstractmethod
    def fetch_dataframe(
        self,
        query: str,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Execute a query and return results as a DataFrame.

        Args:
            query: SQL query string or table name (connector-dependent).
            limit: Maximum number of rows to return.
        """
        ...

    @abstractmethod
    def get_row_count(self, table: str, schema: Optional[str] = None) -> int:
        """Return the total row count for a table."""
        ...

    def fetch_table(
        self,
        table: str,
        schema: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Convenience: fetch an entire table as a DataFrame.

        Default implementation builds a SELECT query. Override if the
        connector has a more efficient path (e.g. file reads).
        """
        qualified = f"{schema}.{table}" if schema else table
        query = f"SELECT * FROM {qualified}"
        return self.fetch_dataframe(query, limit=limit)

    def get_config_fields(self) -> list[dict[str, Any]]:
        """Return a list of configuration field definitions for the UI.

        Each field dict should contain:
            - name: str - field identifier
            - label: str - display label
            - type: str - "text", "password", "number", "file"
            - required: bool
            - default: Any (optional)
            - help: str (optional)
        """
        return []
