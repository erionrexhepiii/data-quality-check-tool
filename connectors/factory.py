"""Connector factory — creates connectors by type string."""

from __future__ import annotations

from typing import Any, Optional

from connectors.base import BaseConnector
from core.exceptions import ConnectorNotFoundError
from core.models import ConnectorType


class ConnectorFactory:
    """Factory for creating and managing connector instances.

    New connectors are registered here and discovered by the UI automatically.
    """

    def __init__(self) -> None:
        self._registry: dict[str, type[BaseConnector]] = {}

    def register(self, type_key: str, connector_class: type[BaseConnector]) -> None:
        """Register a connector class under a type key."""
        self._registry[type_key] = connector_class

    def create(self, type_key: str, config: Optional[dict[str, Any]] = None) -> BaseConnector:
        """Instantiate a connector and optionally connect it.

        Args:
            type_key: The connector type identifier (e.g. "csv", "postgresql").
            config: If provided, connect() is called immediately.

        Returns:
            A connected (or unconnected) BaseConnector instance.

        Raises:
            ConnectorNotFoundError: If type_key is not registered.
        """
        if type_key not in self._registry:
            available = ", ".join(sorted(self._registry.keys()))
            raise ConnectorNotFoundError(
                f"Connector type '{type_key}' not registered. Available: {available}"
            )

        connector = self._registry[type_key]()
        if config is not None:
            connector.connect(config)
        return connector

    def available_types(self) -> list[str]:
        """Return all registered connector type keys."""
        return sorted(self._registry.keys())

    def get_display_names(self) -> dict[str, str]:
        """Return a mapping of type_key -> display_name."""
        result = {}
        for key, cls in self._registry.items():
            instance = cls()
            result[key] = instance.display_name
        return result

    def get_config_fields(self, type_key: str) -> list[dict[str, Any]]:
        """Return configuration field definitions for a connector type."""
        if type_key not in self._registry:
            raise ConnectorNotFoundError(f"Connector type '{type_key}' not registered.")
        instance = self._registry[type_key]()
        return instance.get_config_fields()

    def __contains__(self, type_key: str) -> bool:
        return type_key in self._registry


# Global factory instance
_factory: Optional[ConnectorFactory] = None


def get_factory() -> ConnectorFactory:
    """Get or create the global connector factory with built-in connectors."""
    global _factory
    if _factory is None:
        _factory = ConnectorFactory()
        _register_builtin_connectors(_factory)
    return _factory


def _register_builtin_connectors(factory: ConnectorFactory) -> None:
    """Register all built-in connector types."""
    from connectors.csv_connector import CSVConnector

    factory.register("csv", CSVConnector)
    factory.register("parquet", CSVConnector)  # CSVConnector handles both

    # Phase 2 connectors will be registered here:
    # from connectors.postgresql import PostgreSQLConnector
    # factory.register("postgresql", PostgreSQLConnector)
