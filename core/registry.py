"""Check registry — discovers and catalogs all available checks."""

from __future__ import annotations

from typing import Optional

from checks.base import BaseCheck
from core.exceptions import CheckNotFoundError


class CheckRegistry:
    """Central registry for all data quality check types.

    Checks register themselves here so the engine can discover them by name.
    """

    def __init__(self) -> None:
        self._checks: dict[str, BaseCheck] = {}

    def register(self, check: BaseCheck) -> None:
        """Register a check instance."""
        self._checks[check.name] = check

    def get(self, name: str) -> BaseCheck:
        """Retrieve a registered check by name."""
        if name not in self._checks:
            available = ", ".join(sorted(self._checks.keys()))
            raise CheckNotFoundError(
                f"Check '{name}' not found. Available: {available}"
            )
        return self._checks[name]

    def list_checks(self) -> list[BaseCheck]:
        """Return all registered checks."""
        return list(self._checks.values())

    def list_names(self) -> list[str]:
        """Return names of all registered checks."""
        return sorted(self._checks.keys())

    def __contains__(self, name: str) -> bool:
        return name in self._checks

    def __len__(self) -> int:
        return len(self._checks)


# Global registry instance
_registry: Optional[CheckRegistry] = None


def get_registry() -> CheckRegistry:
    """Get or create the global check registry, auto-registering all built-in checks."""
    global _registry
    if _registry is None:
        _registry = CheckRegistry()
        _register_builtin_checks(_registry)
    return _registry


def _register_builtin_checks(registry: CheckRegistry) -> None:
    """Import and register all built-in check classes."""
    from checks.nulls import NullCheck
    from checks.duplicates import DuplicateCheck
    from checks.range_checks import RangeCheck
    from checks.format_validation import FormatValidationCheck

    registry.register(NullCheck())
    registry.register(DuplicateCheck())
    registry.register(RangeCheck())
    registry.register(FormatValidationCheck())
