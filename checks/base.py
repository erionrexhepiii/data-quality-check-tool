"""Abstract base class for all data quality checks."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

import pandas as pd

from core.models import CheckResult, CheckStatus, Severity


class BaseCheck(ABC):
    """Base class that all DQC checks must inherit from.

    Subclasses must implement:
        - name (property): unique identifier for the check type
        - description (property): human-readable description
        - run(): execute the check and return results
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for this check type (e.g. 'null_check')."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what this check does."""
        ...

    @property
    def supported_dtypes(self) -> Optional[list[str]]:
        """Return list of pandas dtype kinds this check applies to.

        None means the check applies to all dtypes.
        Common kinds: 'i' (int), 'f' (float), 'O' (object/string),
                      'M' (datetime), 'b' (bool)
        """
        return None

    def applies_to_column(self, series: pd.Series) -> bool:
        """Check if this check is applicable to the given column."""
        if self.supported_dtypes is None:
            return True
        return series.dtype.kind in self.supported_dtypes

    @abstractmethod
    def run(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: Optional[list[str]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> list[CheckResult]:
        """Execute the check against the given DataFrame.

        Args:
            df: The data to check.
            table_name: Name of the table/dataset being checked.
            columns: Specific columns to check. None means all applicable columns.
            params: Check-specific parameters (thresholds, etc.).

        Returns:
            List of CheckResult objects, one per column or one for the table.
        """
        ...

    def _determine_severity(
        self,
        value: float,
        warning_threshold: float,
        critical_threshold: float,
    ) -> Severity:
        """Determine severity based on value and thresholds.

        Assumes higher values are worse (e.g. null percentage).
        """
        if value >= critical_threshold:
            return Severity.CRITICAL
        elif value >= warning_threshold:
            return Severity.WARNING
        elif value > 0:
            return Severity.INFO
        return Severity.PASS

    def _make_result(
        self,
        table: str,
        column: Optional[str],
        status: CheckStatus,
        severity: Severity,
        message: str,
        value: Optional[float] = None,
        threshold: Optional[float] = None,
        affected_rows: Optional[int] = None,
        total_rows: Optional[int] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> CheckResult:
        """Helper to create a CheckResult with this check's metadata."""
        return CheckResult(
            check_name=self.name,
            check_type=self.name,
            table=table,
            column=column,
            status=status,
            severity=severity,
            message=message,
            value=value,
            threshold=threshold,
            affected_rows=affected_rows,
            total_rows=total_rows,
            details=details or {},
        )
