"""Null and missing value detection check."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from checks.base import BaseCheck
from core.models import CheckResult, CheckStatus, Severity


class NullCheck(BaseCheck):
    """Detects null and missing values in each column.

    Reports null count, null percentage, and determines severity
    based on configurable thresholds.

    Params:
        warning_threshold: float — null % that triggers WARNING (default: 5.0)
        critical_threshold: float — null % that triggers CRITICAL (default: 20.0)
        include_empty_strings: bool — treat "" as null (default: True)
    """

    @property
    def name(self) -> str:
        return "null_check"

    @property
    def description(self) -> str:
        return "Detects null and missing values in dataset columns."

    def run(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: Optional[list[str]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> list[CheckResult]:
        params = params or {}
        warning_threshold = params.get("warning_threshold", 5.0)
        critical_threshold = params.get("critical_threshold", 20.0)
        include_empty_strings = params.get("include_empty_strings", True)

        target_columns = columns if columns else list(df.columns)
        results: list[CheckResult] = []
        total_rows = len(df)

        if total_rows == 0:
            return [self._make_result(
                table=table_name,
                column=None,
                status=CheckStatus.SKIPPED,
                severity=Severity.INFO,
                message="Table is empty — no rows to check.",
                total_rows=0,
            )]

        for col in target_columns:
            if col not in df.columns:
                continue

            series = df[col]

            # Count standard nulls (NaN, None, NaT)
            null_count = int(series.isna().sum())

            # Optionally count empty strings as nulls
            if include_empty_strings and series.dtype.kind == "O":
                empty_count = int((series == "").sum())
                null_count += empty_count
            else:
                empty_count = 0

            null_pct = (null_count / total_rows) * 100

            severity = self._determine_severity(
                null_pct, warning_threshold, critical_threshold
            )
            status = CheckStatus.PASSED if severity == Severity.PASS else CheckStatus.FAILED

            if severity == Severity.PASS:
                message = f"Column '{col}' has no missing values."
            else:
                message = (
                    f"Column '{col}' has {null_count:,} missing values "
                    f"({null_pct:.1f}% of {total_rows:,} rows)."
                )

            results.append(self._make_result(
                table=table_name,
                column=col,
                status=status,
                severity=severity,
                message=message,
                value=round(null_pct, 2),
                threshold=warning_threshold if severity == Severity.WARNING
                    else critical_threshold if severity == Severity.CRITICAL
                    else warning_threshold,
                affected_rows=null_count,
                total_rows=total_rows,
                details={
                    "null_count": null_count - empty_count,
                    "empty_string_count": empty_count,
                    "null_percentage": round(null_pct, 2),
                },
            ))

        return results
