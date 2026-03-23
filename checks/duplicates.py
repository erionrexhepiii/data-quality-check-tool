"""Duplicate row detection check."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from checks.base import BaseCheck
from core.models import CheckResult, CheckStatus, Severity


class DuplicateCheck(BaseCheck):
    """Detects duplicate rows in the dataset.

    Can check for:
    - Full row duplicates (all columns match)
    - Key column duplicates (specified columns match)

    Params:
        warning_threshold: float — duplicate % that triggers WARNING (default: 1.0)
        critical_threshold: float — duplicate % that triggers CRITICAL (default: 5.0)
        key_columns: list[str] — columns that define uniqueness. If not provided,
                     uses `columns` param; if that's also empty, checks all columns.
    """

    @property
    def name(self) -> str:
        return "duplicate_check"

    @property
    def description(self) -> str:
        return "Detects duplicate rows based on all or selected key columns."

    def run(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: Optional[list[str]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> list[CheckResult]:
        params = params or {}
        warning_threshold = params.get("warning_threshold", 1.0)
        critical_threshold = params.get("critical_threshold", 5.0)
        key_columns = params.get("key_columns", columns)

        total_rows = len(df)
        results: list[CheckResult] = []

        if total_rows == 0:
            return [self._make_result(
                table=table_name,
                column=None,
                status=CheckStatus.SKIPPED,
                severity=Severity.INFO,
                message="Table is empty — no rows to check.",
                total_rows=0,
            )]

        # Full-row duplicate check
        full_dup_mask = df.duplicated(keep="first")
        full_dup_count = int(full_dup_mask.sum())
        full_dup_pct = (full_dup_count / total_rows) * 100

        severity = self._determine_severity(
            full_dup_pct, warning_threshold, critical_threshold
        )
        status = CheckStatus.PASSED if severity == Severity.PASS else CheckStatus.FAILED

        if severity == Severity.PASS:
            message = "No exact duplicate rows found."
        else:
            message = (
                f"Found {full_dup_count:,} exact duplicate rows "
                f"({full_dup_pct:.1f}% of {total_rows:,} rows)."
            )

        results.append(self._make_result(
            table=table_name,
            column=None,
            status=status,
            severity=severity,
            message=message,
            value=round(full_dup_pct, 2),
            threshold=warning_threshold if severity == Severity.WARNING
                else critical_threshold if severity == Severity.CRITICAL
                else warning_threshold,
            affected_rows=full_dup_count,
            total_rows=total_rows,
            details={
                "duplicate_count": full_dup_count,
                "duplicate_percentage": round(full_dup_pct, 2),
                "check_scope": "all_columns",
            },
        ))

        # Key-column duplicate check (if specific columns provided)
        if key_columns:
            valid_keys = [c for c in key_columns if c in df.columns]
            if valid_keys:
                key_dup_mask = df.duplicated(subset=valid_keys, keep="first")
                key_dup_count = int(key_dup_mask.sum())
                key_dup_pct = (key_dup_count / total_rows) * 100

                severity = self._determine_severity(
                    key_dup_pct, warning_threshold, critical_threshold
                )
                status = CheckStatus.PASSED if severity == Severity.PASS else CheckStatus.FAILED
                key_label = ", ".join(valid_keys)

                if severity == Severity.PASS:
                    message = f"No duplicates found on key columns: [{key_label}]."
                else:
                    message = (
                        f"Found {key_dup_count:,} duplicate rows on key columns "
                        f"[{key_label}] ({key_dup_pct:.1f}% of {total_rows:,} rows)."
                    )

                results.append(self._make_result(
                    table=table_name,
                    column=key_label,
                    status=status,
                    severity=severity,
                    message=message,
                    value=round(key_dup_pct, 2),
                    threshold=warning_threshold if severity == Severity.WARNING
                        else critical_threshold if severity == Severity.CRITICAL
                        else warning_threshold,
                    affected_rows=key_dup_count,
                    total_rows=total_rows,
                    details={
                        "duplicate_count": key_dup_count,
                        "duplicate_percentage": round(key_dup_pct, 2),
                        "key_columns": valid_keys,
                        "check_scope": "key_columns",
                    },
                ))

        return results
