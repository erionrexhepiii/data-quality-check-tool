"""Range and boundary checks for numeric and date columns."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from checks.base import BaseCheck
from core.models import CheckResult, CheckStatus, Severity


class RangeCheck(BaseCheck):
    """Validates that numeric/date values fall within expected boundaries.

    Checks performed:
    - Values outside explicit [min_value, max_value] range
    - Unexpected negative values (for columns expected to be non-negative)
    - Future dates (for date columns where future values are invalid)

    Params:
        warning_threshold: float — out-of-range % for WARNING (default: 2.0)
        critical_threshold: float — out-of-range % for CRITICAL (default: 10.0)
        min_value: float/str — minimum allowed value (None = no lower bound)
        max_value: float/str — maximum allowed value (None = no upper bound)
        allow_negatives: bool — whether negative values are acceptable (default: True)
        allow_future_dates: bool — whether future dates are acceptable (default: True)
    """

    @property
    def name(self) -> str:
        return "range_check"

    @property
    def description(self) -> str:
        return "Validates that values fall within expected min/max boundaries."

    @property
    def supported_dtypes(self) -> Optional[list[str]]:
        # i=int, u=unsigned int, f=float, M=datetime
        return ["i", "u", "f", "M"]

    def run(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: Optional[list[str]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> list[CheckResult]:
        params = params or {}
        warning_threshold = params.get("warning_threshold", 2.0)
        critical_threshold = params.get("critical_threshold", 10.0)
        min_value = params.get("min_value")
        max_value = params.get("max_value")
        allow_negatives = params.get("allow_negatives", True)
        allow_future_dates = params.get("allow_future_dates", True)

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
            if not self.applies_to_column(series):
                continue

            non_null = series.dropna()
            if len(non_null) == 0:
                continue

            is_datetime = series.dtype.kind == "M"

            # ---- Explicit range check ----
            if min_value is not None or max_value is not None:
                results.extend(self._check_explicit_range(
                    non_null, col, table_name, total_rows,
                    min_value, max_value,
                    warning_threshold, critical_threshold,
                ))

            # ---- Negative value check (numeric only) ----
            if not allow_negatives and not is_datetime:
                results.extend(self._check_negatives(
                    non_null, col, table_name, total_rows,
                    warning_threshold, critical_threshold,
                ))

            # ---- Future date check ----
            if is_datetime and not allow_future_dates:
                results.extend(self._check_future_dates(
                    non_null, col, table_name, total_rows,
                    warning_threshold, critical_threshold,
                ))

            # ---- Auto-detect: if no explicit bounds, report observed range ----
            if min_value is None and max_value is None and not is_datetime:
                actual_min = float(non_null.min())
                actual_max = float(non_null.max())
                results.append(self._make_result(
                    table=table_name,
                    column=col,
                    status=CheckStatus.PASSED,
                    severity=Severity.PASS,
                    message=(
                        f"Column '{col}' range: [{actual_min:,.2f}, {actual_max:,.2f}]. "
                        f"No explicit bounds configured."
                    ),
                    total_rows=total_rows,
                    details={
                        "observed_min": actual_min,
                        "observed_max": actual_max,
                        "mean": float(non_null.mean()),
                        "std": float(non_null.std()) if len(non_null) > 1 else 0.0,
                    },
                ))

        return results

    def _check_explicit_range(
        self,
        series: pd.Series,
        col: str,
        table: str,
        total_rows: int,
        min_value: Any,
        max_value: Any,
        warning_threshold: float,
        critical_threshold: float,
    ) -> list[CheckResult]:
        out_of_range = pd.Series([False] * len(series), index=series.index)

        if min_value is not None:
            out_of_range = out_of_range | (series < min_value)
        if max_value is not None:
            out_of_range = out_of_range | (series > max_value)

        violation_count = int(out_of_range.sum())
        violation_pct = (violation_count / total_rows) * 100

        severity = self._determine_severity(
            violation_pct, warning_threshold, critical_threshold
        )
        status = CheckStatus.PASSED if severity == Severity.PASS else CheckStatus.FAILED

        bounds = f"[{min_value}, {max_value}]"
        if severity == Severity.PASS:
            message = f"Column '{col}' — all values within range {bounds}."
        else:
            message = (
                f"Column '{col}' has {violation_count:,} values outside range "
                f"{bounds} ({violation_pct:.1f}%)."
            )

        return [self._make_result(
            table=table,
            column=col,
            status=status,
            severity=severity,
            message=message,
            value=round(violation_pct, 2),
            threshold=warning_threshold,
            affected_rows=violation_count,
            total_rows=total_rows,
            details={
                "min_bound": min_value,
                "max_bound": max_value,
                "violations": violation_count,
                "violation_percentage": round(violation_pct, 2),
            },
        )]

    def _check_negatives(
        self,
        series: pd.Series,
        col: str,
        table: str,
        total_rows: int,
        warning_threshold: float,
        critical_threshold: float,
    ) -> list[CheckResult]:
        neg_count = int((series < 0).sum())
        neg_pct = (neg_count / total_rows) * 100

        severity = self._determine_severity(
            neg_pct, warning_threshold, critical_threshold
        )
        status = CheckStatus.PASSED if severity == Severity.PASS else CheckStatus.FAILED

        if severity == Severity.PASS:
            message = f"Column '{col}' has no unexpected negative values."
        else:
            message = (
                f"Column '{col}' has {neg_count:,} negative values ({neg_pct:.1f}%)."
            )

        return [self._make_result(
            table=table,
            column=col,
            status=status,
            severity=severity,
            message=message,
            value=round(neg_pct, 2),
            affected_rows=neg_count,
            total_rows=total_rows,
            details={"negative_count": neg_count, "check_sub_type": "negative_values"},
        )]

    def _check_future_dates(
        self,
        series: pd.Series,
        col: str,
        table: str,
        total_rows: int,
        warning_threshold: float,
        critical_threshold: float,
    ) -> list[CheckResult]:
        now = pd.Timestamp.now()
        future_count = int((series > now).sum())
        future_pct = (future_count / total_rows) * 100

        severity = self._determine_severity(
            future_pct, warning_threshold, critical_threshold
        )
        status = CheckStatus.PASSED if severity == Severity.PASS else CheckStatus.FAILED

        if severity == Severity.PASS:
            message = f"Column '{col}' has no future dates."
        else:
            message = (
                f"Column '{col}' has {future_count:,} future dates ({future_pct:.1f}%)."
            )

        return [self._make_result(
            table=table,
            column=col,
            status=status,
            severity=severity,
            message=message,
            value=round(future_pct, 2),
            affected_rows=future_count,
            total_rows=total_rows,
            details={"future_date_count": future_count, "check_sub_type": "future_dates"},
        )]
