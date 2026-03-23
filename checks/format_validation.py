"""Format validation checks using regex patterns."""

from __future__ import annotations

import re
from typing import Any, Optional

import pandas as pd

from checks.base import BaseCheck
from core.models import CheckResult, CheckStatus, Severity

# Built-in format patterns
BUILTIN_PATTERNS: dict[str, dict[str, str]] = {
    "email": {
        "pattern": r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$",
        "label": "Email Address",
    },
    "phone_e164": {
        "pattern": r"^\+?[1-9]\d{1,14}$",
        "label": "Phone (E.164)",
    },
    "phone_us": {
        "pattern": r"^(\+1)?[\s\-.]?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}$",
        "label": "Phone (US)",
    },
    "url": {
        "pattern": r"^https?://[^\s/$.?#].[^\s]*$",
        "label": "URL",
    },
    "date_iso": {
        "pattern": r"^\d{4}-\d{2}-\d{2}$",
        "label": "Date (ISO: YYYY-MM-DD)",
    },
    "date_us": {
        "pattern": r"^\d{2}/\d{2}/\d{4}$",
        "label": "Date (US: MM/DD/YYYY)",
    },
    "zip_us": {
        "pattern": r"^\d{5}(-\d{4})?$",
        "label": "US Zip Code",
    },
    "ipv4": {
        "pattern": r"^(\d{1,3}\.){3}\d{1,3}$",
        "label": "IPv4 Address",
    },
    "uuid": {
        "pattern": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        "label": "UUID",
    },
}


class FormatValidationCheck(BaseCheck):
    """Validates column values against expected format patterns.

    Uses regex to check that string values conform to expected formats
    such as emails, phone numbers, dates, URLs, etc.

    Params:
        warning_threshold: float — invalid % for WARNING (default: 5.0)
        critical_threshold: float — invalid % for CRITICAL (default: 15.0)
        format_type: str — one of the built-in format keys (e.g. "email", "url")
        custom_pattern: str — a custom regex pattern (overrides format_type)
        custom_label: str — label for the custom pattern
        skip_nulls: bool — whether to exclude nulls from the check (default: True)
        column_formats: dict[str, str] — mapping of column name to format_type,
                        for checking different formats on different columns
    """

    @property
    def name(self) -> str:
        return "format_check"

    @property
    def description(self) -> str:
        return "Validates values against expected format patterns (email, phone, date, URL, etc.)."

    @property
    def supported_dtypes(self) -> Optional[list[str]]:
        return ["O"]  # object/string columns only

    def run(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: Optional[list[str]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> list[CheckResult]:
        params = params or {}
        warning_threshold = params.get("warning_threshold", 5.0)
        critical_threshold = params.get("critical_threshold", 15.0)
        skip_nulls = params.get("skip_nulls", True)

        # Per-column format assignments
        column_formats: dict[str, str] = params.get("column_formats", {})

        # Global format (applies to all target columns without a specific assignment)
        global_format = params.get("format_type")
        custom_pattern = params.get("custom_pattern")
        custom_label = params.get("custom_label", "Custom Pattern")

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

            # Determine which pattern to use for this column
            fmt_type = column_formats.get(col, global_format)

            if custom_pattern:
                pattern = custom_pattern
                label = custom_label
            elif fmt_type and fmt_type in BUILTIN_PATTERNS:
                pattern = BUILTIN_PATTERNS[fmt_type]["pattern"]
                label = BUILTIN_PATTERNS[fmt_type]["label"]
            else:
                # No format specified for this column — try auto-detection
                detected = self._auto_detect_format(series)
                if detected is None:
                    continue  # Can't determine format, skip
                pattern = BUILTIN_PATTERNS[detected]["pattern"]
                label = BUILTIN_PATTERNS[detected]["label"] + " (auto-detected)"

            # Run the validation
            result = self._validate_column(
                series=series,
                col=col,
                table=table_name,
                pattern=pattern,
                label=label,
                total_rows=total_rows,
                skip_nulls=skip_nulls,
                warning_threshold=warning_threshold,
                critical_threshold=critical_threshold,
            )
            results.append(result)

        return results

    def _validate_column(
        self,
        series: pd.Series,
        col: str,
        table: str,
        pattern: str,
        label: str,
        total_rows: int,
        skip_nulls: bool,
        warning_threshold: float,
        critical_threshold: float,
    ) -> CheckResult:
        """Validate a single column against a regex pattern."""
        if skip_nulls:
            check_series = series.dropna()
            check_series = check_series[check_series != ""]
        else:
            check_series = series.fillna("")

        check_count = len(check_series)
        if check_count == 0:
            return self._make_result(
                table=table,
                column=col,
                status=CheckStatus.PASSED,
                severity=Severity.PASS,
                message=f"Column '{col}' — no non-null values to validate.",
                total_rows=total_rows,
            )

        compiled = re.compile(pattern)
        matches = check_series.astype(str).apply(lambda v: bool(compiled.match(v)))
        invalid_count = int((~matches).sum())
        invalid_pct = (invalid_count / check_count) * 100

        severity = self._determine_severity(
            invalid_pct, warning_threshold, critical_threshold
        )
        status = CheckStatus.PASSED if severity == Severity.PASS else CheckStatus.FAILED

        if severity == Severity.PASS:
            message = f"Column '{col}' — all values match {label} format."
        else:
            message = (
                f"Column '{col}' has {invalid_count:,} values not matching {label} "
                f"format ({invalid_pct:.1f}% of {check_count:,} non-null values)."
            )

        # Collect sample invalid values for debugging
        invalid_mask = ~matches
        sample_invalid = (
            check_series[invalid_mask].head(5).tolist() if invalid_count > 0 else []
        )

        return self._make_result(
            table=table,
            column=col,
            status=status,
            severity=severity,
            message=message,
            value=round(invalid_pct, 2),
            threshold=warning_threshold,
            affected_rows=invalid_count,
            total_rows=check_count,
            details={
                "format_type": label,
                "pattern": pattern,
                "invalid_count": invalid_count,
                "invalid_percentage": round(invalid_pct, 2),
                "checked_count": check_count,
                "sample_invalid_values": sample_invalid,
            },
        )

    def _auto_detect_format(self, series: pd.Series) -> Optional[str]:
        """Try to guess the format by sampling non-null values.

        Returns a format_type key or None if no pattern matches.
        """
        sample = series.dropna()
        sample = sample[sample != ""]
        if len(sample) == 0:
            return None

        # Sample up to 100 values for detection
        sample = sample.head(100).astype(str)

        best_match: Optional[str] = None
        best_rate = 0.0

        for fmt_key, fmt_info in BUILTIN_PATTERNS.items():
            compiled = re.compile(fmt_info["pattern"])
            match_rate = sample.apply(lambda v: bool(compiled.match(v))).mean()
            # Require at least 80% match rate to consider it a detected format
            if match_rate >= 0.8 and match_rate > best_rate:
                best_rate = match_rate
                best_match = fmt_key

        return best_match

    @staticmethod
    def available_formats() -> dict[str, str]:
        """Return available built-in format types and their labels."""
        return {k: v["label"] for k, v in BUILTIN_PATTERNS.items()}
