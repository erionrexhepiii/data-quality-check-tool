"""CSV export for check results and run summaries."""

from __future__ import annotations

import csv
import io
from typing import Any, Optional

import pandas as pd

from core.models import CheckResult, RunSummary


def results_to_dataframe(results: list[CheckResult]) -> pd.DataFrame:
    """Convert a list of CheckResult objects into a flat DataFrame."""
    if not results:
        return pd.DataFrame()

    rows = []
    for r in results:
        rows.append({
            "run_id": r.run_id,
            "check_name": r.check_name,
            "check_type": r.check_type,
            "table": r.table,
            "column": r.column or "",
            "status": r.status.value,
            "severity": r.severity.value,
            "message": r.message,
            "value": r.value,
            "threshold": r.threshold,
            "affected_rows": r.affected_rows,
            "total_rows": r.total_rows,
            "pass_rate": f"{r.pass_rate:.2%}" if r.pass_rate is not None else "",
            "timestamp": r.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


def summary_to_dataframe(summary: RunSummary) -> pd.DataFrame:
    """Convert a RunSummary into a single-row DataFrame."""
    return pd.DataFrame([{
        "run_id": summary.run_id,
        "suite_name": summary.suite_name,
        "connection_name": summary.connection_name,
        "started_at": summary.started_at.strftime("%Y-%m-%d %H:%M:%S") if summary.started_at else "",
        "completed_at": summary.completed_at.strftime("%Y-%m-%d %H:%M:%S") if summary.completed_at else "",
        "total_checks": summary.total_checks,
        "passed": summary.passed_count,
        "failed": summary.failed_count,
        "errors": summary.error_count,
        "critical": summary.critical_count,
        "warnings": summary.warning_count,
        "quality_score": round(summary.quality_score, 2),
    }])


def export_results_csv(
    results: list[CheckResult],
    delimiter: str = ",",
) -> str:
    """Export check results to a CSV string.

    Returns the CSV content as a string (suitable for Streamlit download).
    """
    df = results_to_dataframe(results)
    if df.empty:
        return ""
    return df.to_csv(index=False, sep=delimiter)


def export_results_bytes(
    results: list[CheckResult],
    delimiter: str = ",",
) -> bytes:
    """Export check results to CSV as bytes (for Streamlit download_button)."""
    return export_results_csv(results, delimiter).encode("utf-8")


def export_run_csv(
    summary: RunSummary,
    delimiter: str = ",",
) -> str:
    """Export a full run: summary header + detail rows, as a CSV string."""
    parts = []

    # Summary section
    parts.append("# Run Summary")
    summary_df = summary_to_dataframe(summary)
    parts.append(summary_df.to_csv(index=False, sep=delimiter))

    # Detail section
    parts.append("# Check Results")
    results_df = results_to_dataframe(summary.results)
    if not results_df.empty:
        parts.append(results_df.to_csv(index=False, sep=delimiter))
    else:
        parts.append("No results.\n")

    return "\n".join(parts)


def export_run_bytes(
    summary: RunSummary,
    delimiter: str = ",",
) -> bytes:
    """Export a full run as CSV bytes."""
    return export_run_csv(summary, delimiter).encode("utf-8")
