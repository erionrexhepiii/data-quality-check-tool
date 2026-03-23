"""Tests for CSV export functionality."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import csv
import io
from datetime import datetime, timezone

import pytest

from core.models import CheckResult, CheckStatus, RunSummary, Severity
from reporting.csv_export import (
    export_results_bytes,
    export_results_csv,
    export_run_bytes,
    export_run_csv,
    results_to_dataframe,
    summary_to_dataframe,
)


@pytest.fixture
def sample_results():
    now = datetime.now(timezone.utc)
    return [
        CheckResult(
            check_name="null_check",
            check_type="null_check",
            table="users",
            column="email",
            status=CheckStatus.FAILED,
            severity=Severity.WARNING,
            message="5% nulls",
            value=5.0,
            threshold=5.0,
            affected_rows=50,
            total_rows=1000,
            run_id="run123",
            timestamp=now,
        ),
        CheckResult(
            check_name="duplicate_check",
            check_type="duplicate_check",
            table="users",
            column=None,
            status=CheckStatus.PASSED,
            severity=Severity.PASS,
            message="No duplicates",
            value=0.0,
            threshold=1.0,
            affected_rows=0,
            total_rows=1000,
            run_id="run123",
            timestamp=now,
        ),
    ]


@pytest.fixture
def sample_summary(sample_results):
    now = datetime.now(timezone.utc)
    return RunSummary(
        run_id="run123",
        suite_name="test_suite",
        connection_name="test_conn",
        results=sample_results,
        started_at=now,
        completed_at=now,
    )


class TestCSVExport:
    def test_results_to_dataframe(self, sample_results):
        df = results_to_dataframe(sample_results)
        assert len(df) == 2
        assert "check_name" in df.columns
        assert "severity" in df.columns
        assert df.iloc[0]["check_name"] == "null_check"

    def test_results_to_dataframe_empty(self):
        df = results_to_dataframe([])
        assert df.empty

    def test_summary_to_dataframe(self, sample_summary):
        df = summary_to_dataframe(sample_summary)
        assert len(df) == 1
        assert df.iloc[0]["run_id"] == "run123"
        assert df.iloc[0]["total_checks"] == 2

    def test_export_results_csv(self, sample_results):
        csv_str = export_results_csv(sample_results)
        assert csv_str
        # Parse and verify
        reader = csv.DictReader(io.StringIO(csv_str))
        rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["check_name"] == "null_check"
        assert rows[0]["severity"] == "warning"

    def test_export_results_csv_empty(self):
        assert export_results_csv([]) == ""

    def test_export_results_bytes(self, sample_results):
        data = export_results_bytes(sample_results)
        assert isinstance(data, bytes)
        decoded = data.decode("utf-8")
        assert "null_check" in decoded

    def test_export_run_csv(self, sample_summary):
        csv_str = export_run_csv(sample_summary)
        assert "# Run Summary" in csv_str
        assert "# Check Results" in csv_str
        assert "run123" in csv_str

    def test_export_run_bytes(self, sample_summary):
        data = export_run_bytes(sample_summary)
        assert isinstance(data, bytes)
        assert b"run123" in data

    def test_custom_delimiter(self, sample_results):
        csv_str = export_results_csv(sample_results, delimiter="\t")
        # Tab-separated
        lines = csv_str.strip().split("\n")
        assert "\t" in lines[0]

    def test_pass_rate_formatting(self, sample_results):
        df = results_to_dataframe(sample_results)
        # First result: 50 affected / 1000 total = 95% pass rate
        assert df.iloc[0]["pass_rate"] == "95.00%"
        # Second result: 0 affected / 1000 total = 100%
        assert df.iloc[1]["pass_rate"] == "100.00%"
