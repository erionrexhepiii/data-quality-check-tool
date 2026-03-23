"""Tests for the duplicate check."""

import pandas as pd
import pytest

from checks.duplicates import DuplicateCheck
from core.models import CheckStatus, Severity


@pytest.fixture
def check():
    return DuplicateCheck()


class TestDuplicateCheck:
    def test_no_duplicates(self, check, clean_df):
        results = check.run(clean_df, "test_table")
        assert len(results) == 1
        assert results[0].status == CheckStatus.PASSED

    def test_detects_full_duplicates(self, check):
        df = pd.DataFrame({
            "a": [1, 2, 3, 1, 2],
            "b": ["x", "y", "z", "x", "y"],
        })
        results = check.run(df, "test_table")
        # Full-row duplicates: rows 3 and 4 are duplicates of rows 0 and 1
        r = results[0]
        assert r.status == CheckStatus.FAILED
        assert r.affected_rows == 2

    def test_key_column_duplicates(self, check, sample_df):
        results = check.run(
            sample_df, "test_table",
            params={"key_columns": ["id"]},
        )
        # Should have 2 results: full-row check + key-column check
        assert len(results) == 2
        key_result = results[1]
        assert key_result.details["check_scope"] == "key_columns"
        # id=5 is duplicated
        assert key_result.affected_rows == 1

    def test_empty_dataframe(self, check, empty_df):
        results = check.run(empty_df, "test_table")
        assert results[0].status == CheckStatus.SKIPPED

    def test_severity_thresholds(self, check):
        # 50% duplicates should be critical
        df = pd.DataFrame({"a": [1, 1, 1, 1, 1, 2, 2, 2, 2, 2]})
        results = check.run(df, "test", params={
            "warning_threshold": 1.0,
            "critical_threshold": 5.0,
        })
        assert results[0].severity == Severity.CRITICAL
