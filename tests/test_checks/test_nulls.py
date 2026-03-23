"""Tests for the null check."""

import pandas as pd
import pytest

from checks.nulls import NullCheck
from core.models import CheckStatus, Severity


@pytest.fixture
def check():
    return NullCheck()


class TestNullCheck:
    def test_no_nulls(self, check, clean_df):
        results = check.run(clean_df, "test_table")
        for r in results:
            assert r.status == CheckStatus.PASSED
            assert r.severity == Severity.PASS

    def test_detects_nulls(self, check, sample_df):
        results = check.run(sample_df, "test_table", columns=["name"])
        assert len(results) == 1
        r = results[0]
        assert r.status == CheckStatus.FAILED
        assert r.affected_rows > 0
        # 2 None + 1 empty string = 3 nulls out of 10
        assert r.affected_rows == 3

    def test_empty_dataframe(self, check, empty_df):
        results = check.run(empty_df, "test_table")
        assert len(results) == 1
        assert results[0].status == CheckStatus.SKIPPED

    def test_empty_strings_excluded(self, check, sample_df):
        results = check.run(
            sample_df, "test_table",
            columns=["name"],
            params={"include_empty_strings": False},
        )
        r = results[0]
        # Only 2 actual None values
        assert r.affected_rows == 2

    def test_severity_thresholds(self, check):
        # 50% nulls should be critical
        df = pd.DataFrame({"col": [None, None, None, None, None, 1, 2, 3, 4, 5]})
        results = check.run(df, "test", params={
            "warning_threshold": 5.0,
            "critical_threshold": 20.0,
        })
        assert results[0].severity == Severity.CRITICAL

    def test_column_filtering(self, check, sample_df):
        results = check.run(sample_df, "test", columns=["id", "email"])
        columns_checked = {r.column for r in results}
        assert columns_checked == {"id", "email"}
