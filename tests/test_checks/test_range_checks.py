"""Tests for the range check."""

import pandas as pd
import pytest

from checks.range_checks import RangeCheck
from core.models import CheckStatus, Severity


@pytest.fixture
def check():
    return RangeCheck()


class TestRangeCheck:
    def test_no_bounds_reports_observed_range(self, check, clean_df):
        results = check.run(clean_df, "test_table", columns=["value"])
        assert len(results) == 1
        r = results[0]
        assert r.status == CheckStatus.PASSED
        assert r.details["observed_min"] == 10.0
        assert r.details["observed_max"] == 50.0

    def test_explicit_range_violation(self, check, sample_df):
        results = check.run(
            sample_df, "test_table",
            columns=["age"],
            params={"min_value": 0, "max_value": 120},
        )
        assert len(results) >= 1
        # age has -5 (below 0) and 150 (above 120) and one null
        range_result = results[0]
        assert range_result.affected_rows == 2

    def test_negative_check(self, check, sample_df):
        results = check.run(
            sample_df, "test_table",
            columns=["score"],
            params={"allow_negatives": False},
        )
        # score has -3.0 as a negative value
        neg_results = [r for r in results if r.details.get("check_sub_type") == "negative_values"]
        assert len(neg_results) == 1
        assert neg_results[0].affected_rows == 1

    def test_skips_string_columns(self, check, sample_df):
        results = check.run(sample_df, "test_table", columns=["name"])
        # String column should be skipped entirely
        assert len(results) == 0

    def test_empty_dataframe(self, check, empty_df):
        results = check.run(empty_df, "test_table")
        assert results[0].status == CheckStatus.SKIPPED
