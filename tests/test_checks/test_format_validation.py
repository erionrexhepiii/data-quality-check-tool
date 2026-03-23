"""Tests for the format validation check."""

import pandas as pd
import pytest

from checks.format_validation import FormatValidationCheck, BUILTIN_PATTERNS
from core.models import CheckStatus, Severity


@pytest.fixture
def check():
    return FormatValidationCheck()


class TestFormatValidationCheck:
    def test_valid_emails(self, check):
        df = pd.DataFrame({"email": [
            "alice@example.com", "bob@test.org", "charlie@company.co.uk",
        ]})
        results = check.run(df, "test", columns=["email"], params={"format_type": "email"})
        assert len(results) == 1
        assert results[0].status == CheckStatus.PASSED

    def test_invalid_emails(self, check, sample_df):
        results = check.run(
            sample_df, "test",
            columns=["email"],
            params={"format_type": "email"},
        )
        assert len(results) == 1
        r = results[0]
        assert r.status == CheckStatus.FAILED
        assert r.affected_rows > 0
        assert len(r.details["sample_invalid_values"]) > 0

    def test_phone_validation(self, check, sample_df):
        results = check.run(
            sample_df, "test",
            columns=["phone"],
            params={"format_type": "phone_e164"},
        )
        assert len(results) == 1
        r = results[0]
        assert r.status == CheckStatus.FAILED

    def test_custom_pattern(self, check):
        df = pd.DataFrame({"code": ["AB-123", "CD-456", "invalid", "EF-789"]})
        results = check.run(
            df, "test",
            columns=["code"],
            params={
                "custom_pattern": r"^[A-Z]{2}-\d{3}$",
                "custom_label": "Product Code",
            },
        )
        r = results[0]
        assert r.affected_rows == 1  # "invalid" doesn't match

    def test_skips_numeric_columns(self, check, clean_df):
        results = check.run(clean_df, "test", columns=["value"])
        assert len(results) == 0

    def test_empty_dataframe(self, check, empty_df):
        results = check.run(empty_df, "test")
        assert results[0].status == CheckStatus.SKIPPED

    def test_auto_detection(self, check):
        df = pd.DataFrame({"col": [
            "alice@example.com", "bob@test.org", "charlie@company.co.uk",
            "diana@test.io", "eve@foo.com",
        ]})
        results = check.run(df, "test", columns=["col"])
        # Should auto-detect email format
        assert len(results) == 1
        assert "auto-detected" in results[0].details["format_type"]

    def test_available_formats(self, check):
        formats = check.available_formats()
        assert "email" in formats
        assert "url" in formats
        assert "phone_e164" in formats
