"""Shared test fixtures for the DQC test suite."""

import sys
from pathlib import Path

# Ensure imports work from the project root
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

import pandas as pd
import pytest


@pytest.fixture
def sample_df():
    """A small DataFrame with a mix of clean and dirty data."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 5, 6, 7, 8, 9],
        "name": ["Alice", "Bob", None, "Diana", "", "Eve", "Frank", None, "Helen", "Ivan"],
        "email": [
            "alice@example.com",
            "bad-email",
            "charlie@example.com",
            "diana@example.com",
            "not_an_email",
            "eve@example.com",
            "frank@example.com",
            "george@example.com",
            "helen@example",
            "ivan@example.com",
        ],
        "age": [25, 30, -5, 150, 22, 28, 35, 40, None, 29],
        "score": [88.5, 92.0, 101.0, 75.0, 88.5, 92.0, -3.0, 85.0, 90.0, 88.5],
        "phone": [
            "+14155551234",
            "415-555-1234",
            "not-a-phone",
            "+14155551237",
            "",
            "+14155551239",
            "12345",
            "+14155551241",
            "+14155551242",
            "+14155551243",
        ],
    })


@pytest.fixture
def clean_df():
    """A fully clean DataFrame with no issues."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "value": [10.0, 20.0, 30.0, 40.0, 50.0],
    })


@pytest.fixture
def empty_df():
    """An empty DataFrame."""
    return pd.DataFrame({"id": [], "name": [], "value": []})
