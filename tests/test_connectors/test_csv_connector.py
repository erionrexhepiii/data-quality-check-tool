"""Tests for the CSV connector."""

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from connectors.csv_connector import CSVConnector
from core.exceptions import ConnectionError


@pytest.fixture
def csv_file(tmp_path):
    """Create a temp CSV file."""
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    path = tmp_path / "test.csv"
    df.to_csv(path, index=False)
    return str(path)


@pytest.fixture
def csv_dir(tmp_path):
    """Create a temp directory with multiple CSV files."""
    for name in ["users", "orders"]:
        df = pd.DataFrame({"id": [1, 2], "data": ["x", "y"]})
        df.to_csv(tmp_path / f"{name}.csv", index=False)
    return str(tmp_path)


class TestCSVConnector:
    def test_connect_to_file(self, csv_file):
        conn = CSVConnector()
        conn.connect({"file_path": csv_file})
        assert conn.is_connected
        assert conn.test_connection()

    def test_connect_to_directory(self, csv_dir):
        conn = CSVConnector()
        conn.connect({"file_path": csv_dir})
        tables = conn.list_tables()
        assert "users" in tables
        assert "orders" in tables

    def test_connect_missing_path(self):
        conn = CSVConnector()
        with pytest.raises(ConnectionError):
            conn.connect({"file_path": "/nonexistent/path.csv"})

    def test_list_tables(self, csv_file):
        conn = CSVConnector()
        conn.connect({"file_path": csv_file})
        tables = conn.list_tables()
        assert len(tables) == 1
        assert tables[0] == "test"

    def test_get_columns(self, csv_file):
        conn = CSVConnector()
        conn.connect({"file_path": csv_file})
        columns = conn.get_columns("test")
        assert len(columns) == 2
        col_names = [c.name for c in columns]
        assert "id" in col_names
        assert "name" in col_names

    def test_fetch_dataframe(self, csv_file):
        conn = CSVConnector()
        conn.connect({"file_path": csv_file})
        df = conn.fetch_dataframe("test")
        assert len(df) == 3
        assert list(df.columns) == ["id", "name"]

    def test_fetch_with_limit(self, csv_file):
        conn = CSVConnector()
        conn.connect({"file_path": csv_file})
        df = conn.fetch_dataframe("test", limit=2)
        assert len(df) == 2

    def test_get_row_count(self, csv_file):
        conn = CSVConnector()
        conn.connect({"file_path": csv_file})
        assert conn.get_row_count("test") == 3

    def test_disconnect(self, csv_file):
        conn = CSVConnector()
        conn.connect({"file_path": csv_file})
        conn.disconnect()
        assert not conn.is_connected

    def test_invalid_table_name(self, csv_file):
        conn = CSVConnector()
        conn.connect({"file_path": csv_file})
        with pytest.raises(ConnectionError):
            conn.fetch_dataframe("nonexistent")
