"""Tests for the SQLite database layer."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import os
import tempfile

import pytest

from storage.database import Database, CURRENT_SCHEMA_VERSION


@pytest.fixture
def db(tmp_path):
    """Create a temporary database."""
    db_path = str(tmp_path / "test.db")
    database = Database(db_path)
    database.initialize()
    return database


class TestDatabase:
    def test_initialize_creates_tables(self, db):
        tables = db.fetchall(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        table_names = [t["name"] for t in tables]
        assert "connections" in table_names
        assert "run_summaries" in table_names
        assert "check_results" in table_names
        assert "schema_version" in table_names

    def test_schema_version_set(self, db):
        row = db.fetchone("SELECT version FROM schema_version")
        assert row["version"] == CURRENT_SCHEMA_VERSION

    def test_initialize_idempotent(self, db):
        # Calling initialize again should not fail
        db.initialize()
        row = db.fetchone("SELECT version FROM schema_version")
        assert row["version"] == CURRENT_SCHEMA_VERSION

    def test_foreign_keys_enabled(self, db):
        row = db.fetchone("PRAGMA foreign_keys")
        assert row[0] == 1

    def test_wal_mode(self, db):
        row = db.fetchone("PRAGMA journal_mode")
        assert row[0] == "wal"

    def test_close_and_reopen(self, tmp_path):
        db_path = str(tmp_path / "reopen_test.db")
        db1 = Database(db_path)
        db1.initialize()
        db1.execute("INSERT INTO connections VALUES (?, ?, ?, ?, ?, ?)",
                     ("id1", "test", "csv", "enc", "2024-01-01", "2024-01-01"))
        db1.commit()
        db1.close()

        db2 = Database(db_path)
        db2.initialize()
        row = db2.fetchone("SELECT name FROM connections WHERE connection_id = 'id1'")
        assert row["name"] == "test"
        db2.close()
