"""Tests for the connection store with Fernet encryption."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from core.models import ConnectionConfig, ConnectorType
from storage.database import Database
from storage.connection_store import ConnectionStore


@pytest.fixture
def store(tmp_path):
    """Create a connection store backed by a temp DB."""
    db = Database(str(tmp_path / "test.db"))
    db.initialize()
    key_path = str(tmp_path / ".key")
    return ConnectionStore(db, encryption_key_path=key_path)


@pytest.fixture
def sample_config():
    return ConnectionConfig(
        name="test-csv",
        connector_type=ConnectorType.CSV,
        params={"file_path": "/data/test.csv", "delimiter": ","},
    )


class TestConnectionStore:
    def test_save_and_retrieve(self, store, sample_config):
        store.save(sample_config)
        loaded = store.get_by_name("test-csv")
        assert loaded is not None
        assert loaded.name == "test-csv"
        assert loaded.connector_type == ConnectorType.CSV
        assert loaded.params["file_path"] == "/data/test.csv"

    def test_params_are_encrypted(self, store, sample_config, tmp_path):
        """Verify the raw DB value is not plaintext JSON."""
        store.save(sample_config)
        # Read raw encrypted value from DB
        db = Database(str(tmp_path / "test.db"))
        db.initialize()
        row = db.fetchone("SELECT params_encrypted FROM connections WHERE name = 'test-csv'")
        raw = row["params_encrypted"]
        # Fernet tokens start with 'gAAAAA'
        assert raw.startswith("gAAAAA")
        assert "/data/test.csv" not in raw

    def test_list_all(self, store):
        store.save(ConnectionConfig(name="a", connector_type=ConnectorType.CSV, params={}))
        store.save(ConnectionConfig(name="b", connector_type=ConnectorType.CSV, params={}))
        all_conns = store.list_all()
        assert len(all_conns) == 2
        names = [c.name for c in all_conns]
        assert "a" in names
        assert "b" in names

    def test_list_names(self, store):
        store.save(ConnectionConfig(name="alpha", connector_type=ConnectorType.CSV, params={}))
        store.save(ConnectionConfig(name="beta", connector_type=ConnectorType.CSV, params={}))
        names = store.list_names()
        assert names == ["alpha", "beta"]

    def test_update_existing(self, store, sample_config):
        store.save(sample_config)
        sample_config.params["delimiter"] = ";"
        store.save(sample_config)

        loaded = store.get_by_name("test-csv")
        assert loaded.params["delimiter"] == ";"
        assert store.count() == 1

    def test_delete(self, store, sample_config):
        store.save(sample_config)
        assert store.delete("test-csv") is True
        assert store.get_by_name("test-csv") is None

    def test_delete_nonexistent(self, store):
        assert store.delete("nonexistent") is False

    def test_get_by_id(self, store, sample_config):
        store.save(sample_config)
        loaded = store.get_by_id(sample_config.connection_id)
        assert loaded is not None
        assert loaded.name == "test-csv"

    def test_count(self, store):
        assert store.count() == 0
        store.save(ConnectionConfig(name="x", connector_type=ConnectorType.CSV, params={}))
        assert store.count() == 1

    def test_password_encrypted(self, store):
        """Ensure sensitive params like passwords survive round-trip."""
        config = ConnectionConfig(
            name="pg-prod",
            connector_type=ConnectorType.POSTGRESQL,
            params={"host": "localhost", "password": "s3cret!@#$%"},
        )
        store.save(config)
        loaded = store.get_by_name("pg-prod")
        assert loaded.params["password"] == "s3cret!@#$%"
