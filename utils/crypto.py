"""Encryption utilities for securing stored credentials."""

from __future__ import annotations

import os
from pathlib import Path

from cryptography.fernet import Fernet

from core.exceptions import EncryptionError


def get_or_create_key(key_path: str) -> bytes:
    """Load an existing encryption key or generate a new one.

    The key is stored as a file. On first run, a new key is generated.
    """
    path = Path(key_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        return path.read_bytes()

    key = Fernet.generate_key()
    path.write_bytes(key)
    return key


def encrypt(plaintext: str, key: bytes) -> str:
    """Encrypt a string and return the ciphertext as a string."""
    try:
        f = Fernet(key)
        return f.encrypt(plaintext.encode("utf-8")).decode("utf-8")
    except Exception as e:
        raise EncryptionError(f"Encryption failed: {e}")


def decrypt(ciphertext: str, key: bytes) -> str:
    """Decrypt a ciphertext string back to plaintext."""
    try:
        f = Fernet(key)
        return f.decrypt(ciphertext.encode("utf-8")).decode("utf-8")
    except Exception as e:
        raise EncryptionError(f"Decryption failed: {e}")
