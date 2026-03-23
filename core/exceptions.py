"""Custom exception hierarchy for the DQC Tool."""


class DQCError(Exception):
    """Base exception for all DQC Tool errors."""


class ConnectionError(DQCError):
    """Raised when a database connection fails."""


class ConnectorNotFoundError(DQCError):
    """Raised when a requested connector type is not registered."""


class CheckError(DQCError):
    """Raised when a check fails to execute (not a data quality failure)."""


class CheckNotFoundError(DQCError):
    """Raised when a requested check type is not registered."""


class ConfigError(DQCError):
    """Raised when configuration is invalid or missing."""


class StorageError(DQCError):
    """Raised when a storage operation fails."""


class EncryptionError(DQCError):
    """Raised when encryption or decryption of credentials fails."""


class ValidationError(DQCError):
    """Raised when input validation fails."""
