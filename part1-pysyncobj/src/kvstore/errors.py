"""Errors that the API maps to HTTP status codes.

The split matters in production. A client may retry a rejected command at once.
A client must not blindly retry a command with an unknown outcome: it may already be
committed (AGENTS.md, Raft rule 3).
"""


class KeyValueStoreError(Exception):
    pass


class CommandRejectedError(KeyValueStoreError):
    """The command is known NOT to be applied. A retry is safe. HTTP 503."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"command rejected: {reason}")
        self.reason = reason


class CommandOutcomeUnknownError(KeyValueStoreError):
    """The command may or may not be committed. HTTP 504."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"command outcome unknown: {reason}")
        self.reason = reason
