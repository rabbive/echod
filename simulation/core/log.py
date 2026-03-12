"""Replicated log for the ECHO / Raft consensus protocols.

In-memory list-backed log with append, commit, truncate, and comparison
operations.  Uses 1-based indexing to match the Raft paper conventions.
"""

from __future__ import annotations

from simulation.core.messages import LogEntry


class ReplicatedLog:
    """Ordered, in-memory replicated log."""

    def __init__(self) -> None:
        self._entries: list[LogEntry] = []
        self.commit_index: int = 0
        self.last_applied: int = 0

    # ----- queries -----

    @property
    def last_index(self) -> int:
        """1-based index of the most recent entry, or 0 if empty."""
        return len(self._entries)

    @property
    def last_term(self) -> int:
        """Term of the most recent entry, or 0 if empty."""
        if self._entries:
            return self._entries[-1].term
        return 0

    def get(self, index: int) -> LogEntry | None:
        """Return the entry at *1-based* index, or None if out of range."""
        if 1 <= index <= len(self._entries):
            return self._entries[index - 1]
        return None

    def entries_from(self, start_index: int) -> list[LogEntry]:
        """Return all entries from *start_index* (1-based, inclusive) onward."""
        if start_index < 1:
            start_index = 1
        return list(self._entries[start_index - 1:])

    def term_at(self, index: int) -> int:
        """Return the term at a given 1-based index, or 0 if absent."""
        entry = self.get(index)
        return entry.term if entry is not None else 0

    # ----- mutations -----

    def append(self, entry: LogEntry) -> None:
        """Append a single entry to the end of the log."""
        self._entries.append(entry)

    def append_entries(self, entries: list[LogEntry]) -> None:
        """Append a batch of entries (e.g. from an AppendEntries RPC)."""
        self._entries.extend(entries)

    def truncate_from(self, index: int) -> None:
        """Remove all entries from *index* (1-based, inclusive) onward.

        Used when a log conflict is detected during AppendEntries.
        """
        if index >= 1:
            self._entries = self._entries[:index - 1]

    def commit(self, new_commit_index: int) -> list[LogEntry]:
        """Advance the commit index and return newly committed entries.

        Entries between the old commit_index+1 and new_commit_index
        (inclusive) are returned so the caller can apply them to the
        state machine.
        """
        if new_commit_index <= self.commit_index:
            return []
        old = self.commit_index
        self.commit_index = min(new_commit_index, self.last_index)
        return list(self._entries[old:self.commit_index])

    # ----- comparison helpers -----

    def is_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        """Check whether the given (index, term) is at least as up-to-date.

        Raft §5.4.1: the candidate's log is at least as up-to-date as the
        receiver's log if the candidate's last term is higher, or if the terms
        match and the candidate's log is at least as long.
        """
        if last_log_term != self.last_term:
            return last_log_term > self.last_term
        return last_log_index >= self.last_index

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:
        return (
            f"ReplicatedLog(entries={len(self._entries)}, "
            f"commit={self.commit_index})"
        )
