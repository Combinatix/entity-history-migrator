"""Database backend interface for Entity History Migrator."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TypedDict


class LedgerEntry(TypedDict):
    """One append-only metadata record stored beside history data."""

    record_id: str
    registry_entry_id: str
    timeline_event_id: str
    operation_id: str
    kind: str
    logical_entity_id: str
    physical_entity_id: str
    start: str
    stop: str
    record_count: int
    created_at: str


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    """Operations supported by one history backend."""

    supports_copy: bool
    supports_delete: bool
    supports_units: bool
    supports_time_ranges: bool
    supports_ledger: bool


class HistoryBackend(ABC):
    """Abstract storage backend used by the migration engine."""

    backend_type: str
    display_name: str
    capabilities: BackendCapabilities

    @property
    @abstractmethod
    def target_name(self) -> str:
        """Return a short human-readable storage target name."""

    @property
    def storage_identity(self) -> str:
        """Identify the store to which a persisted deletion approval applies."""
        return f"{self.backend_type}:{self.target_name}"

    @property
    @abstractmethod
    def write_enabled(self) -> bool:
        """Return whether destructive/write operations are currently enabled."""

    @abstractmethod
    def verify_connection(self) -> None:
        """Raise when the configured history store is not readable."""

    @abstractmethod
    def count_records_and_units(
        self,
        entity_id: str,
        *,
        start: str,
        stop: str,
    ) -> tuple[int, set[str]]:
        """Return record count and historical units for one exact interval."""

    def count_records(self, entity_id: str, *, start: str, stop: str) -> int:
        """Return record count for one exact interval."""
        records, _ = self.count_records_and_units(
            entity_id,
            start=start,
            stop=stop,
        )
        return records

    @abstractmethod
    def count_measurements(
        self,
        entity_id: str,
        *,
        start: str,
        stop: str,
    ) -> int:
        """Return the number of distinct Home Assistant state measurements."""

    @abstractmethod
    def count_all_history(self, entity_id: str) -> int:
        """Return all historical records currently stored for an entity."""

    @abstractmethod
    def history_time_bounds(
        self, entity_id: str
    ) -> tuple[str | None, str | None]:
        """Return the first and last sample timestamps for the full entity history.

        The timestamps describe all history currently stored for the physical
        entity ID, independently of the time windows involved in a migration.
        Returned values are ISO-8601 UTC strings, or ``None`` when no history
        exists.
        """

    @abstractmethod
    def copy_records(
        self,
        source_entity_id: str,
        target_entity_id: str,
        *,
        start: str,
        stop: str,
    ) -> None:
        """Copy one exact historical interval to another entity ID.

        Implementations must make this operation idempotent for the same
        source, target and time range because the engine may repeat COPY after
        an interrupted migration.
        """

    @abstractmethod
    def delete_records(
        self,
        entity_id: str,
        *,
        start: str,
        stop: str,
    ) -> None:
        """Delete one exact historical interval for an entity ID."""

    @abstractmethod
    def list_ledger_entries(self, registry_entry_id: str) -> list[LedgerEntry]:
        """Return append-only migration metadata for one registry identity."""

    @abstractmethod
    def append_ledger_entry(self, entry: LedgerEntry) -> None:
        """Append one migration metadata record idempotently."""
