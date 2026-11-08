"""Persistent processing and audit journal."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import logging
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

from ..const import (
    JOURNAL_SCHEMA,
    JOURNAL_STORAGE_KEY,
    JOURNAL_STORAGE_VERSION,
    RETRY_BASE_SECONDS,
    RETRY_MAX_SECONDS,
    STATUS_ANALYZING,
    STATUS_APPROVED_CUSTOM_TARGET,
    STATUS_APPROVED_DELETE,
    STATUS_APPROVED_MERGE,
    STATUS_CONFLICT,
    STATUS_FAILED,
    STATUS_COMPLETED,
    STATUS_MIGRATING,
    STATUS_NO_SOURCE,
    STATUS_IGNORED,
    STATUS_PENDING,
    STATUS_READY,
    STATUS_RETRY,
    STATUS_SUPERSEDED,
)
from .timeline import EntityTimeline, TimelineEvent

if TYPE_CHECKING:
    from .ledger import DeletionSegment

_LOGGER = logging.getLogger(__name__)


class JournalEntry(TypedDict):
    """Processing state for one immutable timeline revision."""

    timeline_event_id: str
    registry_entry_id: str
    rename_timestamp: str
    old_entity_id: str
    new_entity_id: str
    status: str
    attempts: int
    last_error: str | None

    next_retry_at: NotRequired[str | None]
    analyzed_at: NotRequired[str]
    duration_seconds: NotRequired[float]
    window_count: NotRequired[int]
    ready_windows: NotRequired[int]
    no_source_windows: NotRequired[int]
    conflict_windows: NotRequired[int]
    source_records: NotRequired[int]
    unknown_target_records: NotRequired[int]
    source_measurements: NotRequired[int]
    unknown_target_measurements: NotRequired[int]
    resolved_at: NotRequired[str]
    source_units: NotRequired[list[str]]
    target_units: NotRequired[list[str]]
    current_unit: NotRequired[str | None]
    current_unit_known: NotRequired[bool]
    merge_allowed: NotRequired[bool]
    merge_block_reason: NotRequired[str | None]
    custom_target_entity_id: NotRequired[str]
    write_attempts: NotRequired[int]
    write_expected_records: NotRequired[int]
    write_started_at: NotRequired[str]
    write_completed_at: NotRequired[str]
    write_operation: NotRequired[str]
    write_expected_target_records: NotRequired[int]
    analysis_windows: NotRequired[list[dict[str, Any]]]


class DeletionRequest(TypedDict):
    """A persisted decision for one deletion event and history store."""

    event_id: str
    registry_entry_id: str
    storage_identity: str
    decision: Literal["delete", "keep"]
    approved: list[DeletionSegment]
    status: Literal["queued", "review", "failed"]


class ProcessingJournal:
    """Mutable processing state linked to immutable timeline event IDs."""

    def __init__(self, hass: HomeAssistant) -> None:
        self._store = Store[dict[str, Any]](
            hass,
            JOURNAL_STORAGE_VERSION,
            JOURNAL_STORAGE_KEY,
        )
        self._entries: list[JournalEntry] = []
        self._deletion_requests: list[DeletionRequest] = []
        self._ledger_model_version = 1
        self._measurement_count_model_version = 1
        self._lock = asyncio.Lock()

    async def async_load(self) -> None:
        """Load journal and reject unknown storage schemas safely."""
        data = await self._store.async_load()

        if data is None:
            self._entries = []
            self._ledger_model_version = 1
            self._measurement_count_model_version = 1
            return

        if not isinstance(data, dict) or data.get("schema") != JOURNAL_SCHEMA:
            _LOGGER.warning(
                "Ignoring unknown Entity History Migrator journal schema; "
                "processing state will be rebuilt from the append-only timeline"
            )
            self._entries = []
            return

        raw_entries = data.get("entries", [])
        if not isinstance(raw_entries, list):
            _LOGGER.error("Invalid processing journal entries; rebuilding from timeline")
            self._entries = []
            return

        self._entries = raw_entries
        self._deletion_requests = data.get("deletion_requests", [])
        self._ledger_model_version = int(data.get("ledger_model_version", 0))
        self._measurement_count_model_version = int(
            data.get("measurement_count_model_version", 0)
        )

    async def async_reconcile(self, timeline: EntityTimeline) -> None:
        """Reconcile processing state with the immutable timeline after startup."""
        async with self._lock:
            changed = False
            existing_ids = {entry["timeline_event_id"] for entry in self._entries}
            grouped = timeline.rename_grouped()

            # 0.6.2 introduced a database-side physical-location ledger. A
            # current no_source result produced by older versions may be wrong
            # after a previous migration moved records under another alias.
            if self._ledger_model_version < 1:
                for registry_entry_id, events in grouped.items():
                    latest_id = events[-1]["id"]
                    for entry in self._entries:
                        if (
                            entry["registry_entry_id"] == registry_entry_id
                            and entry["timeline_event_id"] == latest_id
                            and entry["status"]
                            in {
                                STATUS_NO_SOURCE,
                                STATUS_CONFLICT,
                                STATUS_READY,
                                STATUS_APPROVED_MERGE,
                                STATUS_APPROVED_DELETE,
                                STATUS_APPROVED_CUSTOM_TARGET,
                            }
                        ):
                            entry["status"] = STATUS_PENDING
                            entry["last_error"] = None
                            entry["next_retry_at"] = None
                            entry.pop("resolved_at", None)
                            entry.pop("custom_target_entity_id", None)
                            changed = True
                self._ledger_model_version = 1
                changed = True

            # 0.6.4 fixes the user-facing measurement counter introduced in
            # 0.6.3. Re-analyze unresolved current conflicts once so persisted
            # zero/incorrect counts are replaced by real HA state-measurement
            # counts. Write verification remains record-count based.
            if self._measurement_count_model_version < 1:
                for registry_entry_id, events in grouped.items():
                    latest_id = events[-1]["id"]
                    for entry in self._entries:
                        if (
                            entry["registry_entry_id"] == registry_entry_id
                            and entry["timeline_event_id"] == latest_id
                            and entry["status"] == STATUS_CONFLICT
                        ):
                            entry["status"] = STATUS_PENDING
                            entry["last_error"] = None
                            entry["next_retry_at"] = None
                            entry.pop("resolved_at", None)
                            changed = True
                self._measurement_count_model_version = 1
                changed = True

            # Add missing audit rows. Old revisions with no persisted processing
            # state are already obsolete; only each registry's latest revision
            # needs analysis.
            for registry_entry_id, events in grouped.items():
                latest_id = events[-1]["id"]
                for event in events:
                    if event["id"] in existing_ids:
                        continue
                    status = (
                        STATUS_PENDING if event["id"] == latest_id else STATUS_SUPERSEDED
                    )
                    self._entries.append(self._new_entry(event, status=status))
                    existing_ids.add(event["id"])
                    changed = True

            # Repair active states after a crash and ensure only current
            # revisions may remain active.
            for registry_entry_id, events in grouped.items():
                latest_id = events[-1]["id"]
                for entry in self._entries:
                    if entry["registry_entry_id"] != registry_entry_id:
                        continue

                    if entry["status"] == "approved_replace":
                        entry["status"] = STATUS_APPROVED_DELETE
                        changed = True

                    if entry["timeline_event_id"] != latest_id:
                        if entry["status"] in {
                            STATUS_PENDING,
                            STATUS_ANALYZING,
                            STATUS_MIGRATING,
                            STATUS_APPROVED_CUSTOM_TARGET,
                            STATUS_APPROVED_DELETE,
                            STATUS_APPROVED_MERGE,
                            STATUS_RETRY,
                            STATUS_CONFLICT,
                        }:
                            entry["status"] = STATUS_SUPERSEDED
                            entry["next_retry_at"] = None
                            changed = True
                        continue

                    if entry["status"] == STATUS_ANALYZING:
                        entry["status"] = STATUS_PENDING
                        entry["last_error"] = (
                            "Recovered after Home Assistant restart while analyzing"
                        )
                        entry["next_retry_at"] = None
                        changed = True

                    if entry["status"] == STATUS_MIGRATING:
                        # Do not guess which write step completed before a crash.
                        # Restore the approved operation and let the writer infer
                        # progress from verified source/target counts in the DB.
                        operation = entry.get("write_operation")
                        if operation == "merge":
                            entry["status"] = STATUS_APPROVED_MERGE
                        elif operation == "delete":
                            entry["status"] = STATUS_APPROVED_DELETE
                        elif operation == "custom_target":
                            entry["status"] = STATUS_APPROVED_CUSTOM_TARGET
                        else:
                            entry["status"] = STATUS_READY
                        entry["last_error"] = (
                            "Recovered after Home Assistant restart while migrating"
                        )
                        entry["next_retry_at"] = None
                        changed = True

                    # 0.4.7 adds historical unit metadata. Re-analyze unresolved
                    # conflicts, and old Merge approvals, so unit safety is based
                    # on real Influx measurements rather than a stale decision.
                    if (
                        entry["status"] in {STATUS_CONFLICT, STATUS_APPROVED_MERGE}
                        and (
                            "source_units" not in entry
                            or "target_units" not in entry
                            or "current_unit_known" not in entry
                            or "merge_allowed" not in entry
                        )
                    ):
                        entry["status"] = STATUS_PENDING
                        entry["last_error"] = None
                        entry["next_retry_at"] = None
                        entry.pop("resolved_at", None)
                        changed = True

                    # 0.6.3 adds user-facing measurement counts. Re-analyze
                    # unresolved conflicts once so Repairs never labels Flux
                    # field-record counts as measurements.
                    if (
                        entry["status"] == STATUS_CONFLICT
                        and (
                            "source_measurements" not in entry
                            or "unknown_target_measurements" not in entry
                        )
                    ):
                        entry["status"] = STATUS_PENDING
                        entry["last_error"] = None
                        entry["next_retry_at"] = None
                        entry.pop("resolved_at", None)
                        changed = True

            if changed:
                await self._async_save_locked()

    async def async_register_rename_revision(self, event: TimelineEvent) -> JournalEntry:
        """Create pending state for a new revision and supersede older active work."""
        async with self._lock:
            for entry in self._entries:
                if (
                    entry["registry_entry_id"] == event["registry_entry_id"]
                    and entry["timeline_event_id"] != event["id"]
                    and entry["status"]
                    in {
                        STATUS_PENDING,
                        STATUS_ANALYZING,
                        STATUS_MIGRATING,
                        STATUS_APPROVED_CUSTOM_TARGET,
                        STATUS_APPROVED_DELETE,
                        STATUS_APPROVED_MERGE,
                        STATUS_RETRY,
                        STATUS_CONFLICT,
                    }
                ):
                    entry["status"] = STATUS_SUPERSEDED
                    entry["next_retry_at"] = None

            new_entry = self._new_entry(event, status=STATUS_PENDING)
            self._entries.append(new_entry)
            await self._async_save_locked()
            return dict(new_entry)

    async def async_claim_next(
        self,
        settling_delay_seconds: int,
        timeline: EntityTimeline,
    ) -> JournalEntry | None:
        """Atomically claim the oldest eligible current revision."""
        now = datetime.now(UTC)
        eligible_before = now - timedelta(seconds=settling_delay_seconds)

        async with self._lock:
            candidates: list[JournalEntry] = []

            for entry in self._entries:
                if (
                    timeline.latest_rename_revision(entry["registry_entry_id"])
                    != entry["timeline_event_id"]
                ):
                    continue

                if entry["status"] not in {STATUS_PENDING, STATUS_RETRY}:
                    continue

                if datetime.fromisoformat(entry["rename_timestamp"]) > eligible_before:
                    continue

                if entry["status"] == STATUS_RETRY:
                    retry_at = entry.get("next_retry_at")
                    if retry_at and datetime.fromisoformat(retry_at) > now:
                        continue

                candidates.append(entry)

            if not candidates:
                return None

            entry = min(candidates, key=lambda item: item["rename_timestamp"])
            entry["status"] = STATUS_ANALYZING
            entry["attempts"] += 1
            entry["last_error"] = None
            entry["next_retry_at"] = None
            await self._async_save_locked()
            return dict(entry)

    async def async_claim_ready_for_write(
        self,
        timeline: EntityTimeline,
    ) -> JournalEntry | None:
        """Atomically claim the oldest current ready revision for writing."""
        async with self._lock:
            candidates = [
                entry
                for entry in self._entries
                if entry["status"] == STATUS_READY
                and int(entry.get("source_records", 0)) > 0
                and timeline.latest_rename_revision(entry["registry_entry_id"])
                == entry["timeline_event_id"]
            ]

            if not candidates:
                return None

            entry = min(candidates, key=lambda item: item["rename_timestamp"])
            entry["status"] = STATUS_MIGRATING
            entry["write_operation"] = "ready"
            entry["write_attempts"] = int(entry.get("write_attempts", 0)) + 1
            entry.setdefault(
                "write_expected_records",
                int(entry.get("source_records", 0)),
            )
            entry.setdefault("write_started_at", datetime.now(UTC).isoformat())
            entry["last_error"] = None
            entry["next_retry_at"] = None
            await self._async_save_locked()
            return dict(entry)

    async def async_claim_merge_for_write(
        self,
        timeline: EntityTimeline,
    ) -> JournalEntry | None:
        """Atomically claim the oldest current approved Merge revision."""
        async with self._lock:
            candidates = [
                entry
                for entry in self._entries
                if entry["status"] == STATUS_APPROVED_MERGE
                and bool(entry.get("merge_allowed", False))
                and int(entry.get("source_records", 0)) > 0
                and timeline.latest_rename_revision(entry["registry_entry_id"])
                == entry["timeline_event_id"]
            ]

            if not candidates:
                return None

            entry = min(candidates, key=lambda item: item["rename_timestamp"])
            entry["status"] = STATUS_MIGRATING
            entry["write_operation"] = "merge"
            entry["write_attempts"] = int(entry.get("write_attempts", 0)) + 1
            entry.setdefault(
                "write_expected_records",
                int(entry.get("source_records", 0)),
            )
            entry.setdefault("write_started_at", datetime.now(UTC).isoformat())
            entry["last_error"] = None
            entry["next_retry_at"] = None
            await self._async_save_locked()
            return dict(entry)

    async def async_claim_delete_for_write(
        self,
        timeline: EntityTimeline,
    ) -> JournalEntry | None:
        """Atomically claim the oldest current approved Delete revision."""
        async with self._lock:
            candidates = [
                entry
                for entry in self._entries
                if entry["status"] == STATUS_APPROVED_DELETE
                and int(entry.get("unknown_target_records", 0)) > 0
                and timeline.latest_rename_revision(entry["registry_entry_id"])
                == entry["timeline_event_id"]
            ]

            if not candidates:
                return None

            entry = min(candidates, key=lambda item: item["rename_timestamp"])
            entry["status"] = STATUS_MIGRATING
            entry["write_operation"] = "delete"
            entry["write_attempts"] = int(entry.get("write_attempts", 0)) + 1
            entry.setdefault(
                "write_expected_records",
                int(entry.get("source_records", 0)),
            )
            entry.setdefault(
                "write_expected_target_records",
                int(entry.get("unknown_target_records", 0)),
            )
            entry.setdefault("write_started_at", datetime.now(UTC).isoformat())
            entry["last_error"] = None
            entry["next_retry_at"] = None
            await self._async_save_locked()
            return dict(entry)

    async def async_claim_custom_target_for_write(
        self,
        timeline: EntityTimeline,
    ) -> JournalEntry | None:
        """Atomically claim the oldest current approved archive-target revision."""
        async with self._lock:
            candidates = [
                entry
                for entry in self._entries
                if entry["status"] == STATUS_APPROVED_CUSTOM_TARGET
                and int(entry.get("unknown_target_records", 0)) > 0
                and bool(entry.get("custom_target_entity_id"))
                and timeline.latest_rename_revision(entry["registry_entry_id"])
                == entry["timeline_event_id"]
            ]

            if not candidates:
                return None

            entry = min(candidates, key=lambda item: item["rename_timestamp"])
            entry["status"] = STATUS_MIGRATING
            entry["write_operation"] = "custom_target"
            entry["write_attempts"] = int(entry.get("write_attempts", 0)) + 1
            entry.setdefault(
                "write_expected_records",
                int(entry.get("source_records", 0)),
            )
            entry.setdefault(
                "write_expected_target_records",
                int(entry.get("unknown_target_records", 0)),
            )
            entry.setdefault("write_started_at", datetime.now(UTC).isoformat())
            entry["last_error"] = None
            entry["next_retry_at"] = None
            await self._async_save_locked()
            return dict(entry)

    async def async_mark_write_completed(
        self,
        timeline_event_id: str,
        timeline: EntityTimeline,
    ) -> bool:
        """Persist successful completion only if the revision is still current."""
        async with self._lock:
            entry = self._find(timeline_event_id)
            if (
                timeline.latest_rename_revision(entry["registry_entry_id"])
                != timeline_event_id
            ):
                entry["status"] = STATUS_SUPERSEDED
                entry["last_error"] = (
                    "Write completed after a newer rename revision appeared"
                )
                await self._async_save_locked()
                return False

            entry["status"] = STATUS_COMPLETED
            entry["last_error"] = None
            entry["next_retry_at"] = None
            entry["write_completed_at"] = datetime.now(UTC).isoformat()
            await self._async_save_locked()
            return True

    async def async_mark_write_failed(
        self,
        timeline_event_id: str,
        error: str,
    ) -> None:
        """Persist a write failure without automatic retry."""
        async with self._lock:
            entry = self._find(timeline_event_id)
            if entry["status"] == STATUS_SUPERSEDED:
                return
            entry["status"] = STATUS_FAILED
            entry["last_error"] = f"Write failed: {error}"
            entry["next_retry_at"] = None
            await self._async_save_locked()

    async def async_finish(
        self,
        timeline_event_id: str,
        *,
        status: str,
        analyzed_at: str,
        duration_seconds: float,
        window_count: int,
        ready_windows: int,
        no_source_windows: int,
        conflict_windows: int,
        source_records: int,
        unknown_target_records: int,
        source_measurements: int,
        unknown_target_measurements: int,
        source_units: list[str],
        target_units: list[str],
        current_unit: str | None,
        current_unit_known: bool,
        merge_allowed: bool,
        merge_block_reason: str | None,
        windows: list[dict[str, Any]],
    ) -> bool:
        """Persist analysis unless the revision became obsolete meanwhile."""
        async with self._lock:
            entry = self._find(timeline_event_id)

            if entry["status"] == STATUS_SUPERSEDED:
                return False

            entry["status"] = status
            entry["last_error"] = None
            entry["next_retry_at"] = None
            entry["analyzed_at"] = analyzed_at
            entry["duration_seconds"] = round(duration_seconds, 3)
            entry["window_count"] = window_count
            entry["ready_windows"] = ready_windows
            entry["no_source_windows"] = no_source_windows
            entry["conflict_windows"] = conflict_windows
            entry["source_records"] = source_records
            entry["unknown_target_records"] = unknown_target_records
            entry["source_measurements"] = source_measurements
            entry["unknown_target_measurements"] = unknown_target_measurements
            entry["source_units"] = sorted(set(source_units))
            entry["target_units"] = sorted(set(target_units))
            entry["current_unit"] = current_unit
            entry["current_unit_known"] = current_unit_known
            entry["merge_allowed"] = merge_allowed
            entry["merge_block_reason"] = merge_block_reason
            entry["analysis_windows"] = [
                {
                    "entity_id": str(window["entity_id"]),
                    "logical_entity_id": str(
                        window.get("logical_entity_id", window["entity_id"])
                    ),
                    "start": str(window["start"]),
                    "stop": str(window["stop"]),
                    "source_records": int(window["source_records"]),
                    "unknown_target_records": int(window["unknown_target_records"]),
                    "source_measurements": int(window.get("source_measurements", 0)),
                    "unknown_target_measurements": int(
                        window.get("unknown_target_measurements", 0)
                    ),
                }
                for window in windows
            ]
            entry.pop("custom_target_entity_id", None)
            await self._async_save_locked()
            return True

    async def async_mark_superseded(self, timeline_event_id: str) -> None:
        """Mark an obsolete revision as superseded."""
        async with self._lock:
            entry = self._find(timeline_event_id)
            entry["status"] = STATUS_SUPERSEDED
            entry["next_retry_at"] = None
            await self._async_save_locked()

    async def async_close_registry_entry(self, registry_entry_id: str) -> None:
        """Supersede unfinished rename work after the registry entry is deleted."""
        active = {
            STATUS_PENDING,
            STATUS_ANALYZING,
            STATUS_MIGRATING,
            STATUS_APPROVED_CUSTOM_TARGET,
            STATUS_APPROVED_DELETE,
            STATUS_APPROVED_MERGE,
            STATUS_RETRY,
            STATUS_CONFLICT,
            STATUS_READY,
        }
        async with self._lock:
            changed = False
            for entry in self._entries:
                if (
                    entry["registry_entry_id"] == registry_entry_id
                    and entry["status"] in active
                ):
                    entry["status"] = STATUS_SUPERSEDED
                    entry["next_retry_at"] = None
                    changed = True
            if changed:
                await self._async_save_locked()

    async def async_mark_retry(
        self,
        timeline_event_id: str,
        error: str,
    ) -> tuple[int, str]:
        """Schedule a transient failure using bounded exponential backoff."""
        async with self._lock:
            entry = self._find(timeline_event_id)

            if entry["status"] == STATUS_SUPERSEDED:
                return 0, datetime.now(UTC).isoformat()

            # attempts increments when claimed: 1=>30s, 2=>60s, 3=>120s...
            exponent = max(0, entry["attempts"] - 1)
            delay = min(RETRY_BASE_SECONDS * (2**exponent), RETRY_MAX_SECONDS)
            retry_at = datetime.now(UTC) + timedelta(seconds=delay)

            entry["status"] = STATUS_RETRY
            entry["last_error"] = error
            entry["next_retry_at"] = retry_at.isoformat()
            await self._async_save_locked()
            return delay, retry_at.isoformat()

    async def async_mark_failed(self, timeline_event_id: str, error: str) -> None:
        """Persist a permanent processing failure."""
        async with self._lock:
            entry = self._find(timeline_event_id)
            entry["status"] = STATUS_FAILED
            entry["last_error"] = error
            entry["next_retry_at"] = None
            await self._async_save_locked()


    async def async_set_resolutions(
        self,
        timeline_event_ids: list[str],
        resolution: str,
        timeline: EntityTimeline,
        *,
        custom_target_entity_id: str | None = None,
    ) -> int:
        """Resolve current conflict revisions in one atomic journal save."""
        if resolution not in {
            STATUS_APPROVED_CUSTOM_TARGET,
            STATUS_APPROVED_DELETE,
            STATUS_APPROVED_MERGE,
            STATUS_IGNORED,
        }:
            raise ValueError(f"Unsupported conflict resolution: {resolution}")

        if (
            resolution == STATUS_APPROVED_CUSTOM_TARGET
            and not custom_target_entity_id
        ):
            raise ValueError("Custom target resolution requires an entity_id")

        changed = 0

        async with self._lock:
            for timeline_event_id in timeline_event_ids:
                entry = self._find(timeline_event_id)

                # A Repairs flow can remain open while another rename happens.
                # Never approve a stale historical revision.
                if (
                    entry["status"] != STATUS_CONFLICT
                    or timeline.latest_rename_revision(entry["registry_entry_id"])
                    != timeline_event_id
                ):
                    continue

                entry["status"] = resolution
                entry["last_error"] = None
                entry["next_retry_at"] = None
                entry["resolved_at"] = datetime.now(UTC).isoformat()
                if resolution == STATUS_APPROVED_CUSTOM_TARGET:
                    entry["custom_target_entity_id"] = custom_target_entity_id  # type: ignore[assignment]
                else:
                    entry.pop("custom_target_entity_id", None)
                changed += 1

            if changed:
                await self._async_save_locked()

        return changed

    def entries(self) -> tuple[JournalEntry, ...]:
        """Return detached audit history."""
        return tuple(dict(entry) for entry in self._entries)

    def current_entries(self, timeline: EntityTimeline) -> tuple[JournalEntry, ...]:
        """Return only latest revision entries for each registry entity."""
        return tuple(
            dict(entry)
            for entry in self._entries
            if timeline.latest_rename_revision(entry["registry_entry_id"])
            == entry["timeline_event_id"]
        )

    def summary(self, timeline: EntityTimeline) -> dict[str, int]:
        """Return HA diagnostic counters for current revisions only."""
        result = {
            "queued": 0,
            "ready": 0,
            "conflicts": 0,
            "approved": 0,
            "ignored": 0,
            "no_source": 0,
            "failed": 0,
        }

        for entry in self.current_entries(timeline):
            status = entry["status"]

            if status in {STATUS_PENDING, STATUS_ANALYZING, STATUS_MIGRATING, STATUS_RETRY, STATUS_CONFLICT}:
                result["queued"] += 1

            if status == STATUS_CONFLICT:
                result["conflicts"] += 1
            elif status == STATUS_READY:
                result["ready"] += int(entry.get("ready_windows", 0))
                result["no_source"] += int(entry.get("no_source_windows", 0))
            elif status in {
                STATUS_APPROVED_CUSTOM_TARGET,
                STATUS_APPROVED_DELETE,
                STATUS_APPROVED_MERGE,
            }:
                result["approved"] += 1
            elif status == STATUS_IGNORED:
                result["ignored"] += 1
            elif status == STATUS_NO_SOURCE:
                result["no_source"] += max(
                    1,
                    int(entry.get("no_source_windows", 0)),
                )
            elif status == STATUS_FAILED:
                result["failed"] += 1

        return result

    def current_conflicts(
        self,
        timeline: EntityTimeline,
    ) -> tuple[JournalEntry, ...]:
        """Return unresolved current conflict revisions."""
        return tuple(
            entry
            for entry in self.current_entries(timeline)
            if entry["status"] == STATUS_CONFLICT
        )

    def deletion_request(
        self, storage_identity: str, event_id: str,
    ) -> DeletionRequest | None:
        return next(
            (request for request in self._deletion_requests
             if request["storage_identity"] == storage_identity
             and request["event_id"] == event_id),
            None,
        )

    def next_deletion_request(self, storage_identity: str) -> DeletionRequest | None:
        return next(
            (request for request in self._deletion_requests
             if request["storage_identity"] == storage_identity
             and request["status"] == "queued"),
            None,
        )

    async def async_enqueue_deletion_request(self, request: DeletionRequest) -> bool:
        """Accept at most one outstanding decision per deletion and store."""
        async with self._lock:
            previous = self.deletion_request(request["storage_identity"], request["event_id"])
            if previous is not None and previous["status"] == "queued":
                return False
            await self._async_replace_deletion_request(previous, request)
            return True

    async def async_finish_deletion_request(
        self,
        request: DeletionRequest,
        status: Literal["review", "failed"] | None = None,
    ) -> None:
        async with self._lock:
            replacement = None if status is None else {**request, "status": status}
            await self._async_replace_deletion_request(request, replacement)

    async def _async_replace_deletion_request(
        self,
        previous: DeletionRequest | None,
        replacement: DeletionRequest | None,
    ) -> None:
        requests = [
            request for request in self._deletion_requests if request is not previous
        ]
        if replacement is not None:
            requests.append(replacement)
        await self._async_save_locked(deletion_requests=requests)
        self._deletion_requests = requests

    @staticmethod
    def _new_entry(event: TimelineEvent, *, status: str) -> JournalEntry:
        return {
            "timeline_event_id": event["id"],
            "registry_entry_id": event["registry_entry_id"],
            "rename_timestamp": event["event_timestamp"],
            "old_entity_id": event["previous_entity_id"],
            "new_entity_id": event["entity_id"],
            "status": status,
            "attempts": 0,
            "last_error": None,
            "next_retry_at": None,
        }

    def _find(self, timeline_event_id: str) -> JournalEntry:
        for entry in self._entries:
            if entry["timeline_event_id"] == timeline_event_id:
                return entry
        raise KeyError(timeline_event_id)

    async def _async_save_locked(
        self, *, deletion_requests: list[DeletionRequest] | None = None,
    ) -> None:
        await self._store.async_save(
            {
                "schema": JOURNAL_SCHEMA,
                "ledger_model_version": self._ledger_model_version,
                "measurement_count_model_version": self._measurement_count_model_version,
                "entries": self._entries,
                "deletion_requests": (
                    self._deletion_requests if deletion_requests is None else deletion_requests
                ),
            }
        )
