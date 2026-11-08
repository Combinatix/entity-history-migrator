"""Persistent database-side migration ledger helpers."""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
from typing import TYPE_CHECKING, Iterable, TypedDict

from ..backends.base import HistoryBackend, LedgerEntry
from .ownership import OwnershipTimelineError, closed_ownership_intervals

if TYPE_CHECKING:
    from .journal import JournalEntry
    from .timeline import TimelineEvent

EPOCH = "1970-01-01T00:00:00.000000Z"
_TRACKED_KINDS = {
    "baseline",
    "observed",
    "ready",
    "merge",
    "delete",
    "custom_target",
    "deleted",
    "deleted_kept",
    "deleted_removed",
}


class DeletionSegment(TypedDict):
    """Physical interval and counts shown when Delete is approved."""

    logical_entity_id: str
    physical_entity_id: str
    start: str
    stop: str
    record_count: int
    measurement_count: int


class DeletionPlanChanged(RuntimeError):
    """The approved deletion no longer matches current history."""


def _segment_key(segment: DeletionSegment | LedgerEntry) -> tuple[str, str, str, str]:
    return (
        segment["start"],
        segment["stop"],
        segment["logical_entity_id"],
        segment["physical_entity_id"],
    )


def _record_id(
    registry_entry_id: str,
    kind: str,
    operation_id: str,
    start: str,
    stop: str,
    logical_entity_id: str,
    physical_entity_id: str,
) -> str:
    """Return a deterministic ID so retries cannot create duplicate metadata."""
    raw = "|".join(
        (
            registry_entry_id,
            kind,
            operation_id,
            start,
            stop,
            logical_entity_id,
            physical_entity_id,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def make_ledger_entry(
    *,
    registry_entry_id: str,
    timeline_event_id: str,
    operation_id: str,
    kind: str,
    logical_entity_id: str,
    physical_entity_id: str,
    start: str,
    stop: str,
    record_count: int,
    created_at: str | None = None,
) -> LedgerEntry:
    """Build one deterministic append-only ledger record."""
    created = created_at or datetime.now(UTC).isoformat()
    return {
        "record_id": _record_id(
            registry_entry_id,
            kind,
            operation_id,
            start,
            stop,
            logical_entity_id,
            physical_entity_id,
        ),
        "registry_entry_id": registry_entry_id,
        "timeline_event_id": timeline_event_id,
        "operation_id": operation_id,
        "kind": kind,
        "logical_entity_id": logical_entity_id,
        "physical_entity_id": physical_entity_id,
        "start": start,
        "stop": stop,
        "record_count": int(record_count),
        "created_at": created,
    }


def latest_segment_states(entries: Iterable[LedgerEntry]) -> dict[tuple[str, str], LedgerEntry]:
    """Return the latest physical location for every logical time segment.

    Timeline answers *which alias owned this interval*; the ledger answers
    *where those trusted records physically live now*. Keeping these separate is
    necessary because migration changes tags but preserves original timestamps.
    """
    states: dict[tuple[str, str], LedgerEntry] = {}
    for entry in sorted(entries, key=lambda item: item["created_at"]):
        if entry["kind"] not in _TRACKED_KINDS:
            continue
        key = (entry["start"], entry["stop"])
        previous = states.get(key)
        # An observed interval or journal replay cannot revive removed history.
        if previous is None or previous["kind"] != "deleted_removed":
            states[key] = entry
    return states


def append_segment_state(
    backend: HistoryBackend,
    *,
    registry_entry_id: str,
    timeline_event_id: str,
    operation_id: str,
    kind: str,
    logical_entity_id: str,
    physical_entity_id: str,
    start: str,
    stop: str,
    record_count: int,
    created_at: str | None = None,
) -> None:
    """Append one current tracked-location state."""
    backend.append_ledger_entry(
        make_ledger_entry(
            registry_entry_id=registry_entry_id,
            timeline_event_id=timeline_event_id,
            operation_id=operation_id,
            kind=kind,
            logical_entity_id=logical_entity_id,
            physical_entity_id=physical_entity_id,
            start=start,
            stop=stop,
            record_count=record_count,
            created_at=created_at,
        )
    )


def append_archive_record(
    backend: HistoryBackend,
    *,
    registry_entry_id: str,
    timeline_event_id: str,
    operation_id: str,
    logical_entity_id: str,
    archive_entity_id: str,
    start: str,
    stop: str,
    record_count: int,
    created_at: str | None = None,
) -> None:
    """Audit a conflict archive without treating it as tracked entity history."""
    backend.append_ledger_entry(
        make_ledger_entry(
            registry_entry_id=registry_entry_id,
            timeline_event_id=timeline_event_id,
            operation_id=operation_id,
            kind="archive",
            logical_entity_id=logical_entity_id,
            physical_entity_id=archive_entity_id,
            start=start,
            stop=stop,
            record_count=record_count,
            created_at=created_at,
        )
    )


def ensure_deletion_ledger_entry(
    backend: HistoryBackend,
    event: TimelineEvent,
) -> LedgerEntry | None:
    """Persist the final trusted interval of one deleted registry entity.

    Deletion never changes history. The ledger row only records that this exact
    alias/time interval belonged to the stable registry identity, which allows
    later orphan analysis to distinguish owned history from same-name leftovers.
    """
    if event.get("event_type") != "delete":
        return None

    registry_entry_id = str(event["registry_entry_id"])
    event_id = str(event["id"])
    existing = backend.list_ledger_entries(registry_entry_id)
    for item in existing:
        if item["kind"] == "deleted" and item["timeline_event_id"] == event_id:
            return item

    entity_id = str(event["entity_id"])
    start = str(event.get("ownership_start", ""))
    stop = str(event["event_timestamp"])
    if not start:
        raise ValueError("Deletion timeline event has no ownership_start")
    if datetime.fromisoformat(start) > datetime.fromisoformat(stop):
        raise ValueError("Deletion ownership interval starts after deletion timestamp")

    physical_state = latest_segment_states(existing).get((start, stop))
    physical_entity_id = (
        str(physical_state["physical_entity_id"])
        if physical_state is not None
        else entity_id
    )
    logical_entity_id = (
        str(physical_state["logical_entity_id"])
        if physical_state is not None
        else entity_id
    )
    record_count = (
        0
        if start == stop
        else backend.count_records(physical_entity_id, start=start, stop=stop)
    )
    entry = make_ledger_entry(
        registry_entry_id=registry_entry_id,
        timeline_event_id=event_id,
        operation_id=f"deleted:{event_id}",
        kind="deleted",
        logical_entity_id=logical_entity_id,
        physical_entity_id=physical_entity_id,
        start=start,
        stop=stop,
        record_count=record_count,
        created_at=stop,
    )
    backend.append_ledger_entry(entry)
    return entry



_DELETION_DECISION_KINDS = {
    "deleted_keep_complete": "keep_complete",
    "deleted_delete_complete": "delete_complete",
}
_DELETION_PENDING_KINDS = {
    "deleted_keep_pending": "keep_pending",
    "deleted_delete_pending": "delete_pending",
}


def deletion_decision_state(
    backend: HistoryBackend,
    event: TimelineEvent,
) -> str | None:
    """Return completed or interrupted user-decision state for this deletion."""
    event_id = str(event["id"])
    entries = backend.list_ledger_entries(str(event["registry_entry_id"]))
    for item in entries:
        if item["timeline_event_id"] == event_id and item["kind"] in _DELETION_DECISION_KINDS:
            return _DELETION_DECISION_KINDS[item["kind"]]
    for item in entries:
        if item["timeline_event_id"] == event_id and item["kind"] in _DELETION_PENDING_KINDS:
            return _DELETION_PENDING_KINDS[item["kind"]]
    return None


def has_deletion_decision(backend: HistoryBackend, event: TimelineEvent) -> bool:
    """Return whether a complete user decision was persisted for this deletion."""
    return deletion_decision_state(backend, event) in {"keep_complete", "delete_complete"}


def _pending_deletion_record_count(
    backend: HistoryBackend,
    event: TimelineEvent,
    fallback: int,
) -> int:
    """Keep the originally approved count when resuming a partial operation."""
    event_id = str(event["id"])
    for item in backend.list_ledger_entries(str(event["registry_entry_id"])):
        if (
            item["timeline_event_id"] == event_id
            and item["kind"] in _DELETION_PENDING_KINDS
        ):
            return int(item["record_count"])
    return fallback


def _append_deletion_intent(
    backend: HistoryBackend,
    event: TimelineEvent,
    *,
    decision: str,
    record_count: int,
) -> None:
    """Persist the selected action before any per-segment work begins."""
    kind = "deleted_keep_pending" if decision == "keep" else "deleted_delete_pending"
    event_id = str(event["id"])
    backend.append_ledger_entry(
        make_ledger_entry(
            registry_entry_id=str(event["registry_entry_id"]),
            timeline_event_id=event_id,
            operation_id=f"{kind}:{event_id}",
            kind=kind,
            logical_entity_id=str(event["entity_id"]),
            physical_entity_id=str(event["entity_id"]),
            start=str(event.get("ownership_start", EPOCH)),
            stop=str(event["event_timestamp"]),
            record_count=record_count,
        )
    )


def deletion_history_segments(
    backend: HistoryBackend,
    event: TimelineEvent,
) -> list[DeletionSegment]:
    """Return the current physical locations of proven history for a deletion."""
    entries = backend.list_ledger_entries(str(event["registry_entry_id"]))
    states = latest_segment_states(entries)
    result: list[DeletionSegment] = []
    deletion_stop = str(event["event_timestamp"])
    for state in sorted(states.values(), key=lambda item: (item["start"], item["stop"])):
        physical_entity_id = str(state["physical_entity_id"])
        start = str(state["start"])
        stop = str(state["stop"])
        # A later restore can create new owned intervals for the same registry
        # identity. A Repair for this deletion must never touch post-deletion data.
        if state["kind"] == "deleted_removed" or stop > deletion_stop:
            continue
        records = (
            0
            if start == stop
            else backend.count_records(physical_entity_id, start=start, stop=stop)
        )
        measurements = (
            0
            if records == 0
            else backend.count_measurements(
                physical_entity_id, start=start, stop=stop
            )
        )
        result.append(
            {
                "logical_entity_id": str(state["logical_entity_id"]),
                "physical_entity_id": physical_entity_id,
                "start": start,
                "stop": stop,
                "record_count": records,
                "measurement_count": measurements,
            }
        )
    return result


def summarize_deletion_segments(
    segments: list[DeletionSegment],
) -> dict[str, str | int]:
    """Summarize the same snapshot used to approve a deletion."""
    return {
        "segments": len(segments),
        "record_count": sum(item["record_count"] for item in segments),
        "measurement_count": sum(item["measurement_count"] for item in segments),
        "start": min((item["start"] for item in segments), default=""),
        "stop": max((item["stop"] for item in segments), default=""),
    }


def _append_deletion_completion(
    backend: HistoryBackend,
    event: TimelineEvent,
    *,
    decision_kind: str,
    total_records: int,
) -> None:
    """Record completion after all intervals have been processed."""
    registry_entry_id = str(event["registry_entry_id"])
    event_id = str(event["id"])
    backend.append_ledger_entry(
        make_ledger_entry(
            registry_entry_id=registry_entry_id,
            timeline_event_id=event_id,
            operation_id=f"{decision_kind}:{event_id}",
            kind=decision_kind,
            logical_entity_id=str(event["entity_id"]),
            physical_entity_id=str(event["entity_id"]),
            start=str(event.get("ownership_start", EPOCH)),
            stop=str(event["event_timestamp"]),
            record_count=total_records,
        )
    )


def keep_deletion_history(
    backend: HistoryBackend,
    event: TimelineEvent,
) -> None:
    """Record that the user intentionally retained all proven history."""
    state = deletion_decision_state(backend, event)
    if state == "keep_complete":
        return
    if state in {"delete_complete", "delete_pending"}:
        raise RuntimeError("A Delete decision already exists")

    ensure_deletion_ledger_entry(backend, event)
    segments = deletion_history_segments(backend, event)
    count = sum(segment["record_count"] for segment in segments)
    total_records = (
        _pending_deletion_record_count(backend, event, count)
        if state == "keep_pending"
        else count
    )
    if state is None:
        _append_deletion_intent(
            backend, event, decision="keep", record_count=total_records
        )
    for segment in segments:
        backend.append_ledger_entry(
            make_ledger_entry(
                registry_entry_id=str(event["registry_entry_id"]),
                timeline_event_id=str(event["id"]),
                operation_id=f"deleted_kept:{event['id']}",
                kind="deleted_kept",
                logical_entity_id=segment["logical_entity_id"],
                physical_entity_id=segment["physical_entity_id"],
                start=segment["start"],
                stop=segment["stop"],
                record_count=segment["record_count"],
            )
        )
    _append_deletion_completion(
        backend, event, decision_kind="deleted_keep_complete",
        total_records=total_records,
    )


def delete_deletion_history(
    backend: HistoryBackend,
    event: TimelineEvent,
    approved: list[DeletionSegment],
) -> None:
    """Delete and verify each approved physical interval, resuming safely."""
    if not backend.write_enabled or not backend.capabilities.supports_delete:
        raise PermissionError("The configured backend does not allow deletion")
    state = deletion_decision_state(backend, event)
    if state == "delete_complete":
        return
    if state in {"keep_complete", "keep_pending"}:
        raise RuntimeError("A Keep decision already exists")

    completed = {
        _segment_key(entry)
        for entry in backend.list_ledger_entries(str(event["registry_entry_id"]))
        if entry["kind"] == "deleted_removed"
        and entry["timeline_event_id"] == event["id"]
    }
    remaining_plan = {
        _segment_key(segment): segment
        for segment in approved
        if _segment_key(segment) not in completed
    }
    segments = deletion_history_segments(backend, event)
    if remaining_plan.keys() != {_segment_key(segment) for segment in segments}:
        raise DeletionPlanChanged("The physical deletion intervals changed")

    resuming = state == "delete_pending"
    for segment in segments:
        original = remaining_plan[_segment_key(segment)]
        if segment != original and not (resuming and segment["record_count"] == 0):
            raise DeletionPlanChanged("The history counts changed")

    approved_count = sum(segment["record_count"] for segment in approved)
    total_records = (
        _pending_deletion_record_count(backend, event, approved_count)
        if resuming
        else approved_count
    )
    if state is None:
        _append_deletion_intent(
            backend, event, decision="delete", record_count=total_records
        )
    for segment in segments:
        physical_entity_id = segment["physical_entity_id"]
        start = segment["start"]
        stop = segment["stop"]
        if start != stop:
            before = backend.count_records(physical_entity_id, start=start, stop=stop)
            if before != segment["record_count"]:
                raise DeletionPlanChanged("The history changed before interval deletion")
            if before:
                backend.delete_records(physical_entity_id, start=start, stop=stop)
            remaining = backend.count_records(physical_entity_id, start=start, stop=stop)
            if remaining != 0:
                raise RuntimeError(
                    f"Deletion verification failed for {physical_entity_id} "
                    f"[{start}, {stop}): {remaining} records remain"
                )
        # Persist exactly one removal marker after each verified interval.
        backend.append_ledger_entry(
            make_ledger_entry(
                registry_entry_id=str(event["registry_entry_id"]),
                timeline_event_id=str(event["id"]),
                operation_id=f"deleted_removed:{event['id']}",
                kind="deleted_removed",
                logical_entity_id=segment["logical_entity_id"],
                physical_entity_id=physical_entity_id,
                start=start,
                stop=stop,
                record_count=0,
            )
        )

    _append_deletion_completion(
        backend, event, decision_kind="deleted_delete_complete",
        total_records=total_records,
    )


def _events_through_id(
    events: tuple[TimelineEvent, ...], timeline_event_id: str
) -> tuple[TimelineEvent, ...]:
    result: list[TimelineEvent] = []
    for event in sorted(events, key=lambda item: item["event_timestamp"]):
        result.append(event)
        if event["id"] == timeline_event_id:
            return tuple(result)
    return ()


def _logical_intervals(events: tuple[TimelineEvent, ...]) -> list[dict[str, str | bool]]:
    """Derive every closed logical ownership segment through these events."""
    if not events:
        return []
    try:
        intervals = closed_ownership_intervals(events)
    except OwnershipTimelineError as err:
        raise ValueError(str(err)) from err
    return [
        {
            "entity_id": item["entity_id"],
            "start": item["start"],
            "stop": str(item["stop"]),
            "baseline": item["baseline"],
        }
        for item in intervals
    ]


def _journal_plan(
    entry: JournalEntry,
    events: tuple[TimelineEvent, ...],
) -> list[dict[str, object]]:
    raw = entry.get("analysis_windows")
    if isinstance(raw, list) and raw:
        return [dict(item) for item in raw if isinstance(item, dict)]

    through = _events_through_id(events, entry["timeline_event_id"])
    intervals = _logical_intervals(through)
    if len(intervals) != 1:
        return []
    interval = intervals[0]
    return [
        {
            "entity_id": interval["entity_id"],
            "logical_entity_id": interval["entity_id"],
            "start": interval["start"],
            "stop": interval["stop"],
            "source_records": int(entry.get("write_expected_records", entry.get("source_records", 0))),
            "unknown_target_records": int(entry.get("write_expected_target_records", entry.get("unknown_target_records", 0))),
        }
    ]


def ensure_ledger(
    backend: HistoryBackend,
    registry_entry_id: str,
    events: tuple[TimelineEvent, ...],
    journal_entries: tuple[JournalEntry, ...],
) -> list[LedgerEntry]:
    """Initialize and reconcile physical ownership states for closed intervals.

    Lifecycle events now include deletion and restoration boundaries. Completed
    writes are replayed first, then any still-untracked interval is recorded at
    its observed physical location. This keeps the database ledger authoritative
    for physical placement without duplicating the Entity Registry's live state.
    """
    if not events:
        return []

    entries = backend.list_ledger_entries(registry_entry_id)
    ordered = tuple(sorted(events, key=lambda item: item["event_timestamp"]))
    rename_events = tuple(
        item for item in ordered if item.get("event_type", "rename") == "rename"
    )

    completed = sorted(
        (
            item
            for item in journal_entries
            if item["registry_entry_id"] == registry_entry_id
            and item["status"] == "completed"
        ),
        key=lambda item: item["rename_timestamp"],
    )

    # Preserve the explicit baseline kind for legacy timelines whose first
    # observed fact is a rename. A timeline that starts with deletion has its
    # first closed interval recorded by the deletion ledger entry instead.
    if rename_events and ordered[0]["id"] == rename_events[0]["id"]:
        first = rename_events[0]
        baseline_start = str(first.get("ownership_start", EPOCH))
        baseline_key = (baseline_start, first["event_timestamp"])

        if not any(
            item["kind"] == "baseline"
            and (item["start"], item["stop"]) == baseline_key
            for item in entries
        ):
            baseline_count = backend.count_records(
                str(first.get("previous_entity_id", first.get("old_entity_id", ""))),
                start=baseline_start,
                stop=first["event_timestamp"],
            )

            # A pre-ledger migration may already have moved the baseline away.
            # Prefer its persisted pre-write source count when available.
            for item in completed:
                for window in _journal_plan(item, ordered):
                    if (
                        str(window.get("start")) == baseline_start
                        and str(window.get("stop")) == first["event_timestamp"]
                    ):
                        baseline_count = int(
                            window.get("source_records", baseline_count)
                        )
                        break

            append_segment_state(
                backend,
                registry_entry_id=registry_entry_id,
                timeline_event_id=first["id"],
                operation_id=f"baseline:{first['id']}",
                kind="baseline",
                logical_entity_id=str(
                    first.get("previous_entity_id", first.get("old_entity_id", ""))
                ),
                physical_entity_id=str(
                    first.get("previous_entity_id", first.get("old_entity_id", ""))
                ),
                start=baseline_start,
                stop=first["event_timestamp"],
                record_count=baseline_count,
                created_at=first["event_timestamp"],
            )
            entries = backend.list_ledger_entries(registry_entry_id)

    # Replay completed writes from versions that predate the DB ledger. Stored
    # analysis_windows already contain exact half-open intervals and remain the
    # preferred source during upgrades.
    for item in completed:
        operation = str(item.get("write_operation", ""))
        if operation not in {"ready", "merge", "delete", "custom_target"}:
            continue
        if any(
            ledger["timeline_event_id"] == item["timeline_event_id"]
            and ledger["kind"] == operation
            for ledger in entries
        ):
            continue

        created_at = str(item.get("write_completed_at", item["rename_timestamp"]))
        for window in _journal_plan(item, ordered):
            start = str(window.get("start", ""))
            stop = str(window.get("stop", ""))
            if not start or not stop:
                continue
            logical_entity_id = str(
                window.get("logical_entity_id", window.get("entity_id", ""))
            )
            source_records = int(window.get("source_records", 0))
            conflict_records = int(window.get("unknown_target_records", 0))
            final_count = (
                source_records + conflict_records
                if operation == "merge"
                else source_records
            )
            append_segment_state(
                backend,
                registry_entry_id=registry_entry_id,
                timeline_event_id=item["timeline_event_id"],
                operation_id=f"bootstrap:{item['timeline_event_id']}",
                kind=operation,
                logical_entity_id=logical_entity_id,
                physical_entity_id=item["new_entity_id"],
                start=start,
                stop=stop,
                record_count=final_count,
                created_at=created_at,
            )
            if operation == "custom_target" and conflict_records > 0:
                archive = str(item.get("custom_target_entity_id", ""))
                if archive:
                    append_archive_record(
                        backend,
                        registry_entry_id=registry_entry_id,
                        timeline_event_id=item["timeline_event_id"],
                        operation_id=f"bootstrap:{item['timeline_event_id']}",
                        logical_entity_id=item["new_entity_id"],
                        archive_entity_id=archive,
                        start=start,
                        stop=stop,
                        record_count=conflict_records,
                        created_at=created_at,
                    )
        entries = backend.list_ledger_entries(registry_entry_id)

    # A known alias is not globally trusted. Only exact closed intervals are
    # tracked; gaps while an entity was deleted are deliberately left unowned.
    states = latest_segment_states(entries)
    for interval in _logical_intervals(ordered):
        start = str(interval["start"])
        stop = str(interval["stop"])
        key = (start, stop)
        if key in states:
            continue
        entity_id = str(interval["entity_id"])
        record_count = backend.count_records(
            entity_id, start=start, stop=stop
        )
        containing_event = next(
            (
                event
                for event in ordered
                if event["event_timestamp"] == stop
            ),
            ordered[-1],
        )
        kind = "baseline" if bool(interval.get("baseline", False)) else "observed"
        append_segment_state(
            backend,
            registry_entry_id=registry_entry_id,
            timeline_event_id=containing_event["id"],
            operation_id=f"{kind}:{containing_event['id']}:{start}",
            kind=kind,
            logical_entity_id=entity_id,
            physical_entity_id=entity_id,
            start=start,
            stop=stop,
            record_count=record_count,
            created_at=stop,
        )
        entries = backend.list_ledger_entries(registry_entry_id)
        states = latest_segment_states(entries)

    return entries


def prepare_deletion_history(
    backend: HistoryBackend,
    event: TimelineEvent,
    lifecycle_events: tuple[TimelineEvent, ...],
    journal_entries: tuple[JournalEntry, ...],
) -> list[DeletionSegment]:
    """Ensure ledger coverage and read the physical deletion plan."""
    ensure_deletion_ledger_entry(backend, event)
    ensure_ledger(
        backend,
        str(event["registry_entry_id"]),
        lifecycle_events,
        journal_entries,
    )
    return deletion_history_segments(backend, event)


def ensure_deletion_history_ledger(
    backend: HistoryBackend,
    event: TimelineEvent,
    lifecycle_events: tuple[TimelineEvent, ...],
    journal_entries: tuple[JournalEntry, ...],
) -> dict[str, str | int]:
    """Ensure ledger coverage and summarize the current deletion plan."""
    return summarize_deletion_segments(
        prepare_deletion_history(backend, event, lifecycle_events, journal_entries)
    )
