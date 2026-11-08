"""Timeline analyzer and migration orchestrator for Entity History Migrator."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import logging
import os
from pathlib import Path
import time
from typing import TypedDict

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.util import unit_conversion

from ..backends.base import HistoryBackend, LedgerEntry
from ..const import (
    CONF_CREATE_DELETION_REPAIR,
    CONF_MIGRATION_DELAY,
    CONF_TRACE_LOG_ENABLED,
    DEFAULT_CREATE_DELETION_REPAIR,
    DEFAULT_MIGRATION_DELAY,
    DEFAULT_TRACE_LOG_ENABLED,
    DOMAIN,
    INTER_JOB_DELAY,
    MIGRATION_TIMEOUT,
    STATUS_CONFLICT,
    STATUS_FAILED,
    STATUS_NO_SOURCE,
    STATUS_READY,
    TRACE_LOG_FILENAME,
    TRACE_LOG_MAX_BYTES,
    VERSION,
    WORKER_POLL_INTERVAL,
)
from .journal import DeletionRequest, ProcessingJournal
from .ownership import OwnershipTimelineError, derive_ownership_intervals
from .ledger import (
    DeletionPlanChanged,
    append_archive_record,
    append_segment_state,
    delete_deletion_history,
    ensure_ledger,
    keep_deletion_history,
    latest_segment_states,
)
from ..repairs import (
    async_create_deletion_issue,
    async_remove_deletion_issue,
    async_sync_conflict_issues,
)
from .timeline import EntityTimeline, TimelineEvent

_LOGGER = logging.getLogger(__name__)

_EPOCH = "1970-01-01T00:00:00.000000Z"


class TimelineError(Exception):
    """The observed rename events do not form one continuous timeline."""


class WriteVerificationError(Exception):
    """A copy/delete verification step did not match the expected counts."""


class WindowSnapshot(TypedDict):
    """Current source/target counts for one migration window."""

    source_records: int
    target_records: int


class OwnershipInterval(TypedDict):
    """One proven interval during which an entity_id belonged to this registry."""

    entity_id: str
    start: str
    stop: str | None
    baseline: bool


class WindowCount(TypedDict):
    """Read-only counts for one historical ownership interval."""

    entity_id: str
    logical_entity_id: str
    start: str
    stop: str
    source_records: int
    unknown_target_records: int
    source_measurements: int
    unknown_target_measurements: int
    source_units: list[str]
    target_units: list[str]


class AnalysisResult(TypedDict):
    """Read-only history backend analysis for one registry timeline."""

    current_entity_id: str
    event_count: int
    windows: list[WindowCount]
    source_records: int
    unknown_target_records: int
    source_measurements: int
    unknown_target_measurements: int
    ready_windows: int
    no_source_windows: int
    conflict_windows: int
    source_units: list[str]
    target_units: list[str]
    current_unit: str | None
    current_unit_known: bool
    merge_allowed: bool
    merge_block_reason: str | None
    duration_seconds: float


class TraceLog:
    """Detailed trace for HA debug logging and the optional disk audit file.

    Home Assistant debug logging and the disk trace are intentionally separate:
    the standard HA action controls `_LOGGER.debug()`, while the integration
    option controls `/config/entity_history_migrator.log`. This keeps normal
    operation quiet but still allows a persistent trace without changing HA's
    global logging level.
    """

    def __init__(self, hass: HomeAssistant, *, write_file: bool = False) -> None:
        self._hass = hass
        self._write_file = write_file
        self._path = Path(hass.config.path(TRACE_LOG_FILENAME))
        self._lock = asyncio.Lock()

    async def async_write(self, message: str) -> None:
        """Emit one trace line to HA debug and optionally to the disk trace."""
        _LOGGER.debug("%s", message)
        if not self._write_file:
            return

        line = f"{datetime.now(UTC).isoformat()} {message}\n"
        async with self._lock:
            await self._hass.async_add_executor_job(self._write, line)

    def _write(self, line: str) -> None:
        """Blocking file write executed in HA executor."""
        if self._path.exists() and self._path.stat().st_size >= TRACE_LOG_MAX_BYTES:
            rotated = Path(f"{self._path}.1")
            try:
                rotated.unlink(missing_ok=True)
                os.replace(self._path, rotated)
            except OSError:
                pass

        with self._path.open("a", encoding="utf-8", newline="") as file:
            file.write(line)


def _unit_classes(unit: str) -> set[str]:
    """Return Home Assistant conversion classes which recognize a unit."""
    classes: set[str] = set()
    base = unit_conversion.BaseUnitConverter
    for candidate in vars(unit_conversion).values():
        if (
            isinstance(candidate, type)
            and candidate is not base
            and issubclass(candidate, base)
            and unit in getattr(candidate, "VALID_UNITS", set())
        ):
            unit_class = getattr(candidate, "UNIT_CLASS", None)
            if isinstance(unit_class, str):
                classes.add(unit_class)
    return classes


def _merge_units_compatible(
    source_units: set[str],
    target_units: set[str],
    current_unit: str | None,
    current_unit_known: bool,
) -> tuple[bool, str | None]:
    """Check that known units all represent the same physical quantity.

    A unitless current HA entity is intentionally unrestricted. Multiple
    historical units are valid when HA recognizes them as convertible within
    one unit class (for example V/mV or °C/°F).
    """
    if current_unit_known and current_unit is None:
        return True, None

    detected = set(source_units) | set(target_units)
    if current_unit is not None:
        detected.add(current_unit)
    if len(detected) <= 1:
        return True, None

    class_sets = [_unit_classes(unit) for unit in detected]
    if all(class_sets):
        common = set.intersection(*class_sets)
        if common:
            return True, None

    # Unknown/custom units are still safe when the literal unit is identical;
    # reaching this point means at least two different units could not be shown
    # to belong to one HA conversion class.
    return (
        False,
        "Detected source/target units do not belong to one compatible "
        "Home Assistant unit class",
    )


def _derive_intervals(
    events: tuple[TimelineEvent, ...],
) -> tuple[str, list[OwnershipInterval]]:
    """Derive proven ownership intervals from rename/delete/restore facts."""
    try:
        current_entity_id, intervals = derive_ownership_intervals(events)
    except OwnershipTimelineError as err:
        raise TimelineError(str(err)) from err

    if current_entity_id is None:
        raise TimelineError("Timeline ends while the registry entry is deleted")
    return current_entity_id, intervals  # type: ignore[return-value]


def _historical_windows(
    current_entity_id: str,
    intervals: list[OwnershipInterval],
) -> list[OwnershipInterval]:
    """Return every closed logical interval before the current live interval.

    A known alias does not make every record under that name trusted. Only the
    exact ownership interval belongs to this registry identity. Records can also
    have been physically moved by an earlier migration, so physical location is
    resolved from the database ledger rather than inferred from the alias.
    """
    del current_entity_id
    return [interval for interval in intervals if interval["stop"] is not None]


def _window_snapshot(
    backend: HistoryBackend,
    source_entity_id: str,
    target_entity_id: str,
    *,
    start: str,
    stop: str,
) -> WindowSnapshot:
    """Read source and target counts through the configured backend."""
    return {
        "source_records": backend.count_records(
            source_entity_id, start=start, stop=stop
        ),
        "target_records": backend.count_records(
            target_entity_id, start=start, stop=stop
        ),
    }


def _registry_target_is_current(
    hass: HomeAssistant,
    registry_entry_id: str,
    target_entity_id: str,
) -> bool:
    """Confirm the target still names the same Entity Registry entry."""
    registry = er.async_get(hass)
    registry_entry = registry.async_get(target_entity_id)
    return registry_entry is not None and registry_entry.id == registry_entry_id


async def _async_process_ready_write(
    hass: HomeAssistant,
    timeline: EntityTimeline,
    journal: ProcessingJournal,
    trace: TraceLog,
    backend: HistoryBackend,
    job,
) -> None:
    """Migrate one current ready revision using physical ledger locations."""
    registry_entry_id = job["registry_entry_id"]
    revision = job["timeline_event_id"]
    expected_records = int(job.get("write_expected_records", 0))
    write_attempt = int(job.get("write_attempts", 1))

    if not backend.write_enabled:
        error = f"writes are disabled for backend target {backend.target_name!r}"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE BLOCKED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    try:
        events = timeline.lifecycle_events_through(registry_entry_id, revision)
        current_entity_id, intervals = _derive_intervals(events)
        plan = await _async_build_write_plan(
            hass, backend, job, current_entity_id, intervals
        )
    except (KeyError, TimelineError, WriteVerificationError) as err:
        error = f"{type(err).__name__}: {err}"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    if expected_records <= 0:
        error = f"invalid expected record count {expected_records}"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    if sum(int(w["source_records"]) for w in plan) != expected_records:
        error = "persisted Ready source count does not match approval"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return
    if any(int(w["unknown_target_records"]) != 0 for w in plan):
        error = "Ready write plan unexpectedly contains target conflicts"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    if (
        timeline.latest_rename_revision(registry_entry_id) != revision
        or not _registry_target_is_current(hass, registry_entry_id, current_entity_id)
    ):
        await journal.async_mark_superseded(revision)
        await trace.async_write(
            "WRITE STALE "
            f"registry={registry_entry_id} revision={revision} phase=before-start"
        )
        return

    await trace.async_write(
        "WRITE START "
        f"registry={registry_entry_id} revision={revision} attempt={write_attempt} "
        f"current={current_entity_id} backend={backend.backend_type} "
        f"target={backend.target_name} expected_records={expected_records} "
        f"windows={len(plan)}"
    )

    try:
        for window in plan:
            expected_source = int(window["source_records"])
            if expected_source == 0:
                continue
            source_entity_id = str(window["entity_id"])
            logical_entity_id = str(
                window.get("logical_entity_id", source_entity_id)
            )
            start = str(window["start"])
            stop = str(window["stop"])

            if source_entity_id == current_entity_id:
                raise WriteVerificationError(
                    "Ready plan contains source already at current target"
                )

            snapshot = await hass.async_add_executor_job(
                lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                    backend, se, current_entity_id, start=st, stop=sp
                )
            )
            source_now = snapshot["source_records"]
            target_now = snapshot["target_records"]
            await trace.async_write(
                "WRITE WINDOW CHECK "
                f"registry={registry_entry_id} logical={logical_entity_id} "
                f"source={source_entity_id} target={current_entity_id} "
                f"start={start} stop={stop} source_records={source_now} "
                f"target_records={target_now} expected_source={expected_source}"
            )

            if write_attempt == 1 and (
                source_now != expected_source or target_now != 0
            ):
                raise WriteVerificationError(
                    "initial Ready state changed before write: "
                    f"source={source_now}/{expected_source}, target={target_now}/0"
                )

            if source_now == 0 and target_now == expected_source:
                await trace.async_write(
                    "WRITE WINDOW ALREADY DONE "
                    f"registry={registry_entry_id} source={source_entity_id} "
                    f"target={current_entity_id} records={target_now}"
                )
            else:
                if source_now != expected_source:
                    raise WriteVerificationError(
                        "unexpected Ready source count: "
                        f"source={source_now}, expected={expected_source}"
                    )

                if target_now == 0:
                    await trace.async_write(
                        "WRITE COPY START "
                        f"registry={registry_entry_id} source={source_entity_id} "
                        f"target={current_entity_id} start={start} stop={stop} "
                        f"records={expected_source}"
                    )
                    await _async_run_write(
                        hass,
                        lambda se=source_entity_id, st=start, sp=stop: backend.copy_records(
                            se, current_entity_id, start=st, stop=sp
                        )
                    )
                    copied = await hass.async_add_executor_job(
                        lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                            backend, se, current_entity_id, start=st, stop=sp
                        )
                    )
                    if (
                        copied["source_records"] != expected_source
                        or copied["target_records"] != expected_source
                    ):
                        raise WriteVerificationError(
                            "Ready copy verification failed: "
                            f"source={copied['source_records']} "
                            f"target={copied['target_records']} expected={expected_source}"
                        )
                    await trace.async_write(
                        "WRITE COPY VERIFIED "
                        f"registry={registry_entry_id} source={source_entity_id} "
                        f"target={current_entity_id} records={expected_source}"
                    )
                elif target_now == expected_source:
                    await trace.async_write(
                        "WRITE COPY ALREADY VERIFIED "
                        f"registry={registry_entry_id} source={source_entity_id} "
                        f"target={current_entity_id} records={expected_source}"
                    )
                else:
                    raise WriteVerificationError(
                        "unexpected Ready target count before delete: "
                        f"target={target_now}, expected=0 or {expected_source}"
                    )

                if (
                    timeline.latest_rename_revision(registry_entry_id) != revision
                    or not _registry_target_is_current(
                        hass, registry_entry_id, current_entity_id
                    )
                ):
                    await journal.async_mark_superseded(revision)
                    await trace.async_write(
                        "WRITE STALE "
                        f"registry={registry_entry_id} revision={revision} "
                        "phase=after-copy source_retained=True"
                    )
                    return

                await trace.async_write(
                    "WRITE DELETE START "
                    f"registry={registry_entry_id} source={source_entity_id} "
                    f"start={start} stop={stop} records={expected_source}"
                )
                await _async_run_write(
                    hass,
                    lambda se=source_entity_id, st=start, sp=stop: backend.delete_records(
                        se, start=st, stop=sp
                    )
                )
                final_window = await hass.async_add_executor_job(
                    lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                        backend, se, current_entity_id, start=st, stop=sp
                    )
                )
                if (
                    final_window["source_records"] != 0
                    or final_window["target_records"] != expected_source
                ):
                    raise WriteVerificationError(
                        "Ready delete verification failed: "
                        f"source={final_window['source_records']} "
                        f"target={final_window['target_records']} "
                        f"expected={expected_source}"
                    )
                await trace.async_write(
                    "WRITE DELETE VERIFIED "
                    f"registry={registry_entry_id} source={source_entity_id} "
                    f"target={current_entity_id} records={expected_source}"
                )

            await _async_run_write(
                hass,
                lambda le=logical_entity_id, st=start, sp=stop, cnt=expected_source: append_segment_state(
                    backend,
                    registry_entry_id=registry_entry_id,
                    timeline_event_id=revision,
                    operation_id=revision,
                    kind="ready",
                    logical_entity_id=le,
                    physical_entity_id=current_entity_id,
                    start=st,
                    stop=sp,
                    record_count=cnt,
                )
            )

        completed = await journal.async_mark_write_completed(revision, timeline)
        if not completed:
            await trace.async_write(
                "WRITE STALE "
                f"registry={registry_entry_id} revision={revision} "
                "phase=complete journal=superseded"
            )
            return

        await trace.async_write(
            "WRITE DONE "
            f"registry={registry_entry_id} revision={revision} "
            f"current={current_entity_id} records={expected_records}"
        )
    except Exception as err:  # noqa: BLE001
        error = f"{type(err).__name__}: {err}"
        await journal.async_mark_write_failed(revision, error)
        _LOGGER.error(
            "History write failed for registry %s: %s",
            registry_entry_id,
            error,
        )
        await trace.async_write(
            "WRITE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )

async def _async_process_merge_write(
    hass: HomeAssistant,
    timeline: EntityTimeline,
    journal: ProcessingJournal,
    trace: TraceLog,
    backend: HistoryBackend,
    job,
) -> None:
    """Accept target conflict history and merge proven source history into it."""
    registry_entry_id = job["registry_entry_id"]
    revision = job["timeline_event_id"]
    expected_source_total = int(job.get("write_expected_records", 0))
    expected_conflict_total = int(job.get("unknown_target_records", 0))
    write_attempt = int(job.get("write_attempts", 1))

    if not backend.write_enabled:
        error = f"writes are disabled for backend target {backend.target_name!r}"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE MERGE BLOCKED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    try:
        events = timeline.lifecycle_events_through(registry_entry_id, revision)
        current_entity_id, intervals = _derive_intervals(events)
        plan = await _async_build_write_plan(
            hass, backend, job, current_entity_id, intervals
        )
    except (KeyError, TimelineError, WriteVerificationError) as err:
        error = f"{type(err).__name__}: {err}"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE MERGE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    if expected_source_total <= 0 or expected_conflict_total <= 0:
        error = (
            "invalid approved Merge counts: "
            f"source={expected_source_total}, target_before={expected_conflict_total}"
        )
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE MERGE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return
    if not bool(job.get("merge_allowed", False)):
        error = "Merge is not allowed for the detected historical units"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE MERGE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return
    if sum(int(w["source_records"]) for w in plan) != expected_source_total:
        await journal.async_mark_write_failed(
            revision, "persisted Merge source count does not match approval"
        )
        return
    if sum(int(w["unknown_target_records"]) for w in plan) != expected_conflict_total:
        await journal.async_mark_write_failed(
            revision, "persisted Merge target count does not match approval"
        )
        return

    if (
        timeline.latest_rename_revision(registry_entry_id) != revision
        or not _registry_target_is_current(hass, registry_entry_id, current_entity_id)
    ):
        await journal.async_mark_superseded(revision)
        await trace.async_write(
            "WRITE MERGE STALE "
            f"registry={registry_entry_id} revision={revision} phase=before-start"
        )
        return

    await trace.async_write(
        "WRITE MERGE START "
        f"registry={registry_entry_id} revision={revision} attempt={write_attempt} "
        f"current={current_entity_id} backend={backend.backend_type} "
        f"target={backend.target_name} source_expected={expected_source_total} "
        f"target_before_expected={expected_conflict_total} "
        f"target_final_expected={expected_source_total + expected_conflict_total} "
        f"windows={len(plan)}"
    )

    try:
        final_total = 0
        for window in plan:
            source_entity_id = str(window["entity_id"])
            logical_entity_id = str(
                window.get("logical_entity_id", source_entity_id)
            )
            start = str(window["start"])
            stop = str(window["stop"])
            expected_source = int(window["source_records"])
            expected_conflict = int(window["unknown_target_records"])
            expected_final = expected_source + expected_conflict

            if source_entity_id == current_entity_id and expected_source > 0:
                raise WriteVerificationError(
                    "Merge plan contains source already at current target"
                )

            source_now = 0
            if expected_source > 0:
                source_now = await hass.async_add_executor_job(
                    lambda se=source_entity_id, st=start, sp=stop: backend.count_records(
                        se, start=st, stop=sp
                    )
                )
            target_now = await hass.async_add_executor_job(
                lambda st=start, sp=stop: backend.count_records(
                    current_entity_id, start=st, stop=sp
                )
            )

            await trace.async_write(
                "WRITE MERGE CHECK "
                f"registry={registry_entry_id} logical={logical_entity_id} "
                f"source={source_entity_id} start={start} stop={stop} "
                f"source_records={source_now} target_records={target_now} "
                f"expected_source={expected_source} expected_conflict={expected_conflict} "
                f"expected_final={expected_final}"
            )

            if write_attempt == 1 and (
                source_now != expected_source or target_now != expected_conflict
            ):
                raise WriteVerificationError(
                    "Merge state changed after approval and before first write: "
                    f"source={source_now}/{expected_source}, "
                    f"target={target_now}/{expected_conflict}"
                )

            if expected_source == 0:
                if target_now != expected_conflict:
                    raise WriteVerificationError(
                        "Merge conflict-only window changed: "
                        f"target={target_now}, expected={expected_conflict}"
                    )
            elif source_now == 0 and target_now == expected_final:
                await trace.async_write(
                    "WRITE MERGE WINDOW ALREADY DONE "
                    f"registry={registry_entry_id} target={current_entity_id} "
                    f"records={target_now}"
                )
            else:
                if source_now != expected_source:
                    raise WriteVerificationError(
                        "Merge source count changed before copy: "
                        f"source={source_now}, expected={expected_source}"
                    )

                if target_now < expected_final:
                    if not (expected_conflict <= target_now <= expected_final):
                        raise WriteVerificationError(
                            "unexpected Merge target count: "
                            f"target={target_now}, expected range="
                            f"{expected_conflict}..{expected_final}"
                        )
                    await trace.async_write(
                        "WRITE MERGE COPY START "
                        f"registry={registry_entry_id} source={source_entity_id} "
                        f"target={current_entity_id} start={start} stop={stop} "
                        f"records={expected_source}"
                    )
                    await _async_run_write(
                        hass,
                        lambda se=source_entity_id, st=start, sp=stop: backend.copy_records(
                            se, current_entity_id, start=st, stop=sp
                        )
                    )
                    source_verify = await hass.async_add_executor_job(
                        lambda se=source_entity_id, st=start, sp=stop: backend.count_records(
                            se, start=st, stop=sp
                        )
                    )
                    target_verify = await hass.async_add_executor_job(
                        lambda st=start, sp=stop: backend.count_records(
                            current_entity_id, start=st, stop=sp
                        )
                    )
                    if source_verify != expected_source or target_verify != expected_final:
                        raise WriteVerificationError(
                            "Merge copy verification failed: "
                            f"source={source_verify}, target={target_verify}, "
                            f"expected_source={expected_source}, expected_target={expected_final}"
                        )
                    await trace.async_write(
                        "WRITE MERGE COPY VERIFIED "
                        f"registry={registry_entry_id} source_records={source_verify} "
                        f"target_records={target_verify}"
                    )
                elif target_now == expected_final:
                    await trace.async_write(
                        "WRITE MERGE COPY ALREADY VERIFIED "
                        f"registry={registry_entry_id} source_records={source_now} "
                        f"target_records={target_now}"
                    )
                else:
                    raise WriteVerificationError(
                        f"Merge target exceeds expected final count: {target_now}"
                    )

                if (
                    timeline.latest_rename_revision(registry_entry_id) != revision
                    or not _registry_target_is_current(
                        hass, registry_entry_id, current_entity_id
                    )
                ):
                    await journal.async_mark_superseded(revision)
                    await trace.async_write(
                        "WRITE MERGE STALE "
                        f"registry={registry_entry_id} revision={revision} "
                        "phase=before-delete source_retained=True"
                    )
                    return

                source_now = await hass.async_add_executor_job(
                    lambda se=source_entity_id, st=start, sp=stop: backend.count_records(
                        se, start=st, stop=sp
                    )
                )
                if source_now:
                    await trace.async_write(
                        "WRITE MERGE DELETE START "
                        f"registry={registry_entry_id} source={source_entity_id} "
                        f"start={start} stop={stop} records={source_now}"
                    )
                    await _async_run_write(
                        hass,
                        lambda se=source_entity_id, st=start, sp=stop: backend.delete_records(
                            se, start=st, stop=sp
                        )
                    )
                remaining = await hass.async_add_executor_job(
                    lambda se=source_entity_id, st=start, sp=stop: backend.count_records(
                        se, start=st, stop=sp
                    )
                )
                target_verify = await hass.async_add_executor_job(
                    lambda st=start, sp=stop: backend.count_records(
                        current_entity_id, start=st, stop=sp
                    )
                )
                if remaining != 0 or target_verify != expected_final:
                    raise WriteVerificationError(
                        "Merge delete verification failed: "
                        f"remaining_source={remaining}, target={target_verify}, "
                        f"expected_target={expected_final}"
                    )
                await trace.async_write(
                    "WRITE MERGE DELETE VERIFIED "
                    f"registry={registry_entry_id} remaining_source=0 "
                    f"target_records={target_verify}"
                )

            await _async_run_write(
                hass,
                lambda le=logical_entity_id, st=start, sp=stop, cnt=expected_final: append_segment_state(
                    backend,
                    registry_entry_id=registry_entry_id,
                    timeline_event_id=revision,
                    operation_id=revision,
                    kind="merge",
                    logical_entity_id=le,
                    physical_entity_id=current_entity_id,
                    start=st,
                    stop=sp,
                    record_count=cnt,
                )
            )
            final_total += expected_final

        completed = await journal.async_mark_write_completed(revision, timeline)
        if not completed:
            await trace.async_write(
                "WRITE MERGE STALE "
                f"registry={registry_entry_id} revision={revision} phase=complete"
            )
            return

        await trace.async_write(
            "WRITE MERGE DONE "
            f"registry={registry_entry_id} revision={revision} "
            f"current={current_entity_id} records={final_total}"
        )
    except Exception as err:  # noqa: BLE001
        error = f"{type(err).__name__}: {err}"
        await journal.async_mark_write_failed(revision, error)
        _LOGGER.error(
            "History Merge failed for registry %s: %s",
            registry_entry_id,
            error,
        )
        await trace.async_write(
            "WRITE MERGE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )

async def _async_build_write_plan(
    hass: HomeAssistant,
    backend: HistoryBackend,
    job,
    current_entity_id: str,
    intervals: list[OwnershipInterval],
) -> list[dict[str, object]]:
    """Return persisted analysis windows, with a safe legacy fallback.

    Version 0.6.0 could store Delete/Rename approvals but never executed them,
    so an approval upgraded from 0.6.0 may not have per-window analysis data.
    In that one case the unchanged database can be sampled before the first
    write and used as the write plan.
    """
    raw = job.get("analysis_windows")
    if isinstance(raw, list) and raw:
        plan: list[dict[str, object]] = []
        for item in raw:
            if not isinstance(item, dict):
                raise WriteVerificationError("invalid persisted write plan")
            plan.append(
                {
                    "entity_id": str(item["entity_id"]),
                    "logical_entity_id": str(
                        item.get("logical_entity_id", item["entity_id"])
                    ),
                    "start": str(item["start"]),
                    "stop": str(item["stop"]),
                    "source_records": int(item["source_records"]),
                    "unknown_target_records": int(item["unknown_target_records"]),
                }
            )
        return plan

    if int(job.get("write_attempts", 1)) != 1:
        raise WriteVerificationError(
            "missing per-window write plan after a previous write attempt"
        )

    plan = []
    for window in _historical_windows(current_entity_id, intervals):
        stop = window["stop"]
        if stop is None:
            continue
        snapshot = await hass.async_add_executor_job(
            lambda w=window, st=stop: _window_snapshot(
                backend,
                w["entity_id"],
                current_entity_id,
                start=w["start"],
                stop=st,
            )
        )
        plan.append(
            {
                "entity_id": window["entity_id"],
                "logical_entity_id": window["entity_id"],
                "start": window["start"],
                "stop": stop,
                "source_records": snapshot["source_records"],
                "unknown_target_records": snapshot["target_records"],
            }
        )
    return plan


async def _async_process_delete_write(
    hass: HomeAssistant,
    timeline: EntityTimeline,
    journal: ProcessingJournal,
    trace: TraceLog,
    backend: HistoryBackend,
    job,
) -> None:
    """Delete conflicting target history, then migrate proven source history."""
    registry_entry_id = job["registry_entry_id"]
    revision = job["timeline_event_id"]
    expected_source_total = int(job.get("write_expected_records", job.get("source_records", 0)))
    expected_conflict_total = int(
        job.get("write_expected_target_records", job.get("unknown_target_records", 0))
    )
    write_attempt = int(job.get("write_attempts", 1))

    if not backend.write_enabled:
        error = f"writes are disabled for backend target {backend.target_name!r}"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE CONFLICT DELETE BLOCKED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    try:
        events = timeline.lifecycle_events_through(registry_entry_id, revision)
        current_entity_id, intervals = _derive_intervals(events)
    except (KeyError, TimelineError) as err:
        error = f"{type(err).__name__}: {err}"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE CONFLICT DELETE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    if (
        timeline.latest_rename_revision(registry_entry_id) != revision
        or not _registry_target_is_current(hass, registry_entry_id, current_entity_id)
    ):
        await journal.async_mark_superseded(revision)
        await trace.async_write(
            "WRITE CONFLICT DELETE STALE "
            f"registry={registry_entry_id} revision={revision} phase=before-start"
        )
        return

    try:
        plan = await _async_build_write_plan(
            hass, backend, job, current_entity_id, intervals
        )
        if sum(int(w["source_records"]) for w in plan) != expected_source_total:
            raise WriteVerificationError("persisted Delete source count does not match approval")
        if sum(int(w["unknown_target_records"]) for w in plan) != expected_conflict_total:
            raise WriteVerificationError("persisted Delete conflict count does not match approval")

        await trace.async_write(
            "WRITE CONFLICT DELETE START "
            f"registry={registry_entry_id} revision={revision} attempt={write_attempt} "
            f"current={current_entity_id} source_expected={expected_source_total} "
            f"conflict_expected={expected_conflict_total} windows={len(plan)}"
        )

        for window in plan:
            source_entity_id = str(window["entity_id"])
            logical_entity_id = str(
                window.get("logical_entity_id", source_entity_id)
            )
            start = str(window["start"])
            stop = str(window["stop"])
            expected_source = int(window["source_records"])
            expected_conflict = int(window["unknown_target_records"])

            if (
                timeline.latest_rename_revision(registry_entry_id) != revision
                or not _registry_target_is_current(
                    hass, registry_entry_id, current_entity_id
                )
            ):
                await journal.async_mark_superseded(revision)
                await trace.async_write(
                    "WRITE CONFLICT DELETE STALE "
                    f"registry={registry_entry_id} revision={revision} "
                    "phase=before-window"
                )
                return

            snapshot = await hass.async_add_executor_job(
                lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                    backend, se, current_entity_id, start=st, stop=sp
                )
            )
            source_now = snapshot["source_records"]
            target_now = snapshot["target_records"]
            await trace.async_write(
                "WRITE CONFLICT DELETE CHECK "
                f"registry={registry_entry_id} source={source_entity_id} "
                f"start={start} stop={stop} source_records={source_now} "
                f"target_records={target_now} expected_source={expected_source} "
                f"original_conflict={expected_conflict}"
            )

            if write_attempt == 1 and (
                source_now != expected_source or target_now != expected_conflict
            ):
                raise WriteVerificationError(
                    "Delete state changed after approval and before first write: "
                    f"source={source_now}/{expected_source}, "
                    f"target={target_now}/{expected_conflict}"
                )

            if expected_source == 0:
                if source_now != 0:
                    raise WriteVerificationError(
                        f"unexpected source history in Delete window {source_entity_id}: {source_now}"
                    )
                if target_now:
                    await trace.async_write(
                        "WRITE CONFLICT TARGET DELETE START "
                        f"registry={registry_entry_id} target={current_entity_id} "
                        f"start={start} stop={stop} records={target_now}"
                    )
                    await _async_run_write(
                        hass,
                        lambda st=start, sp=stop: backend.delete_records(
                            current_entity_id, start=st, stop=sp
                        )
                    )
                target_after = await hass.async_add_executor_job(
                    lambda st=start, sp=stop: backend.count_records(
                        current_entity_id, start=st, stop=sp
                    )
                )
                if target_after != 0:
                    raise WriteVerificationError(
                        f"conflicting target delete failed: remaining_target={target_after}"
                    )
                await trace.async_write(
                    "WRITE CONFLICT TARGET DELETE VERIFIED "
                    f"registry={registry_entry_id} target={current_entity_id} "
                    f"start={start} stop={stop} remaining_target=0"
                )
                await _async_run_write(
                    hass,
                    lambda le=logical_entity_id, st=start, sp=stop: append_segment_state(
                        backend,
                        registry_entry_id=registry_entry_id,
                        timeline_event_id=revision,
                        operation_id=revision,
                        kind="delete",
                        logical_entity_id=le,
                        physical_entity_id=current_entity_id,
                        start=st,
                        stop=sp,
                        record_count=0,
                    )
                )
                continue

            if source_now == 0:
                if target_now != expected_source:
                    raise WriteVerificationError(
                        "Delete restart state is inconsistent: "
                        f"source=0 target={target_now} expected_target={expected_source}"
                    )
                await trace.async_write(
                    "WRITE CONFLICT DELETE WINDOW ALREADY DONE "
                    f"registry={registry_entry_id} source={source_entity_id} "
                    f"target={current_entity_id} records={target_now}"
                )
                await _async_run_write(
                    hass,
                    lambda le=logical_entity_id, st=start, sp=stop, cnt=expected_source: append_segment_state(
                        backend,
                        registry_entry_id=registry_entry_id,
                        timeline_event_id=revision,
                        operation_id=revision,
                        kind="delete",
                        logical_entity_id=le,
                        physical_entity_id=current_entity_id,
                        start=st,
                        stop=sp,
                        record_count=cnt,
                    )
                )
                continue

            if source_now != expected_source:
                raise WriteVerificationError(
                    "Delete source count changed before migration: "
                    f"source={source_now}, expected={expected_source}"
                )

            # Repeating this delete is intentional. If a restart happened after
            # COPY but before source DELETE, source still exists and is the
            # authoritative proven copy for rebuilding this target interval.
            if target_now:
                await trace.async_write(
                    "WRITE CONFLICT TARGET DELETE START "
                    f"registry={registry_entry_id} target={current_entity_id} "
                    f"start={start} stop={stop} records={target_now}"
                )
                await _async_run_write(
                    hass,
                    lambda st=start, sp=stop: backend.delete_records(
                        current_entity_id, start=st, stop=sp
                    )
                )

            target_empty = await hass.async_add_executor_job(
                lambda st=start, sp=stop: backend.count_records(
                    current_entity_id, start=st, stop=sp
                )
            )
            if target_empty != 0:
                raise WriteVerificationError(
                    f"target not empty before Delete migration copy: {target_empty}"
                )
            await trace.async_write(
                "WRITE CONFLICT TARGET DELETE VERIFIED "
                f"registry={registry_entry_id} target={current_entity_id} "
                f"start={start} stop={stop} remaining_target=0"
            )

            await trace.async_write(
                "WRITE CONFLICT DELETE COPY START "
                f"registry={registry_entry_id} source={source_entity_id} "
                f"target={current_entity_id} start={start} stop={stop} "
                f"records={source_now}"
            )
            await _async_run_write(
                hass,
                lambda se=source_entity_id, st=start, sp=stop: backend.copy_records(
                    se, current_entity_id, start=st, stop=sp
                )
            )
            copied = await hass.async_add_executor_job(
                lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                    backend, se, current_entity_id, start=st, stop=sp
                )
            )
            if (
                copied["source_records"] != expected_source
                or copied["target_records"] != expected_source
            ):
                raise WriteVerificationError(
                    "Delete migration copy verification failed: "
                    f"source={copied['source_records']} target={copied['target_records']} "
                    f"expected={expected_source}"
                )
            await trace.async_write(
                "WRITE CONFLICT DELETE COPY VERIFIED "
                f"registry={registry_entry_id} source={source_entity_id} "
                f"target={current_entity_id} records={expected_source}"
            )

            await trace.async_write(
                "WRITE CONFLICT DELETE SOURCE DELETE START "
                f"registry={registry_entry_id} source={source_entity_id} "
                f"start={start} stop={stop} records={expected_source}"
            )
            await _async_run_write(
                hass,
                lambda se=source_entity_id, st=start, sp=stop: backend.delete_records(
                    se, start=st, stop=sp
                )
            )
            final_window = await hass.async_add_executor_job(
                lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                    backend, se, current_entity_id, start=st, stop=sp
                )
            )
            if (
                final_window["source_records"] != 0
                or final_window["target_records"] != expected_source
            ):
                raise WriteVerificationError(
                    "Delete migration final window verification failed: "
                    f"source={final_window['source_records']} "
                    f"target={final_window['target_records']} expected={expected_source}"
                )
            await trace.async_write(
                "WRITE CONFLICT DELETE SOURCE DELETE VERIFIED "
                f"registry={registry_entry_id} source={source_entity_id} "
                f"target={current_entity_id} remaining_source=0 "
                f"target_records={expected_source}"
            )
            await _async_run_write(
                hass,
                lambda le=logical_entity_id, st=start, sp=stop, cnt=expected_source: append_segment_state(
                    backend,
                    registry_entry_id=registry_entry_id,
                    timeline_event_id=revision,
                    operation_id=revision,
                    kind="delete",
                    logical_entity_id=le,
                    physical_entity_id=current_entity_id,
                    start=st,
                    stop=sp,
                    record_count=cnt,
                )
            )

        final_source = 0
        final_target = 0
        for window in plan:
            source_entity_id = str(window["entity_id"])
            start = str(window["start"])
            stop = str(window["stop"])
            snapshot = await hass.async_add_executor_job(
                lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                    backend, se, current_entity_id, start=st, stop=sp
                )
            )
            final_source += snapshot["source_records"]
            final_target += snapshot["target_records"]

        if final_source != 0 or final_target != expected_source_total:
            raise WriteVerificationError(
                "Delete resolution final verification failed: "
                f"source={final_source}, target={final_target}, "
                f"expected_target={expected_source_total}"
            )

        completed = await journal.async_mark_write_completed(revision, timeline)
        if not completed:
            await trace.async_write(
                "WRITE CONFLICT DELETE STALE "
                f"registry={registry_entry_id} revision={revision} phase=complete"
            )
            return

        await trace.async_write(
            "WRITE CONFLICT DELETE DONE "
            f"registry={registry_entry_id} revision={revision} "
            f"current={current_entity_id} records={final_target}"
        )
    except Exception as err:  # noqa: BLE001
        error = f"{type(err).__name__}: {err}"
        await journal.async_mark_write_failed(revision, error)
        _LOGGER.error("History Delete resolution failed for registry %s: %s", registry_entry_id, error)
        await trace.async_write(
            "WRITE CONFLICT DELETE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )


async def _async_process_custom_target_write(
    hass: HomeAssistant,
    timeline: EntityTimeline,
    journal: ProcessingJournal,
    trace: TraceLog,
    backend: HistoryBackend,
    job,
) -> None:
    """Archive conflicting target history, then migrate proven source history."""
    registry_entry_id = job["registry_entry_id"]
    revision = job["timeline_event_id"]
    archive_entity_id = str(job.get("custom_target_entity_id", ""))
    expected_source_total = int(job.get("write_expected_records", job.get("source_records", 0)))
    expected_archive_total = int(
        job.get("write_expected_target_records", job.get("unknown_target_records", 0))
    )
    write_attempt = int(job.get("write_attempts", 1))

    if not archive_entity_id:
        await journal.async_mark_write_failed(revision, "missing custom archive entity_id")
        return
    if not backend.write_enabled:
        error = f"writes are disabled for backend target {backend.target_name!r}"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE ARCHIVE BLOCKED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    try:
        events = timeline.lifecycle_events_through(registry_entry_id, revision)
        current_entity_id, intervals = _derive_intervals(events)
    except (KeyError, TimelineError) as err:
        error = f"{type(err).__name__}: {err}"
        await journal.async_mark_write_failed(revision, error)
        await trace.async_write(
            "WRITE ARCHIVE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )
        return

    if (
        timeline.latest_rename_revision(registry_entry_id) != revision
        or not _registry_target_is_current(hass, registry_entry_id, current_entity_id)
    ):
        await journal.async_mark_superseded(revision)
        await trace.async_write(
            "WRITE ARCHIVE STALE "
            f"registry={registry_entry_id} revision={revision} phase=before-start"
        )
        return

    try:
        plan = await _async_build_write_plan(
            hass, backend, job, current_entity_id, intervals
        )
        if sum(int(w["source_records"]) for w in plan) != expected_source_total:
            raise WriteVerificationError("persisted archive source count does not match approval")
        if sum(int(w["unknown_target_records"]) for w in plan) != expected_archive_total:
            raise WriteVerificationError("persisted archive target count does not match approval")

        archive_all = await hass.async_add_executor_job(
            backend.count_all_history, archive_entity_id
        )
        if write_attempt == 1 and archive_all != 0:
            raise WriteVerificationError(
                f"archive target is no longer empty: records={archive_all}"
            )
        if archive_all > expected_archive_total:
            raise WriteVerificationError(
                "archive target contains unexpected history: "
                f"records={archive_all}, expected_max={expected_archive_total}"
            )

        await trace.async_write(
            "WRITE ARCHIVE START "
            f"registry={registry_entry_id} revision={revision} attempt={write_attempt} "
            f"current={current_entity_id} archive={archive_entity_id} "
            f"source_expected={expected_source_total} "
            f"archive_expected={expected_archive_total} windows={len(plan)}"
        )

        for window in plan:
            source_entity_id = str(window["entity_id"])
            logical_entity_id = str(
                window.get("logical_entity_id", source_entity_id)
            )
            start = str(window["start"])
            stop = str(window["stop"])
            expected_source = int(window["source_records"])
            expected_conflict = int(window["unknown_target_records"])

            if (
                timeline.latest_rename_revision(registry_entry_id) != revision
                or not _registry_target_is_current(
                    hass, registry_entry_id, current_entity_id
                )
            ):
                await journal.async_mark_superseded(revision)
                await trace.async_write(
                    "WRITE ARCHIVE STALE "
                    f"registry={registry_entry_id} revision={revision} "
                    "phase=before-window"
                )
                return

            snapshot = await hass.async_add_executor_job(
                lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                    backend, se, current_entity_id, start=st, stop=sp
                )
            )
            source_now = snapshot["source_records"]
            target_now = snapshot["target_records"]
            archive_now = await hass.async_add_executor_job(
                lambda st=start, sp=stop: backend.count_records(
                    archive_entity_id, start=st, stop=sp
                )
            )

            await trace.async_write(
                "WRITE ARCHIVE CHECK "
                f"registry={registry_entry_id} source={source_entity_id} "
                f"start={start} stop={stop} source_records={source_now} "
                f"target_records={target_now} archive_records={archive_now} "
                f"expected_source={expected_source} expected_archive={expected_conflict}"
            )

            if write_attempt == 1 and (
                source_now != expected_source
                or target_now != expected_conflict
                or archive_now != 0
            ):
                raise WriteVerificationError(
                    "archive state changed after approval and before first write: "
                    f"source={source_now}/{expected_source}, "
                    f"target={target_now}/{expected_conflict}, archive={archive_now}/0"
                )

            if expected_source > 0 and source_now == 0:
                if target_now != expected_source or archive_now != expected_conflict:
                    raise WriteVerificationError(
                        "archive restart state is inconsistent after source delete: "
                        f"target={target_now}/{expected_source}, "
                        f"archive={archive_now}/{expected_conflict}"
                    )
                await trace.async_write(
                    "WRITE ARCHIVE WINDOW ALREADY DONE "
                    f"registry={registry_entry_id} source={source_entity_id}"
                )
                await _async_run_write(
                    hass,
                    lambda le=logical_entity_id, st=start, sp=stop, cnt=expected_source: append_segment_state(
                        backend,
                        registry_entry_id=registry_entry_id,
                        timeline_event_id=revision,
                        operation_id=revision,
                        kind="custom_target",
                        logical_entity_id=le,
                        physical_entity_id=current_entity_id,
                        start=st,
                        stop=sp,
                        record_count=cnt,
                    )
                )
                if expected_conflict > 0:
                    await _async_run_write(
                        hass,
                        lambda st=start, sp=stop, cnt=expected_conflict: append_archive_record(
                            backend,
                            registry_entry_id=registry_entry_id,
                            timeline_event_id=revision,
                            operation_id=revision,
                            logical_entity_id=current_entity_id,
                            archive_entity_id=archive_entity_id,
                            start=st,
                            stop=sp,
                            record_count=cnt,
                        )
                    )
                continue

            if source_now != expected_source:
                raise WriteVerificationError(
                    "archive source count changed before migration: "
                    f"source={source_now}, expected={expected_source}"
                )

            if archive_now != expected_conflict:
                if target_now != expected_conflict:
                    raise WriteVerificationError(
                        "cannot safely finish archive copy: "
                        f"target={target_now}, expected_original={expected_conflict}, "
                        f"archive={archive_now}"
                    )
                if expected_conflict > 0:
                    await trace.async_write(
                        "WRITE ARCHIVE COPY START "
                        f"registry={registry_entry_id} source={current_entity_id} "
                        f"archive={archive_entity_id} start={start} stop={stop} "
                        f"records={expected_conflict}"
                    )
                    await _async_run_write(
                        hass,
                        lambda st=start, sp=stop: backend.copy_records(
                            current_entity_id, archive_entity_id, start=st, stop=sp
                        )
                    )
                archive_now = await hass.async_add_executor_job(
                    lambda st=start, sp=stop: backend.count_records(
                        archive_entity_id, start=st, stop=sp
                    )
                )
                target_verify = await hass.async_add_executor_job(
                    lambda st=start, sp=stop: backend.count_records(
                        current_entity_id, start=st, stop=sp
                    )
                )
                if archive_now != expected_conflict or target_verify != expected_conflict:
                    raise WriteVerificationError(
                        "archive copy verification failed: "
                        f"archive={archive_now}, target={target_verify}, "
                        f"expected={expected_conflict}"
                    )
                await trace.async_write(
                    "WRITE ARCHIVE COPY VERIFIED "
                    f"registry={registry_entry_id} archive={archive_entity_id} "
                    f"records={archive_now}"
                )

            if expected_conflict > 0:
                await _async_run_write(
                    hass,
                    lambda st=start, sp=stop, cnt=expected_conflict: append_archive_record(
                        backend,
                        registry_entry_id=registry_entry_id,
                        timeline_event_id=revision,
                        operation_id=revision,
                        logical_entity_id=current_entity_id,
                        archive_entity_id=archive_entity_id,
                        start=st,
                        stop=sp,
                        record_count=cnt,
                    )
                )

            # Once the archive is verified, rebuilding the target from source is
            # safe and restartable. Re-delete a previous source copy if needed.
            target_now = await hass.async_add_executor_job(
                lambda st=start, sp=stop: backend.count_records(
                    current_entity_id, start=st, stop=sp
                )
            )
            if target_now:
                await trace.async_write(
                    "WRITE ARCHIVE TARGET DELETE START "
                    f"registry={registry_entry_id} target={current_entity_id} "
                    f"start={start} stop={stop} records={target_now}"
                )
                await _async_run_write(
                    hass,
                    lambda st=start, sp=stop: backend.delete_records(
                        current_entity_id, start=st, stop=sp
                    )
                )

            target_empty = await hass.async_add_executor_job(
                lambda st=start, sp=stop: backend.count_records(
                    current_entity_id, start=st, stop=sp
                )
            )
            if target_empty != 0:
                raise WriteVerificationError(
                    f"target not empty before archived-history migration: {target_empty}"
                )
            await trace.async_write(
                "WRITE ARCHIVE TARGET DELETE VERIFIED "
                f"registry={registry_entry_id} target={current_entity_id} "
                f"start={start} stop={stop} remaining_target=0"
            )

            if expected_source > 0:
                await trace.async_write(
                    "WRITE ARCHIVE SOURCE COPY START "
                    f"registry={registry_entry_id} source={source_entity_id} "
                    f"target={current_entity_id} start={start} stop={stop} "
                    f"records={expected_source}"
                )
                await _async_run_write(
                    hass,
                    lambda se=source_entity_id, st=start, sp=stop: backend.copy_records(
                        se, current_entity_id, start=st, stop=sp
                    )
                )

            copied = await hass.async_add_executor_job(
                lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                    backend, se, current_entity_id, start=st, stop=sp
                )
            )
            archive_verify = await hass.async_add_executor_job(
                lambda st=start, sp=stop: backend.count_records(
                    archive_entity_id, start=st, stop=sp
                )
            )
            if (
                copied["source_records"] != expected_source
                or copied["target_records"] != expected_source
                or archive_verify != expected_conflict
            ):
                raise WriteVerificationError(
                    "archived-history source copy verification failed: "
                    f"source={copied['source_records']}, target={copied['target_records']}, "
                    f"archive={archive_verify}"
                )
            await trace.async_write(
                "WRITE ARCHIVE SOURCE COPY VERIFIED "
                f"registry={registry_entry_id} source={source_entity_id} "
                f"target={current_entity_id} records={expected_source}"
            )

            if expected_source > 0:
                await trace.async_write(
                    "WRITE ARCHIVE SOURCE DELETE START "
                    f"registry={registry_entry_id} source={source_entity_id} "
                    f"start={start} stop={stop} records={expected_source}"
                )
                await _async_run_write(
                    hass,
                    lambda se=source_entity_id, st=start, sp=stop: backend.delete_records(
                        se, start=st, stop=sp
                    )
                )

            final_window = await hass.async_add_executor_job(
                lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                    backend, se, current_entity_id, start=st, stop=sp
                )
            )
            final_archive = await hass.async_add_executor_job(
                lambda st=start, sp=stop: backend.count_records(
                    archive_entity_id, start=st, stop=sp
                )
            )
            if (
                final_window["source_records"] != 0
                or final_window["target_records"] != expected_source
                or final_archive != expected_conflict
            ):
                raise WriteVerificationError(
                    "archive final window verification failed: "
                    f"source={final_window['source_records']} "
                    f"target={final_window['target_records']} "
                    f"archive={final_archive}"
                )
            await trace.async_write(
                "WRITE ARCHIVE SOURCE DELETE VERIFIED "
                f"registry={registry_entry_id} source={source_entity_id} "
                f"target={current_entity_id} remaining_source=0 "
                f"target_records={expected_source}"
            )
            await _async_run_write(
                hass,
                lambda le=logical_entity_id, st=start, sp=stop, cnt=expected_source: append_segment_state(
                    backend,
                    registry_entry_id=registry_entry_id,
                    timeline_event_id=revision,
                    operation_id=revision,
                    kind="custom_target",
                    logical_entity_id=le,
                    physical_entity_id=current_entity_id,
                    start=st,
                    stop=sp,
                    record_count=cnt,
                )
            )

        final_source = 0
        final_target = 0
        final_archive = 0
        for window in plan:
            source_entity_id = str(window["entity_id"])
            start = str(window["start"])
            stop = str(window["stop"])
            snapshot = await hass.async_add_executor_job(
                lambda se=source_entity_id, st=start, sp=stop: _window_snapshot(
                    backend, se, current_entity_id, start=st, stop=sp
                )
            )
            final_source += snapshot["source_records"]
            final_target += snapshot["target_records"]
            final_archive += await hass.async_add_executor_job(
                lambda st=start, sp=stop: backend.count_records(
                    archive_entity_id, start=st, stop=sp
                )
            )

        archive_all = await hass.async_add_executor_job(
            backend.count_all_history, archive_entity_id
        )
        if (
            final_source != 0
            or final_target != expected_source_total
            or final_archive != expected_archive_total
            or archive_all != expected_archive_total
        ):
            raise WriteVerificationError(
                "archive resolution final verification failed: "
                f"source={final_source}, target={final_target}/{expected_source_total}, "
                f"archive={final_archive}/{expected_archive_total}, "
                f"archive_all={archive_all}"
            )

        completed = await journal.async_mark_write_completed(revision, timeline)
        if not completed:
            await trace.async_write(
                "WRITE ARCHIVE STALE "
                f"registry={registry_entry_id} revision={revision} phase=complete"
            )
            return

        await trace.async_write(
            "WRITE ARCHIVE DONE "
            f"registry={registry_entry_id} revision={revision} "
            f"current={current_entity_id} target_records={final_target} "
            f"archive={archive_entity_id} archive_records={final_archive}"
        )
    except Exception as err:  # noqa: BLE001
        error = f"{type(err).__name__}: {err}"
        await journal.async_mark_write_failed(revision, error)
        _LOGGER.error("History archive resolution failed for registry %s: %s", registry_entry_id, error)
        await trace.async_write(
            "WRITE ARCHIVE FAILED "
            f"registry={registry_entry_id} revision={revision} error={error}"
        )

def _run_analysis(
    backend: HistoryBackend,
    events: tuple[TimelineEvent, ...],
    ledger_entries: list[LedgerEntry],
    current_unit: str | None,
    current_unit_known: bool,
) -> AnalysisResult:
    """Analyze one registry timeline using logical ownership + physical ledger state."""
    started = time.monotonic()
    current_entity_id, intervals = _derive_intervals(events)
    windows = _historical_windows(current_entity_id, intervals)
    states = latest_segment_states(ledger_entries)

    window_counts: list[WindowCount] = []

    for interval in windows:
        stop = interval["stop"]
        if stop is None:
            continue

        state = states.get((interval["start"], stop))
        if state is not None and state["kind"] == "deleted_removed":
            continue
        physical_source = (
            state["physical_entity_id"] if state is not None else interval["entity_id"]
        )

        physical_records, physical_units = backend.count_records_and_units(
            physical_source,
            start=interval["start"],
            stop=stop,
        )

        if physical_source == current_entity_id:
            # This exact segment was already migrated to the current alias. It
            # is trusted history, not an unknown target conflict and does not
            # need another COPY.
            source_records = 0
            source_measurements = 0
            source_units: set[str] = set()
            unknown_target_records = 0
            unknown_target_measurements = 0
            target_units: set[str] = set()
        else:
            source_records = physical_records
            source_units = physical_units
            source_measurements = (
                backend.count_measurements(
                    physical_source,
                    start=interval["start"],
                    stop=stop,
                )
                if source_records > 0
                else 0
            )
            unknown_target_records, target_units = backend.count_records_and_units(
                current_entity_id,
                start=interval["start"],
                stop=stop,
            )
            unknown_target_measurements = (
                backend.count_measurements(
                    current_entity_id,
                    start=interval["start"],
                    stop=stop,
                )
                if unknown_target_records > 0
                else 0
            )

        window_counts.append(
            {
                "entity_id": physical_source,
                "logical_entity_id": interval["entity_id"],
                "start": interval["start"],
                "stop": stop,
                "source_records": source_records,
                "unknown_target_records": unknown_target_records,
                "source_measurements": source_measurements,
                "unknown_target_measurements": unknown_target_measurements,
                "source_units": sorted(source_units),
                "target_units": sorted(target_units),
            }
        )

    source_records = sum(item["source_records"] for item in window_counts)
    unknown_target_records = sum(
        item["unknown_target_records"] for item in window_counts
    )
    source_measurements = sum(item["source_measurements"] for item in window_counts)
    unknown_target_measurements = sum(
        item["unknown_target_measurements"] for item in window_counts
    )

    ready_windows = sum(
        1
        for item in window_counts
        if item["source_records"] > 0 and item["unknown_target_records"] == 0
    )
    no_source_windows = sum(
        1
        for item in window_counts
        if item["source_records"] == 0 and item["unknown_target_records"] == 0
    )
    conflict_windows = sum(
        1 for item in window_counts if item["unknown_target_records"] > 0
    )

    source_units = {
        unit for item in window_counts for unit in item["source_units"]
    }
    target_units = {
        unit
        for item in window_counts
        for unit in item["target_units"]
        if item["unknown_target_records"] > 0
    }
    merge_allowed, merge_block_reason = _merge_units_compatible(
        source_units,
        target_units,
        current_unit,
        current_unit_known,
    )

    return {
        "current_entity_id": current_entity_id,
        "event_count": len(events),
        "windows": window_counts,
        "source_records": source_records,
        "unknown_target_records": unknown_target_records,
        "source_measurements": source_measurements,
        "unknown_target_measurements": unknown_target_measurements,
        "ready_windows": ready_windows,
        "no_source_windows": no_source_windows,
        "conflict_windows": conflict_windows,
        "source_units": sorted(source_units),
        "target_units": sorted(target_units),
        "current_unit": current_unit,
        "current_unit_known": current_unit_known,
        "merge_allowed": merge_allowed,
        "merge_block_reason": merge_block_reason,
        "duration_seconds": time.monotonic() - started,
    }

def _classify(result: AnalysisResult) -> str:
    """Classify the complete current timeline."""
    if result["unknown_target_records"] > 0:
        return STATUS_CONFLICT
    if result["source_records"] == 0:
        return STATUS_NO_SOURCE
    return STATUS_READY


async def _async_run_write(hass: HomeAssistant, action, *args):
    """Wait for an in-flight database mutation before releasing the worker."""
    work = hass.async_add_executor_job(action, *args)
    try:
        return await asyncio.shield(work)
    except asyncio.CancelledError:
        await asyncio.gather(work, return_exceptions=True)
        raise


async def _async_process_deletion_request(
    hass: HomeAssistant,
    entry: ConfigEntry,
    timeline: EntityTimeline,
    journal: ProcessingJournal,
    trace: TraceLog,
    backend: HistoryBackend,
    request: DeletionRequest,
) -> None:
    """Run a Repair decision on the same worker as migration writes."""
    event_id = request["event_id"]
    decision = request["decision"]
    label = f"event={event_id} decision={decision}"
    await trace.async_write(f"DELETION DECISION START {label}")
    event: TimelineEvent | None = None
    try:
        candidate = timeline.lifecycle_events_through(
            request["registry_entry_id"], event_id
        )[-1]
        if candidate["event_type"] != "delete":
            raise ValueError("Deletion request does not reference a delete event")
        event = candidate
        if decision == "delete":
            await _async_run_write(
                hass, delete_deletion_history, backend, event, request["approved"]
            )
        else:
            await _async_run_write(hass, keep_deletion_history, backend, event)
    except DeletionPlanChanged as err:
        await journal.async_finish_deletion_request(request, "review")
        await trace.async_write(f"DELETION DECISION REVIEW {label} reason={err}")
    except Exception:  # noqa: BLE001
        await journal.async_finish_deletion_request(request, "failed")
        _LOGGER.exception("Deletion decision failed for %s", event_id)
        await trace.async_write(f"DELETION DECISION FAILED {label}")
    else:
        await journal.async_finish_deletion_request(request)
        async_remove_deletion_issue(hass, event)
        await trace.async_write(f"DELETION DECISION DONE {label}")
        return

    if decision == "delete" and event is not None and entry.options.get(
        CONF_CREATE_DELETION_REPAIR, DEFAULT_CREATE_DELETION_REPAIR
    ):
        async_create_deletion_issue(hass, entry, event)


async def async_worker(
    hass: HomeAssistant,
    entry: ConfigEntry,
    timeline: EntityTimeline,
    journal: ProcessingJournal,
    backend: HistoryBackend,
    trace: TraceLog | None = None,
) -> None:
    """Sequentially analyze timelines and migrate ready history."""
    # An executor write can outlive cancellation. Keep the lock across entry
    # reloads until the old worker has drained any in-flight mutation.
    lock = hass.data.setdefault(f"{DOMAIN}.worker_lock", asyncio.Lock())
    async with lock:
        await _async_worker(hass, entry, timeline, journal, backend, trace)


async def _async_worker(
    hass: HomeAssistant,
    entry: ConfigEntry,
    timeline: EntityTimeline,
    journal: ProcessingJournal,
    backend: HistoryBackend,
    trace: TraceLog | None,
) -> None:
    trace_file_enabled = bool(
        entry.options.get(CONF_TRACE_LOG_ENABLED, DEFAULT_TRACE_LOG_ENABLED)
    )
    if trace is None:
        trace = TraceLog(hass, write_file=trace_file_enabled)

    await trace.async_write("_" * 78)
    await trace.async_write(
        "*** WORKER START *** "
        f"version={VERSION} "
        f"backend={backend.backend_type} "
        f"target={backend.target_name} "
        f"poll={WORKER_POLL_INTERVAL}s "
        f"inter_job_delay={INTER_JOB_DELAY}s "
        f"query_timeout={MIGRATION_TIMEOUT}s "
        f"write_enabled={backend.write_enabled} "
        f"trace_file={trace_file_enabled} "
        "mode=entity-registry-timeline-restore-journal-ledger-repair-write"
    )

    try:
        while True:
            settling_delay = int(
                entry.options.get(
                    CONF_MIGRATION_DELAY,
                    DEFAULT_MIGRATION_DELAY,
                )
            )

            if backend.write_enabled:
                custom_job = await journal.async_claim_custom_target_for_write(timeline)
                if custom_job is not None:
                    await _async_process_custom_target_write(
                        hass,
                        timeline,
                        journal,
                        trace,
                        backend,
                        custom_job,
                    )
                    async_sync_conflict_issues(hass, entry, journal, timeline)
                    await asyncio.sleep(INTER_JOB_DELAY)
                    continue

                delete_job = await journal.async_claim_delete_for_write(timeline)
                if delete_job is not None:
                    await _async_process_delete_write(
                        hass,
                        timeline,
                        journal,
                        trace,
                        backend,
                        delete_job,
                    )
                    async_sync_conflict_issues(hass, entry, journal, timeline)
                    await asyncio.sleep(INTER_JOB_DELAY)
                    continue

                merge_job = await journal.async_claim_merge_for_write(timeline)
                if merge_job is not None:
                    await _async_process_merge_write(
                        hass,
                        timeline,
                        journal,
                        trace,
                        backend,
                        merge_job,
                    )
                    async_sync_conflict_issues(hass, entry, journal, timeline)
                    await asyncio.sleep(INTER_JOB_DELAY)
                    continue

                write_job = await journal.async_claim_ready_for_write(timeline)
                if write_job is not None:
                    await _async_process_ready_write(
                        hass,
                        timeline,
                        journal,
                        trace,
                        backend,
                        write_job,
                    )
                    async_sync_conflict_issues(hass, entry, journal, timeline)
                    await asyncio.sleep(INTER_JOB_DELAY)
                    continue

            # Finish any persisted migration write first so a Repair checks
            # the final physical location after migration recovery.
            deletion_request = journal.next_deletion_request(backend.storage_identity)
            if deletion_request is not None:
                await _async_process_deletion_request(
                    hass, entry, timeline, journal, trace, backend, deletion_request
                )
                await asyncio.sleep(INTER_JOB_DELAY)
                continue

            job = await journal.async_claim_next(settling_delay, timeline)

            if job is None:
                await asyncio.sleep(WORKER_POLL_INTERVAL)
                continue

            registry_entry_id = job["registry_entry_id"]
            revision = job["timeline_event_id"]

            try:
                events = timeline.lifecycle_events_through(registry_entry_id, revision)
            except KeyError as err:
                error = f"{type(err).__name__}: {err}"
                await journal.async_mark_failed(revision, error)
                await trace.async_write(
                    "TIMELINE FAILED "
                    f"registry={registry_entry_id} "
                    f"revision={revision} "
                    f"error={error}"
                )
                await asyncio.sleep(INTER_JOB_DELAY)
                continue

            latest = events[-1]
            current_state = hass.states.get(latest["entity_id"])
            current_unit_known = current_state is not None
            current_unit_value = (
                current_state.attributes.get("unit_of_measurement")
                if current_state is not None
                else None
            )
            current_unit = (
                current_unit_value
                if isinstance(current_unit_value, str) and current_unit_value
                else None
            )
            await trace.async_write(
                "TIMELINE START "
                f"registry={registry_entry_id} "
                f"revision={revision} "
                f"attempt={job['attempts']} "
                f"events={len(events)} "
                f"event_types={','.join(str(item.get('event_type', 'rename')) for item in events)} "
                f"current={latest['entity_id']} "
                f"backend={backend.backend_type} "
                f"target={backend.target_name}"
            )

            try:
                ledger_entries = await _async_run_write(
                    hass,
                    ensure_ledger,
                    backend,
                    registry_entry_id,
                    events,
                    journal.entries(),
                )
                await trace.async_write(
                    "LEDGER READY "
                    f"registry={registry_entry_id} entries={len(ledger_entries)}"
                )
                result = await hass.async_add_executor_job(
                    _run_analysis,
                    backend,
                    events,
                    ledger_entries,
                    current_unit,
                    current_unit_known,
                )
            except TimelineError as err:
                error = f"{type(err).__name__}: {err}"
                await journal.async_mark_failed(revision, error)
                _LOGGER.error(
                    "Timeline analysis failed permanently for registry %s: %s",
                    registry_entry_id,
                    error,
                )
                await trace.async_write(
                    "TIMELINE FAILED "
                    f"registry={registry_entry_id} "
                    f"revision={revision} "
                    f"error={error}"
                )
                async_sync_conflict_issues(hass, entry, journal, timeline)
                await asyncio.sleep(INTER_JOB_DELAY)
                continue
            except Exception as err:  # noqa: BLE001
                # Backend/network/query failures are transient. Keep the same
                # immutable revision and retry it with bounded exponential backoff.
                error = f"{type(err).__name__}: {err}"
                delay, retry_at = await journal.async_mark_retry(revision, error)
                _LOGGER.warning(
                    "History backend analysis will retry for registry %s: %s",
                    registry_entry_id,
                    error,
                )
                await trace.async_write(
                    "TIMELINE RETRY "
                    f"registry={registry_entry_id} "
                    f"revision={revision} "
                    f"attempt={job['attempts']} "
                    f"retry_in={delay}s "
                    f"next_retry={retry_at} "
                    f"error={error}"
                )
                await asyncio.sleep(INTER_JOB_DELAY)
                continue

            # A rename may have arrived during backend queries. The old result
            # remains useful audit history only; it must not become current state.
            if timeline.latest_rename_revision(registry_entry_id) != revision:
                await journal.async_mark_superseded(revision)
                await trace.async_write(
                    "TIMELINE STALE "
                    f"registry={registry_entry_id} "
                    f"revision={revision} "
                    "journal=superseded"
                )
                await asyncio.sleep(INTER_JOB_DELAY)
                continue

            for window in result["windows"]:
                await trace.async_write(
                    "WINDOW "
                    f"registry={registry_entry_id} "
                    f"logical={window['logical_entity_id']} "
                    f"source={window['entity_id']} "
                    f"target={result['current_entity_id']} "
                    f"start={window['start']} "
                    f"stop={window['stop']} "
                    f"source_records={window['source_records']} "
                    f"unknown_target_records={window['unknown_target_records']} "
                    f"source_measurements={window['source_measurements']} "
                    f"unknown_target_measurements={window['unknown_target_measurements']} "
                    f"source_units={','.join(window['source_units']) or '-'} "
                    f"target_units={','.join(window['target_units']) or '-'}"
                )

            status = _classify(result)
            saved = await journal.async_finish(
                revision,
                status=status,
                analyzed_at=datetime.now(UTC).isoformat(),
                duration_seconds=result["duration_seconds"],
                window_count=len(result["windows"]),
                ready_windows=result["ready_windows"],
                no_source_windows=result["no_source_windows"],
                conflict_windows=result["conflict_windows"],
                source_records=result["source_records"],
                unknown_target_records=result["unknown_target_records"],
                source_measurements=result["source_measurements"],
                unknown_target_measurements=result["unknown_target_measurements"],
                source_units=result["source_units"],
                target_units=result["target_units"],
                current_unit=result["current_unit"],
                current_unit_known=result["current_unit_known"],
                merge_allowed=result["merge_allowed"],
                merge_block_reason=result["merge_block_reason"],
                windows=result["windows"],
            )

            if not saved:
                await trace.async_write(
                    "TIMELINE STALE "
                    f"registry={registry_entry_id} "
                    f"revision={revision} "
                    "journal=superseded-during-save"
                )
                await asyncio.sleep(INTER_JOB_DELAY)
                continue

            await trace.async_write(
                "TIMELINE DONE "
                f"registry={registry_entry_id} "
                f"revision={revision} "
                f"current={result['current_entity_id']} "
                f"result={status} "
                f"windows={len(result['windows'])} "
                f"ready_windows={result['ready_windows']} "
                f"no_source_windows={result['no_source_windows']} "
                f"conflict_windows={result['conflict_windows']} "
                f"source_records={result['source_records']} "
                f"unknown_target_records={result['unknown_target_records']} "
                f"source_measurements={result['source_measurements']} "
                f"unknown_target_measurements={result['unknown_target_measurements']} "
                f"source_units={','.join(result['source_units']) or '-'} "
                f"target_units={','.join(result['target_units']) or '-'} "
                f"current_unit={result['current_unit'] or '-'} "
                f"current_unit_known={result['current_unit_known']} "
                f"merge_allowed={result['merge_allowed']} "
                f"duration={result['duration_seconds']:.3f}s"
            )

            async_sync_conflict_issues(hass, entry, journal, timeline)
            await asyncio.sleep(INTER_JOB_DELAY)
    finally:
        await trace.async_write(
            "*** WORKER STOP *** "
            f"version={VERSION} backend={backend.backend_type} target={backend.target_name}"
        )
