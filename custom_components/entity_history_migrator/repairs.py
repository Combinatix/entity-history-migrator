"""Home Assistant Repairs UI for Entity History Migrator conflicts."""

from __future__ import annotations

import logging
from typing import Any, Literal

import voluptuous as vol

from homeassistant.components.repairs import RepairsFlow, RepairsFlowResult
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback, valid_entity_id
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.util import dt as dt_util
from homeassistant.util import unit_conversion

from .backends.base import HistoryBackend
from .const import (
    DOMAIN,
    STATUS_APPROVED_CUSTOM_TARGET,
    STATUS_APPROVED_DELETE,
    STATUS_APPROVED_MERGE,
    STATUS_CONFLICT,
    STATUS_IGNORED,
)
from .engine.journal import JournalEntry, ProcessingJournal
from .engine.ledger import (
    DeletionSegment,
    deletion_decision_state,
    prepare_deletion_history,
    summarize_deletion_segments,
)
from .engine.timeline import EntityTimeline, TimelineEvent
from .helpers import split_entity_id

_LOGGER = logging.getLogger(__name__)

ISSUE_PREFIX = "history_conflict_"
DELETION_ISSUE_PREFIX = "entity_deleted_0_7_3_"
LEGACY_DELETION_ISSUE_PREFIX = "entity_deleted_"


def _format_count(value: int) -> str:
    """Format record counts for human-readable Repairs text."""
    return f"{value:,}"


def _historical_units(entry: JournalEntry, key: str) -> str:
    """Format detected historical units for one side of a conflict."""
    # The user explicitly chose not to restrict unitless entities. In that
    # case _measurement may be a fallback name rather than a physical unit.
    if bool(entry.get("current_unit_known", False)) and entry.get("current_unit") is None:
        return "No unit"

    raw = entry.get(key, [])
    units = sorted({str(unit) for unit in raw if str(unit)})
    return ", ".join(units) if units else "Not identified"


def _source_label(entry: JournalEntry) -> str:
    """Describe the physical source alias or aliases holding trusted history."""
    source_ids = _source_entity_ids(entry)
    if len(source_ids) == 1:
        return source_ids[0]
    if source_ids:
        return ", ".join(source_ids)
    return str(entry["old_entity_id"])


def _source_entity_ids(entry: JournalEntry) -> list[str]:
    """Return each physical source entity ID with proven records."""
    windows = entry.get("analysis_windows", [])
    return sorted(
        {
            str(window.get("entity_id", ""))
            for window in windows
            if isinstance(window, dict)
            and int(window.get("source_records", 0)) > 0
            and str(window.get("entity_id", ""))
        }
    )


def _target_label(entry: JournalEntry) -> str:
    return str(entry["new_entity_id"])


def _format_repairs_timestamp(value: str | None) -> str:
    """Format a timestamp for Repairs in Home Assistant's configured local timezone."""
    if not value:
        return "—"
    parsed = dt_util.parse_datetime(value)
    if parsed is None:
        return "Unavailable"
    return dt_util.as_local(parsed).strftime("%Y-%m-%d %H:%M:%S %Z")


async def _async_history_details(
    hass: HomeAssistant,
    backend: HistoryBackend,
    entity_ids: list[str],
    cache: dict[tuple[str, str], tuple[str | None, str | None] | None],
) -> str:
    """Build per-entity first/last details from complete backend history."""
    lines: list[str] = []
    for entity_id in sorted(set(entity_ids)):
        key = (backend.storage_identity, entity_id)
        if key not in cache:
            try:
                cache[key] = await hass.async_add_executor_job(
                    backend.history_time_bounds, entity_id
                )
            except Exception:  # noqa: BLE001
                _LOGGER.exception(
                    "Could not read full-history time bounds for %s", entity_id
                )
                cache[key] = None

        bounds = cache[key]
        if bounds is None:
            details = "First seen: Unavailable  \nLast seen: Unavailable"
        else:
            first, last = bounds
            if first is None and last is None:
                details = "No history"
            else:
                details = (
                    f"First seen: `{_format_repairs_timestamp(first)}`  \n"
                    f"Last seen: `{_format_repairs_timestamp(last)}`"
                )
        if len(set(entity_ids)) > 1:
            lines.append(f"`{entity_id}`  \n{details}")
        else:
            lines.append(details)
    return "\n\n".join(lines) if lines else "No history"


_PHYSICAL_QUANTITY_NAMES = {
    "electric_potential": "Voltage",
    "electric_current": "Current",
    "power": "Power",
    "apparent_power": "Apparent power",
    "reactive_power": "Reactive power",
    "energy": "Energy",
    "temperature": "Temperature",
    "volume": "Volume",
    "volume_flow_rate": "Flow rate",
    "pressure": "Pressure",
    "frequency": "Frequency",
    "time": "Time",
    "distance": "Distance",
    "speed": "Speed",
    "mass": "Mass",
    "area": "Area",
    "data_rate": "Data rate",
    "data_size": "Data size",
    "illuminance": "Illuminance",
    "percentage": "Dimensionless",
    "ratio": "Dimensionless",
    "dimensionless": "Dimensionless",
    "count": "Count",
}


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


def _friendly_quantity(unit_class: str) -> str:
    """Return a human-readable physical quantity name."""
    return _PHYSICAL_QUANTITY_NAMES.get(
        unit_class, unit_class.replace("_", " ").capitalize()
    )


def _physical_quantity_label(entry: JournalEntry) -> str:
    """Describe physical quantities represented by known units."""
    detected = {
        str(unit)
        for key in ("source_units", "target_units")
        for unit in entry.get(key, [])
        if str(unit)
    }
    current_unit = entry.get("current_unit")
    if isinstance(current_unit, str) and current_unit:
        detected.add(current_unit)

    # Absence of a unit does not prove that a physical quantity is dimensionless.
    detected = {
        unit for unit in detected
        if unit.strip().lower() not in {"unitless", "no unit", "none"}
    }
    if not detected:
        return "Not identified"

    classes: set[str] = set()
    unknown_units: set[str] = set()
    for unit in detected:
        if unit.strip().lower() in {"count", "counts"}:
            classes.add("count")
            continue
        unit_classes = _unit_classes(unit)
        if unit_classes:
            classes.update(unit_classes)
        else:
            unknown_units.add(unit)

    labels = sorted({_friendly_quantity(unit_class) for unit_class in classes})
    if unknown_units and not labels:
        return "Not identified"
    if unknown_units:
        labels.append("Not identified")
    return ", ".join(labels) if labels else "Not identified"


def _units_compatible_label(entry: JournalEntry) -> str:
    """Return the already analyzed merge unit-safety result for the UI."""
    return "Yes" if bool(entry.get("merge_allowed", True)) else "No"


def _merge_note(entry: JournalEntry) -> str:
    """Explain a per-entity merge restriction when units are incompatible."""
    if bool(entry.get("merge_allowed", True)):
        return ""
    return (
        "**Merge is unavailable:** the detected historical units are not "
        "compatible with one physical quantity. Choose Delete, Rename "
        "historical data, or Ignore instead."
    )


def _issue_id(timeline_event_id: str) -> str:
    """Return a stable issue ID for one immutable rename revision."""
    return f"{ISSUE_PREFIX}{timeline_event_id}"


def _existing_issue_ids(hass: HomeAssistant) -> set[str]:
    """Return our existing Repairs issue IDs."""
    registry = ir.async_get(hass)
    return {
        issue_id
        for domain, issue_id in registry.issues
        if domain == DOMAIN and issue_id.startswith(ISSUE_PREFIX)
    }


def _current_conflicts(
    journal: ProcessingJournal,
    timeline: EntityTimeline,
) -> list[JournalEntry]:
    """Return current unresolved conflict revisions."""
    return [
        entry
        for entry in journal.current_entries(timeline)
        if entry["status"] == STATUS_CONFLICT
    ]


def _find_current_conflict(
    journal: ProcessingJournal,
    timeline: EntityTimeline,
    timeline_event_id: str,
) -> JournalEntry | None:
    """Return one current unresolved conflict by timeline event ID."""
    for entry in _current_conflicts(journal, timeline):
        if entry["timeline_event_id"] == timeline_event_id:
            return entry
    return None


@callback
def async_sync_conflict_issues(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    journal: ProcessingJournal,
    timeline: EntityTimeline,
) -> None:
    """Synchronize one Home Assistant Repair issue per unresolved conflict."""
    try:
        conflicts = _current_conflicts(journal, timeline)
        desired_ids = {_issue_id(entry["timeline_event_id"]) for entry in conflicts}

        for issue_id in _existing_issue_ids(hass) - desired_ids:
            ir.async_delete_issue(hass, DOMAIN, issue_id)

        for entry in conflicts:
            ir.async_create_issue(
                hass,
                DOMAIN,
                _issue_id(entry["timeline_event_id"]),
                is_fixable=True,
                is_persistent=False,
                severity=ir.IssueSeverity.WARNING,
                translation_key="history_conflict",
                translation_placeholders={
                    "entity_id": entry["new_entity_id"],
                    "source_label": _source_label(entry),
                    "target_label": _target_label(entry),
                    "source_measurements": _format_count(
                        int(entry.get("source_measurements", 0))
                    ),
                    "source_records": _format_count(
                        int(entry.get("source_records", 0))
                    ),
                    "source_units": _historical_units(entry, "source_units"),
                    "unknown_target_measurements": _format_count(
                        int(entry.get("unknown_target_measurements", 0))
                    ),
                    "unknown_target_records": _format_count(
                        int(entry.get("unknown_target_records", 0))
                    ),
                    "target_units": _historical_units(entry, "target_units"),
                },
                data={
                    "entry_id": config_entry.entry_id,
                    "timeline_event_id": entry["timeline_event_id"],
                },
            )
    except Exception:  # noqa: BLE001
        _LOGGER.exception("Failed to synchronize Entity History Migrator Repairs issues")


@callback
def async_remove_conflict_issues(hass: HomeAssistant) -> None:
    """Remove all Repairs issues owned by this integration."""
    try:
        for issue_id in _existing_issue_ids(hass):
            ir.async_delete_issue(hass, DOMAIN, issue_id)
    except Exception:  # noqa: BLE001
        _LOGGER.exception("Failed to remove Entity History Migrator Repairs issues")


@callback
def async_remove_deletion_issue(hass: HomeAssistant, event: TimelineEvent) -> None:
    """Remove the persistent Repair after a deletion decision is complete."""
    event_id = str(event["id"])
    ir.async_delete_issue(hass, DOMAIN, f"{DELETION_ISSUE_PREFIX}{event_id}")
    ir.async_delete_issue(hass, DOMAIN, f"{LEGACY_DELETION_ISSUE_PREFIX}{event_id}")


@callback
def async_create_deletion_issue(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    event: TimelineEvent,
    summary: dict[str, str | int] | None = None,
) -> None:
    """Create a persistent decision Repair for one observed deletion."""
    # 0.7.2 used this issue ID but had no decision flow. Remove the legacy
    # informational issue before publishing the actionable 0.7.3 issue, or HA
    # would display both for the same deletion after an upgrade.
    ir.async_delete_issue(
        hass, DOMAIN, f"{LEGACY_DELETION_ISSUE_PREFIX}{event['id']}"
    )
    ir.async_create_issue(
        hass,
        DOMAIN,
        f"{DELETION_ISSUE_PREFIX}{event['id']}",
        is_fixable=True,
        is_persistent=True,
        severity=ir.IssueSeverity.WARNING,
        translation_key="entity_deleted",
        translation_placeholders={
            "entity_id": str(event["entity_id"]),
            "measurement_count": _format_count(
                int((summary or {}).get("measurement_count", 0))
            ),
            "segment_count": _format_count(int((summary or {}).get("segments", 0))),
        },
        data={
            "entry_id": config_entry.entry_id,
            "timeline_event_id": str(event["id"]),
            "repair_type": "deletion",
        },
    )


class HistoryConflictRepairFlow(RepairsFlow):
    """Resolve one entity history conflict."""

    _custom_target_entity_id: str | None = None

    def _history_cache(
        self,
    ) -> dict[tuple[str, str], tuple[str | None, str | None] | None]:
        cache = getattr(self, "_history_bounds_cache", None)
        if cache is None:
            cache = {}
            self._history_bounds_cache = cache
        return cache

    def _loaded_data(
        self,
    ) -> tuple[ConfigEntry, ProcessingJournal, EntityTimeline, HistoryBackend] | None:
        if not self.data:
            return None
        entry_id = self.data.get("entry_id")
        if not isinstance(entry_id, str):
            return None
        config_entry = self.hass.config_entries.async_get_entry(entry_id)
        loaded = self.hass.data.get(DOMAIN, {}).get(entry_id)
        if config_entry is None or loaded is None:
            return None
        return config_entry, loaded["journal"], loaded["timeline"], loaded["backend"]

    def _current_entry(self) -> JournalEntry | None:
        loaded = self._loaded_data()
        if loaded is None or not self.data:
            return None
        _, journal, timeline, _ = loaded
        timeline_event_id = self.data.get("timeline_event_id")
        if not isinstance(timeline_event_id, str):
            return None
        return _find_current_conflict(journal, timeline, timeline_event_id)

    async def _async_placeholders(self, current: JournalEntry) -> dict[str, str]:
        loaded = self._loaded_data()
        if loaded is None:
            return {}
        _, _, _, backend = loaded
        source_ids = _source_entity_ids(current) or [str(current["old_entity_id"])]
        placeholders = {
            "source_label": _source_label(current),
            "target_label": _target_label(current),
            "source_units": _historical_units(current, "source_units"),
            "target_units": _historical_units(current, "target_units"),
            "physical_quantity": _physical_quantity_label(current),
            "units_compatible": _units_compatible_label(current),
            "source_measurements": _format_count(
                int(current.get("source_measurements", 0))
            ),
            "source_records": _format_count(int(current.get("source_records", 0))),
            "unknown_target_measurements": _format_count(
                int(current.get("unknown_target_measurements", 0))
            ),
            "unknown_target_records": _format_count(
                int(current.get("unknown_target_records", 0))
            ),
            "merge_note": _merge_note(current),
        }
        placeholders["source_history"] = await _async_history_details(
            self.hass, backend, source_ids, self._history_cache()
        )
        placeholders["target_history"] = await _async_history_details(
            self.hass,
            backend,
            [str(current["new_entity_id"])],
            self._history_cache(),
        )
        return placeholders

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        """Show the decision menu for this specific conflict."""
        if self._loaded_data() is None:
            return self.async_abort(reason="integration_not_loaded")
        current = self._current_entry()
        if current is None:
            return self.async_abort(reason="already_resolved")
        placeholders = await self._async_placeholders(current)

        options: list[str] = []
        if bool(current.get("merge_allowed", True)):
            options.append("merge")
        options.extend(["delete", "custom", "ignore"])

        return self.async_show_menu(
            step_id="init",
            menu_options=options,
            description_placeholders=placeholders,
        )

    async def async_step_back_to_conflict(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        """Return from a confirmation screen to this conflict."""
        return await self.async_step_init()

    async def _async_resolve(
        self,
        resolution: str,
        *,
        custom_target_entity_id: str | None = None,
    ) -> RepairsFlowResult:
        loaded = self._loaded_data()
        current = self._current_entry()
        if loaded is None:
            return self.async_abort(reason="integration_not_loaded")
        if current is None:
            return self.async_abort(reason="already_resolved")

        config_entry, journal, timeline, _ = loaded
        changed = await journal.async_set_resolutions(
            [current["timeline_event_id"]],
            resolution,
            timeline,
            custom_target_entity_id=custom_target_entity_id,
        )
        if changed != 1:
            return self.async_abort(reason="already_resolved")

        async_sync_conflict_issues(self.hass, config_entry, journal, timeline)
        return self.async_create_entry(data={})

    async def async_step_merge(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        current = self._current_entry()
        if current is None:
            return self.async_abort(reason="already_resolved")
        if not bool(current.get("merge_allowed", True)):
            return await self.async_step_init()
        placeholders = await self._async_placeholders(current)
        return self.async_show_menu(
            step_id="merge",
            menu_options=["confirm_merge", "back_to_conflict"],
            description_placeholders=placeholders,
        )

    async def async_step_confirm_merge(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        current = self._current_entry()
        if current is None:
            return self.async_abort(reason="already_resolved")
        if not bool(current.get("merge_allowed", True)):
            return await self.async_step_init()
        return await self._async_resolve(STATUS_APPROVED_MERGE)

    async def async_step_delete(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        current = self._current_entry()
        if current is None:
            return self.async_abort(reason="already_resolved")
        placeholders = await self._async_placeholders(current)
        return self.async_show_menu(
            step_id="delete",
            menu_options=["confirm_delete", "back_to_conflict"],
            description_placeholders=placeholders,
        )

    async def async_step_confirm_delete(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        if self._current_entry() is None:
            return self.async_abort(reason="already_resolved")
        return await self._async_resolve(STATUS_APPROVED_DELETE)

    async def async_step_ignore(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        current = self._current_entry()
        if current is None:
            return self.async_abort(reason="already_resolved")
        placeholders = await self._async_placeholders(current)
        return self.async_show_menu(
            step_id="ignore",
            menu_options=["confirm_ignore", "back_to_conflict"],
            description_placeholders=placeholders,
        )

    async def async_step_confirm_ignore(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        if self._current_entry() is None:
            return self.async_abort(reason="already_resolved")
        return await self._async_resolve(STATUS_IGNORED)

    async def async_step_custom(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        """Store a safe custom archive target for the conflicting target history."""
        current = self._current_entry()
        loaded = self._loaded_data()
        if current is None:
            return self.async_abort(reason="already_resolved")
        if loaded is None:
            return self.async_abort(reason="integration_not_loaded")

        _, _, timeline, backend = loaded
        placeholders = await self._async_placeholders(current)
        domain, object_id = split_entity_id(current["new_entity_id"])
        suggested = f"{domain}.{object_id}_history"
        errors: dict[str, str] = {}
        field_default = self._custom_target_entity_id or suggested

        if user_input is not None:
            custom_entity_id = str(user_input.get("custom_entity_id", "")).strip()
            field_default = custom_entity_id

            if not valid_entity_id(custom_entity_id):
                errors["custom_entity_id"] = "invalid_entity_id"
            elif split_entity_id(custom_entity_id)[0] != domain:
                errors["custom_entity_id"] = "different_domain"
            elif custom_entity_id in {current["old_entity_id"], current["new_entity_id"]}:
                errors["custom_entity_id"] = "reserved_entity_id"
            elif custom_entity_id in timeline.known_entity_ids():
                errors["custom_entity_id"] = "reserved_entity_id"
            elif (
                self.hass.states.get(custom_entity_id) is not None
                or er.async_get(self.hass).async_get(custom_entity_id) is not None
            ):
                errors["custom_entity_id"] = "entity_already_exists"
            else:
                try:
                    existing = await self.hass.async_add_executor_job(
                        backend.count_all_history,
                        custom_entity_id,
                    )
                except Exception:  # noqa: BLE001
                    _LOGGER.exception("Failed to validate custom historical entity target")
                    errors["base"] = "cannot_check_custom_target"
                else:
                    if existing > 0:
                        errors["custom_entity_id"] = "history_already_exists"
                    else:
                        self._custom_target_entity_id = custom_entity_id
                        return await self.async_step_confirm_custom()

        return self.async_show_form(
            step_id="custom",
            data_schema=vol.Schema(
                {vol.Required("custom_entity_id", default=field_default): str}
            ),
            errors=errors,
            description_placeholders=placeholders,
        )

    async def async_step_confirm_custom(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        """Show the final confirmation for a custom archive target."""
        current = self._current_entry()
        if current is None:
            return self.async_abort(reason="already_resolved")
        if not self._custom_target_entity_id:
            return await self.async_step_custom()
        placeholders = await self._async_placeholders(current)
        placeholders["custom_target_entity_id"] = self._custom_target_entity_id
        return self.async_show_menu(
            step_id="confirm_custom",
            menu_options=["confirm_custom_target", "back_to_custom"],
            description_placeholders=placeholders,
        )

    async def async_step_back_to_custom(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        """Return from archive confirmation to the custom target form."""
        return await self.async_step_custom()

    async def async_step_confirm_custom_target(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        """Commit the approved custom archive target after explicit confirmation."""
        if self._current_entry() is None:
            return self.async_abort(reason="already_resolved")
        if not self._custom_target_entity_id:
            return await self.async_step_custom()
        return await self._async_resolve(
            STATUS_APPROVED_CUSTOM_TARGET,
            custom_target_entity_id=self._custom_target_entity_id,
        )


class EntityDeletionRepairFlow(RepairsFlow):
    """Resolve the fate of history after an Entity Registry deletion."""

    def _history_cache(
        self,
    ) -> dict[tuple[str, str], tuple[str | None, str | None] | None]:
        cache = getattr(self, "_history_bounds_cache", None)
        if cache is None:
            cache = {}
            self._history_bounds_cache = cache
        return cache

    def _loaded_data(
        self,
    ) -> tuple[
        ConfigEntry,
        ProcessingJournal,
        EntityTimeline,
        HistoryBackend,
        TimelineEvent,
    ] | None:
        if not self.data:
            return None
        entry_id = self.data.get("entry_id")
        event_id = self.data.get("timeline_event_id")
        if not isinstance(entry_id, str) or not isinstance(event_id, str):
            return None
        config_entry = self.hass.config_entries.async_get_entry(entry_id)
        loaded = self.hass.data.get(DOMAIN, {}).get(entry_id)
        if config_entry is None or loaded is None:
            return None
        timeline: EntityTimeline = loaded["timeline"]
        journal: ProcessingJournal = loaded["journal"]
        event = next(
            (
                item
                for item in timeline.entries()
                if item["id"] == event_id and item["event_type"] == "delete"
            ),
            None,
        )
        if event is None:
            return None
        return config_entry, journal, timeline, loaded["backend"], event

    def _issue_id(self) -> str:
        return f"{DELETION_ISSUE_PREFIX}{self.data['timeline_event_id']}"

    _storage_identity: str | None = None
    _approved: list[DeletionSegment] | None = None

    async def _plan(
        self,
        journal: ProcessingJournal,
        timeline: EntityTimeline,
        backend: HistoryBackend,
        event: TimelineEvent,
    ) -> list[DeletionSegment]:
        lifecycle_events = timeline.lifecycle_events_through(
            str(event["registry_entry_id"]), str(event["id"])
        )
        return await self.hass.async_add_executor_job(
            prepare_deletion_history,
            backend,
            event,
            lifecycle_events,
            journal.entries(),
        )

    async def _async_placeholders(
        self,
        summary: dict[str, str | int],
        entity_id: str,
        backend: HistoryBackend,
        history_entity_ids: list[str] | None = None,
    ) -> dict[str, str]:
        placeholders = {
            "entity_id": entity_id,
            "measurement_count": _format_count(int(summary["measurement_count"])),
            "record_count": _format_count(int(summary["record_count"])),
            "segment_count": _format_count(int(summary["segments"])),
            "start": _format_repairs_timestamp(str(summary["start"])),
            "stop": _format_repairs_timestamp(str(summary["stop"])),
        }
        placeholders["history_details"] = await _async_history_details(
            self.hass,
            backend,
            history_entity_ids or [entity_id],
            self._history_cache(),
        )
        return placeholders

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        loaded = self._loaded_data()
        if loaded is None:
            return self.async_abort(reason="integration_not_loaded")
        _, journal, timeline, backend, event = loaded
        self._storage_identity = backend.storage_identity
        self._approved = None
        request = journal.deletion_request(self._storage_identity, str(event["id"]))
        if request is not None and request["status"] == "queued":
            return self.async_abort(reason="deletion_queued")
        try:
            decision_state = await self.hass.async_add_executor_job(
                deletion_decision_state, backend, event
            )
            if decision_state in {"keep_complete", "delete_complete"}:
                async_remove_deletion_issue(self.hass, event)
                return self.async_abort(reason="already_resolved")
            plan = await self._plan(journal, timeline, backend, event)
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Could not inspect deleted entity history")
            return self.async_abort(reason="history_backend_unavailable")

        options = []
        if decision_state != "delete_pending":
            options.append("keep_history")
        if (
            decision_state != "keep_pending"
            and backend.write_enabled
            and backend.capabilities.supports_delete
        ):
            options.insert(0, "delete_history")
        if not options:
            return self.async_abort(reason="deletion_write_disabled")
        step_id = "init" if request is None else f"deletion_{request['status']}"
        placeholders = await self._async_placeholders(
            summarize_deletion_segments(plan),
            str(event["entity_id"]),
            backend,
            [str(event["entity_id"])] + [
                str(segment["physical_entity_id"])
                for segment in plan
                if int(segment["record_count"]) > 0
            ],
        )
        return self.async_show_menu(
            step_id=step_id,
            menu_options=options,
            description_placeholders=placeholders,
        )

    async def async_step_back_to_deletion(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        return await self.async_step_init()

    async def async_step_delete_history(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        loaded = self._loaded_data()
        if loaded is None:
            return self.async_abort(reason="integration_not_loaded")
        _, journal, timeline, backend, event = loaded
        if backend.storage_identity != self._storage_identity:
            return self.async_abort(reason="deletion_target_changed")
        if not backend.write_enabled or not backend.capabilities.supports_delete:
            return self.async_abort(reason="deletion_write_disabled")
        try:
            self._approved = await self._plan(journal, timeline, backend, event)
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Could not inspect deleted entity history")
            return self.async_abort(reason="history_backend_unavailable")
        placeholders = await self._async_placeholders(
            summarize_deletion_segments(self._approved),
            str(event["entity_id"]),
            backend,
            [str(event["entity_id"])] + [
                str(segment["physical_entity_id"])
                for segment in self._approved
                if int(segment["record_count"]) > 0
            ],
        )
        return self.async_show_menu(
            step_id="delete_history",
            menu_options=["confirm_delete_history", "back_to_deletion"],
            description_placeholders=placeholders,
        )

    async def async_step_keep_history(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        """Show an explicit confirmation before keeping deleted entity history."""
        loaded = self._loaded_data()
        if loaded is None:
            return self.async_abort(reason="integration_not_loaded")
        _, journal, timeline, backend, event = loaded
        if backend.storage_identity != self._storage_identity:
            return self.async_abort(reason="deletion_target_changed")
        try:
            plan = await self._plan(journal, timeline, backend, event)
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Could not inspect deleted entity history")
            return self.async_abort(reason="history_backend_unavailable")
        placeholders = await self._async_placeholders(
            summarize_deletion_segments(plan),
            str(event["entity_id"]),
            backend,
            [str(event["entity_id"])] + [
                str(segment["physical_entity_id"])
                for segment in plan
                if int(segment["record_count"]) > 0
            ],
        )
        return self.async_show_menu(
            step_id="keep_history",
            menu_options=["confirm_keep_history", "back_to_deletion"],
            description_placeholders=placeholders,
        )

    async def _async_queue_decision(
        self, decision: Literal["delete", "keep"],
    ) -> RepairsFlowResult:
        loaded = self._loaded_data()
        if loaded is None:
            return self.async_abort(reason="integration_not_loaded")
        _, journal, _, backend, event = loaded
        if backend.storage_identity != self._storage_identity:
            return self.async_abort(reason="deletion_target_changed")
        approved = self._approved
        if decision == "delete":
            if approved is None:
                return await self.async_step_delete_history()
            if not backend.write_enabled or not backend.capabilities.supports_delete:
                return self.async_abort(reason="deletion_write_disabled")
        try:
            accepted = await journal.async_enqueue_deletion_request(
                {
                    "event_id": str(event["id"]),
                    "registry_entry_id": str(event["registry_entry_id"]),
                    "storage_identity": backend.storage_identity,
                    "decision": decision,
                    "approved": (approved or []) if decision == "delete" else [],
                    "status": "queued",
                }
            )
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Could not queue deletion decision for %s", event["entity_id"])
            return self.async_abort(reason="history_operation_failed")
        if accepted:
            # A successful fix flow removes its issue. The worker can recreate
            # it if this durable request needs more input.
            return self.async_create_entry(data={})
        return self.async_abort(reason="deletion_queued")

    async def async_step_confirm_delete_history(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        return await self._async_queue_decision("delete")

    async def async_step_confirm_keep_history(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        return await self._async_queue_decision("keep")


async def async_create_fix_flow(
    hass: HomeAssistant,
    issue_id: str,
    data: dict[str, str | int | float | None] | None,
) -> RepairsFlow:
    """Create the appropriate conflict or deletion Repair flow."""
    if data and data.get("repair_type") == "deletion":
        return EntityDeletionRepairFlow()
    return HistoryConflictRepairFlow()
