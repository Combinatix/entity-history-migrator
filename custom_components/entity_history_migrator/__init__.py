"""Entity History Migrator."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import Event, HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import entity_registry as er

from .backends import create_backend
from .backends.influxdb_flux import CONF_INFLUXDB_ENTRY_ID
from .const import (
    CONF_BACKEND_CONFIG,
    CONF_CREATE_DELETION_REPAIR,
    CONF_MIGRATION_DELAY,
    CONF_TRACE_LOG_ENABLED,
    DEFAULT_CREATE_DELETION_REPAIR,
    DEFAULT_MIGRATION_DELAY,
    DEFAULT_TRACE_LOG_ENABLED,
    DOMAIN,
    PLATFORMS,
)
from .engine.journal import ProcessingJournal
from .engine.ledger import (
    ensure_deletion_history_ledger,
    has_deletion_decision,
)
from .engine.timeline import EntityTimeline
from .engine.worker import TraceLog, async_worker
from .repairs import (
    async_create_deletion_issue,
    async_remove_conflict_issues,
    async_remove_deletion_issue,
    async_sync_conflict_issues,
)

_LOGGER = logging.getLogger(__name__)


async def _async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload when integration options or backend settings change."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Entity History Migrator."""
    backend = create_backend(hass, entry)
    if backend is None:
        raise ConfigEntryNotReady(
            "The existing history target does not match an available linked "
            "Home Assistant InfluxDB v2 connection"
        )

    # Keep the existing link and options. Drop any saved debug connection data.
    saved_backend = entry.data[CONF_BACKEND_CONFIG]
    inherited_config = {CONF_INFLUXDB_ENTRY_ID: saved_backend[CONF_INFLUXDB_ENTRY_ID]}
    if saved_backend != inherited_config:
        hass.config_entries.async_update_entry(
            entry, data={**entry.data, CONF_BACKEND_CONFIG: inherited_config}
        )

    timeline = EntityTimeline(hass)
    await timeline.async_load()

    journal = ProcessingJournal(hass)
    await journal.async_load()
    await journal.async_reconcile(timeline)

    migration_delay = entry.options.get(
        CONF_MIGRATION_DELAY,
        DEFAULT_MIGRATION_DELAY,
    )
    create_deletion_repair = entry.options.get(
        CONF_CREATE_DELETION_REPAIR,
        DEFAULT_CREATE_DELETION_REPAIR,
    )
    trace = TraceLog(
        hass,
        write_file=bool(
            entry.options.get(CONF_TRACE_LOG_ENABLED, DEFAULT_TRACE_LOG_ENABLED)
        ),
    )

    entity_registry = er.async_get(hass)
    setup_timestamp = datetime.now(UTC).isoformat()

    # Reconcile lifecycle facts that may have changed while this integration was
    # unloaded. The registry remains authoritative for the current entry; the
    # timeline only records the missing historical transition.
    for registry_entry in tuple(entity_registry.entities.values()):
        latest = timeline.latest_event(registry_entry.id)
        if latest is None:
            continue

        latest_timestamp = str(latest["event_timestamp"])
        modified_timestamp = registry_entry.modified_at.isoformat()
        if datetime.fromisoformat(modified_timestamp) <= datetime.fromisoformat(
            latest_timestamp
        ):
            modified_timestamp = setup_timestamp

        if latest["event_type"] == "delete":
            restored = await timeline.async_append_restore(
                registry_entry.id,
                registry_entry.entity_id,
                modified_timestamp,
            )
            if restored is not None and restored["event_type"] == "restore":
                _LOGGER.info(
                    "Reconciled restored entity %s [registry entry %s, timestamp %s]",
                    registry_entry.entity_id,
                    registry_entry.id,
                    restored["event_timestamp"],
                )
                await trace.async_write(
                    "REGISTRY RESTORE RECONCILED "
                    f"registry={registry_entry.id} entity={registry_entry.entity_id} "
                    f"timestamp={restored['event_timestamp']}"
                )
            continue

        # A different current alias proves that at least one rename occurred
        # while the integration was unloaded. modified_at is the safest
        # available boundary; intermediate renames cannot be reconstructed.
        if latest["entity_id"] != registry_entry.entity_id:
            reconciled_rename = await timeline.async_append_rename(
                registry_entry.id,
                latest["entity_id"],
                registry_entry.entity_id,
                modified_timestamp,
                ownership_start=latest_timestamp,
            )
            await journal.async_register_rename_revision(reconciled_rename)
            _LOGGER.warning(
                "Reconciled an entity rename missed while unloaded: %s -> %s "
                "[registry entry %s, timestamp %s]",
                latest["entity_id"],
                registry_entry.entity_id,
                registry_entry.id,
                modified_timestamp,
            )
            await trace.async_write(
                "REGISTRY RENAME RECONCILED "
                f"registry={registry_entry.id} old={latest['entity_id']} "
                f"new={registry_entry.entity_id} timestamp={modified_timestamp}"
            )

    # Recover deletions that happened while the integration was unloaded.
    # Only reconcile deleted entries whose stable ID is already known to our
    # timeline; unrelated historical deletions must not create surprise issues.
    for deleted_entry in getattr(entity_registry, "deleted_entities", {}).values():
        latest = timeline.latest_event(deleted_entry.id)
        if latest is None or latest["event_type"] == "delete":
            continue
        if latest["entity_id"] != deleted_entry.entity_id:
            _LOGGER.warning(
                "Could not reconstruct deletion of %s [registry entry %s]: "
                "the last known alias was %s",
                deleted_entry.entity_id,
                deleted_entry.id,
                latest["entity_id"],
            )
            continue

        deleted_timestamp = deleted_entry.modified_at.isoformat()
        if datetime.fromisoformat(deleted_timestamp) <= datetime.fromisoformat(
            latest["event_timestamp"]
        ):
            _LOGGER.warning(
                "Skipped stale deleted-registry record for %s [registry entry %s]",
                deleted_entry.entity_id,
                deleted_entry.id,
            )
            continue

        ownership_start = timeline.current_alias_start(
            deleted_entry.id,
            deleted_entry.entity_id,
            deleted_entry.created_at.isoformat(),
        )
        reconciled_delete = await timeline.async_append_delete(
            deleted_entry.id,
            deleted_entry.entity_id,
            deleted_timestamp,
            ownership_start=ownership_start,
        )
        await journal.async_close_registry_entry(deleted_entry.id)
        _LOGGER.info(
            "Reconciled deletion missed while unloaded: %s "
            "[registry entry %s, timeline event %s]",
            deleted_entry.entity_id,
            deleted_entry.id,
            reconciled_delete["id"],
        )
        await trace.async_write(
            "REGISTRY DELETE RECONCILED "
            f"registry={deleted_entry.id} entity={deleted_entry.entity_id} "
            f"event={reconciled_delete['id']} timestamp={deleted_timestamp}"
        )

    # The event payload for remove contains only entity_id. This small cache is
    # therefore an event-time lookup from the authoritative Registry, not a
    # second persistent source of current entity state.
    registry_identity: dict[str, tuple[str, str]] = {
        registry_entry.entity_id: (
            registry_entry.id,
            timeline.current_alias_start(
                registry_entry.id,
                registry_entry.entity_id,
                registry_entry.created_at.isoformat(),
            ),
        )
        for registry_entry in entity_registry.entities.values()
    }

    # Deletion metadata is idempotent. Retry all previously recorded deletion
    # facts at startup in case the history backend was unavailable at event time.
    for deletion_event in timeline.deletion_events():
        request = journal.deletion_request(
            backend.storage_identity, str(deletion_event["id"])
        )
        if (
            request is not None
            and request["decision"] == "delete"
            and request["status"] == "queued"
        ):
            async_remove_deletion_issue(hass, deletion_event)
            continue
        if not create_deletion_repair:
            async_remove_deletion_issue(hass, deletion_event)
        try:
            lifecycle_events = timeline.lifecycle_events_through(
                str(deletion_event["registry_entry_id"]),
                str(deletion_event["id"]),
            )
            summary = await hass.async_add_executor_job(
                ensure_deletion_history_ledger,
                backend,
                deletion_event,
                lifecycle_events,
                journal.entries(),
            )
            if create_deletion_repair:
                resolved = await hass.async_add_executor_job(
                    has_deletion_decision, backend, deletion_event
                )
                if resolved:
                    async_remove_deletion_issue(hass, deletion_event)
                else:
                    async_create_deletion_issue(
                        hass, entry, deletion_event, summary
                    )
        except Exception:  # noqa: BLE001
            _LOGGER.warning(
                "Could not reconcile deletion metadata or Repair for %s; it "
                "will be retried on the next integration restart",
                deletion_event.get("entity_id", "unknown"),
                exc_info=True,
            )
            if create_deletion_repair:
                async_create_deletion_issue(hass, entry, deletion_event)

    async def async_entity_registry_updated(event: Event[dict[str, Any]]) -> None:
        """Record Entity Registry lifecycle changes relevant to ownership."""
        data = event.data
        action = data.get("action")
        current_entity_id = data.get("entity_id")

        if not isinstance(current_entity_id, str) or not current_entity_id:
            return

        if action == "create":
            registry_entry = entity_registry.async_get(current_entity_id)
            if registry_entry is not None:
                latest = timeline.latest_event(registry_entry.id)
                if latest is not None and latest["event_type"] == "delete":
                    restored = await timeline.async_append_restore(
                        registry_entry.id,
                        current_entity_id,
                        event.time_fired.isoformat(),
                    )
                    if restored is not None:
                        _LOGGER.info(
                            "Recorded restored entity %s [registry entry %s, "
                            "timeline event %s]",
                            current_entity_id,
                            registry_entry.id,
                            restored["id"],
                        )
                        await trace.async_write(
                            "REGISTRY RESTORE "
                            f"registry={registry_entry.id} entity={current_entity_id} "
                            f"event={restored['id']} timestamp={restored['event_timestamp']}"
                        )
                ownership_start = timeline.current_alias_start(
                    registry_entry.id,
                    current_entity_id,
                    registry_entry.created_at.isoformat(),
                )
                registry_identity[current_entity_id] = (
                    registry_entry.id,
                    ownership_start,
                )
                await trace.async_write(
                    "REGISTRY CREATE "
                    f"registry={registry_entry.id} entity={current_entity_id} "
                    f"created_at={registry_entry.created_at.isoformat()} "
                    f"ownership_start={ownership_start}"
                )
            return

        if action == "remove":
            identity = registry_identity.pop(current_entity_id, None)
            if identity is None:
                _LOGGER.warning(
                    "Entity deletion %s was observed but its registry entry ID "
                    "was not available in the lifecycle cache",
                    current_entity_id,
                )
                return

            registry_entry_id, created_at = identity
            event_timestamp = event.time_fired.isoformat()
            ownership_start = timeline.current_alias_start(
                registry_entry_id,
                current_entity_id,
                created_at,
            )
            timeline_event = await timeline.async_append_delete(
                registry_entry_id,
                current_entity_id,
                event_timestamp,
                ownership_start=ownership_start,
            )
            await trace.async_write(
                "REGISTRY DELETE "
                f"registry={registry_entry_id} entity={current_entity_id} "
                f"event={timeline_event['id']} timestamp={event_timestamp}"
            )

            # A deleted entity cannot remain an active migration target. Keep
            # completed audit rows intact, but close pending/conflict work.
            await journal.async_close_registry_entry(registry_entry_id)
            async_sync_conflict_issues(hass, entry, journal, timeline)

            deletion_summary = None
            try:
                lifecycle_events = timeline.lifecycle_events_through(
                    registry_entry_id, timeline_event["id"]
                )
                deletion_summary = await hass.async_add_executor_job(
                    ensure_deletion_history_ledger,
                    backend,
                    timeline_event,
                    lifecycle_events,
                    journal.entries(),
                )
            except Exception:  # noqa: BLE001
                _LOGGER.warning(
                    "Deletion of %s was recorded in the timeline but its "
                    "database ownership metadata could not be reconciled",
                    current_entity_id,
                    exc_info=True,
                )
            else:
                _LOGGER.info(
                    "Recorded deletion of %s [registry entry %s, records=%s, intervals=%s]",
                    current_entity_id,
                    registry_entry_id,
                    deletion_summary["record_count"] if deletion_summary else 0,
                    deletion_summary["segments"] if deletion_summary else 0,
                )

            if create_deletion_repair:
                async_create_deletion_issue(
                    hass, entry, timeline_event, deletion_summary
                )
            return

        if action != "update":
            return

        previous_entity_id = data.get("old_entity_id")
        registry_entry = entity_registry.async_get(current_entity_id)

        # Keep the identity cache current even for non-rename registry updates,
        # but preserve the start of the current ownership generation.
        if registry_entry is not None and current_entity_id not in registry_identity:
            registry_identity[current_entity_id] = (
                registry_entry.id,
                timeline.current_alias_start(
                    registry_entry.id,
                    current_entity_id,
                    registry_entry.created_at.isoformat(),
                ),
            )

        if (
            not isinstance(previous_entity_id, str)
            or not previous_entity_id
            or previous_entity_id == current_entity_id
        ):
            return

        if registry_entry is None:
            _LOGGER.error(
                "Entity rename %s -> %s was not recorded because the "
                "Entity Registry entry could not be resolved",
                previous_entity_id,
                current_entity_id,
            )
            return

        previous_identity = registry_identity.pop(previous_entity_id, None)
        generation_start = (
            previous_identity[1]
            if previous_identity is not None
            and previous_identity[0] == registry_entry.id
            else timeline.current_alias_start(
                registry_entry.id,
                previous_entity_id,
                registry_entry.created_at.isoformat(),
            )
        )
        registry_identity[current_entity_id] = (
            registry_entry.id,
            generation_start,
        )

        timeline_event = await timeline.async_append_rename(
            registry_entry.id,
            previous_entity_id,
            current_entity_id,
            event.time_fired.isoformat(),
            ownership_start=generation_start,
        )
        await journal.async_register_rename_revision(timeline_event)
        await trace.async_write(
            "REGISTRY RENAME "
            f"registry={registry_entry.id} old={previous_entity_id} "
            f"new={current_entity_id} event={timeline_event['id']} "
            f"timestamp={timeline_event['event_timestamp']}"
        )

        async_sync_conflict_issues(hass, entry, journal, timeline)

        _LOGGER.info(
            "Appended rename fact %s -> %s [registry entry %s, timeline event %s]",
            previous_entity_id,
            current_entity_id,
            registry_entry.id,
            timeline_event["id"],
        )

    unsubscribe_registry = hass.bus.async_listen(
        er.EVENT_ENTITY_REGISTRY_UPDATED,
        async_entity_registry_updated,
    )
    unsubscribe_update = entry.add_update_listener(_async_reload_entry)

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
        "timeline": timeline,
        "journal": journal,
        "backend": backend,
        "unsubscribe_registry": unsubscribe_registry,
        "unsubscribe_update": unsubscribe_update,
        "migration_delay": migration_delay,
        "registry_identity": registry_identity,
        "trace": trace,
    }

    async_sync_conflict_issues(hass, entry, journal, timeline)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_create_background_task(
        hass,
        async_worker(hass, entry, timeline, journal, backend, trace),
        "Entity History Migrator worker",
    )

    _LOGGER.info(
        "Entity History Migrator loaded: backend=%s target=%s events=%d "
        "journal=%d delay=%ss write_enabled=%s deletion_repairs=%s",
        backend.backend_type,
        backend.target_name,
        len(timeline.entries()),
        len(journal.entries()),
        migration_delay,
        backend.write_enabled,
        create_deletion_repair,
    )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not unload_ok:
        return False

    domain_data = hass.data.get(DOMAIN, {})
    entry_data = domain_data.pop(entry.entry_id, None)

    if entry_data is not None:
        entry_data["unsubscribe_registry"]()
        entry_data["unsubscribe_update"]()

    # Conflict issues describe live migration work and disappear on unload.
    # Persistent deletion notices are intentional audit notifications and stay.
    async_remove_conflict_issues(hass)

    if not domain_data:
        hass.data.pop(DOMAIN, None)

    return True
