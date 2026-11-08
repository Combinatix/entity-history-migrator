"""Append-only Entity Registry lifecycle timeline."""

from __future__ import annotations

import asyncio
from typing import Literal, NotRequired, TypedDict
from uuid import uuid4

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

from ..const import TIMELINE_STORAGE_KEY, TIMELINE_STORAGE_VERSION


class TimelineEvent(TypedDict, total=False):
    """One immutable observed Entity Registry lifecycle event."""

    id: str
    registry_entry_id: str
    event_type: Literal["rename", "delete", "restore"]
    event_timestamp: str
    entity_id: str
    previous_entity_id: NotRequired[str]
    ownership_start: NotRequired[str]

    # Legacy 0.7.1-and-earlier rename storage fields.
    rename_timestamp: NotRequired[str]
    old_entity_id: NotRequired[str]
    new_entity_id: NotRequired[str]


class EntityTimeline:
    """Persistent append-only lifecycle facts keyed by stable registry ID.

    The Home Assistant Entity Registry is authoritative for the current state.
    This timeline only retains lifecycle facts that cannot be reconstructed
    from the current registry after names change or entries are removed.
    """

    def __init__(self, hass: HomeAssistant) -> None:
        self._store = Store[list[TimelineEvent]](
            hass,
            TIMELINE_STORAGE_VERSION,
            TIMELINE_STORAGE_KEY,
        )
        self._events: list[TimelineEvent] = []
        self._lock = asyncio.Lock()

    @staticmethod
    def _normalize_event(raw: TimelineEvent) -> TimelineEvent:
        """Normalize legacy rename rows while preserving lifecycle rows."""
        event = dict(raw)
        if "event_type" not in event:
            event["event_type"] = "rename"
            event["event_timestamp"] = str(event["rename_timestamp"])
            event["previous_entity_id"] = str(event["old_entity_id"])
            event["entity_id"] = str(event["new_entity_id"])
        return event  # type: ignore[return-value]

    async def async_load(self) -> None:
        """Load persisted lifecycle facts and normalize legacy rename rows."""
        raw_events = await self._store.async_load() or []
        self._events = [self._normalize_event(item) for item in raw_events]
        self._events.sort(key=lambda item: item["event_timestamp"])

    async def async_append_rename(
        self,
        registry_entry_id: str,
        previous_entity_id: str,
        entity_id: str,
        event_timestamp: str,
        *,
        ownership_start: str | None = None,
    ) -> TimelineEvent:
        """Append one rename fact without rewriting previous lifecycle facts."""
        event: TimelineEvent = {
            "id": uuid4().hex,
            "registry_entry_id": registry_entry_id,
            "event_type": "rename",
            "event_timestamp": event_timestamp,
            "previous_entity_id": previous_entity_id,
            "entity_id": entity_id,
            # Keep legacy keys so downgrade inspection remains straightforward.
            "rename_timestamp": event_timestamp,
            "old_entity_id": previous_entity_id,
            "new_entity_id": entity_id,
        }
        if ownership_start:
            event["ownership_start"] = ownership_start
        await self._async_append(event)
        return dict(event)

    async def async_append_delete(
        self,
        registry_entry_id: str,
        entity_id: str,
        event_timestamp: str,
        *,
        ownership_start: str,
    ) -> TimelineEvent:
        """Append one deletion fact and close the current ownership interval."""
        event: TimelineEvent = {
            "id": uuid4().hex,
            "registry_entry_id": registry_entry_id,
            "event_type": "delete",
            "event_timestamp": event_timestamp,
            "entity_id": entity_id,
            "ownership_start": ownership_start,
        }
        await self._async_append(event)
        return dict(event)

    async def async_append_restore(
        self,
        registry_entry_id: str,
        entity_id: str,
        event_timestamp: str,
    ) -> TimelineEvent | None:
        """Record restoration only when the same registry entry was deleted.

        Returning the existing restore event makes startup reconciliation and a
        live ``create`` event idempotent if both paths observe the same restore.
        """
        async with self._lock:
            lifecycle = self.grouped().get(registry_entry_id, ())
            if lifecycle and lifecycle[-1]["event_type"] == "restore":
                last = lifecycle[-1]
                if last["entity_id"] == entity_id:
                    return dict(last)
                return None
            if not lifecycle or lifecycle[-1]["event_type"] != "delete":
                return None
            event: TimelineEvent = {
                "id": uuid4().hex,
                "registry_entry_id": registry_entry_id,
                "event_type": "restore",
                "event_timestamp": event_timestamp,
                "entity_id": entity_id,
                "previous_entity_id": lifecycle[-1]["entity_id"],
                "ownership_start": event_timestamp,
            }
            self._events.append(event)
            self._events.sort(key=lambda item: item["event_timestamp"])
            await self._store.async_save(self._events)
            return dict(event)

    async def _async_append(self, event: TimelineEvent) -> None:
        async with self._lock:
            self._events.append(event)
            self._events.sort(key=lambda item: item["event_timestamp"])
            await self._store.async_save(self._events)

    def entries(self) -> tuple[TimelineEvent, ...]:
        """Return a detached snapshot of all lifecycle events."""
        return tuple(dict(item) for item in self._events)

    def grouped(self) -> dict[str, tuple[TimelineEvent, ...]]:
        """Return all lifecycle events grouped by stable registry entry ID."""
        groups: dict[str, list[TimelineEvent]] = {}
        for item in self._events:
            groups.setdefault(item["registry_entry_id"], []).append(dict(item))
        return {
            registry_entry_id: tuple(
                sorted(items, key=lambda event: event["event_timestamp"])
            )
            for registry_entry_id, items in groups.items()
        }

    def latest_event(self, registry_entry_id: str) -> TimelineEvent | None:
        """Return the latest lifecycle fact for one registry entry."""
        events = self.grouped().get(registry_entry_id, ())
        return dict(events[-1]) if events else None

    def rename_grouped(self) -> dict[str, tuple[TimelineEvent, ...]]:
        """Return only rename events; the processing journal is rename-specific."""
        result: dict[str, tuple[TimelineEvent, ...]] = {}
        for registry_entry_id, events in self.grouped().items():
            renames = tuple(
                event for event in events if event["event_type"] == "rename"
            )
            if renames:
                result[registry_entry_id] = renames
        return result

    def lifecycle_events_through(
        self,
        registry_entry_id: str,
        timeline_event_id: str,
    ) -> tuple[TimelineEvent, ...]:
        """Return all lifecycle facts through one immutable rename revision."""
        lifecycle = self.grouped().get(registry_entry_id, ())
        through: list[TimelineEvent] = []
        for item in lifecycle:
            through.append(dict(item))
            if item["id"] == timeline_event_id:
                return tuple(through)
        raise KeyError(
            f"Timeline event {timeline_event_id} was not found for "
            f"registry entry {registry_entry_id}"
        )

    def rename_events_through(
        self,
        registry_entry_id: str,
        timeline_event_id: str,
    ) -> tuple[TimelineEvent, ...]:
        """Return only rename facts for callers using the legacy method name."""
        return tuple(
            event
            for event in self.lifecycle_events_through(
                registry_entry_id, timeline_event_id
            )
            if event["event_type"] == "rename"
        )

    def latest_rename_revision(self, registry_entry_id: str) -> str | None:
        """Return the latest rename event ID for one registry entry."""
        items = self.rename_grouped().get(registry_entry_id, ())
        return items[-1]["id"] if items else None

    def deletion_events(self) -> tuple[TimelineEvent, ...]:
        """Return all recorded deletions in chronological order."""
        return tuple(
            dict(item) for item in self._events if item["event_type"] == "delete"
        )

    def current_alias_start(
        self,
        registry_entry_id: str,
        entity_id: str,
        fallback_start: str,
    ) -> str:
        """Return the start of the currently active ownership interval."""
        lifecycle = self.grouped().get(registry_entry_id, ())
        for event in reversed(lifecycle):
            event_type = event["event_type"]
            if event_type == "delete":
                break
            if event_type in {"rename", "restore"}:
                if event["entity_id"] == entity_id:
                    return event["event_timestamp"]
                break
        return fallback_start

    def known_entity_ids(self) -> set[str]:
        """Return every entity_id ever observed in the lifecycle timeline."""
        result: set[str] = set()
        for item in self._events:
            entity_id = item.get("entity_id")
            previous = item.get("previous_entity_id")
            if entity_id:
                result.add(entity_id)
            if previous:
                result.add(previous)
        return result


# Source compatibility for third-party imports from pre-0.7.2 versions.
RenameTimeline = EntityTimeline
RenameEvent = TimelineEvent
