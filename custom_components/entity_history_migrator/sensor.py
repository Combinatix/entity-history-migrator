"""Diagnostic sensors for Entity History Migrator."""

from __future__ import annotations

from datetime import timedelta

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import DOMAIN
from .engine.journal import ProcessingJournal
from .engine.timeline import EntityTimeline

SCAN_INTERVAL = timedelta(seconds=5)


_SENSOR_DEFINITIONS = (
    ("queued", "Queued migrations", "mdi:timer-sand"),
    ("ready", "Ready migrations", "mdi:check-circle-outline"),
    ("conflicts", "Conflicts", "mdi:alert-circle-outline"),
    ("approved", "Approved migrations", "mdi:clipboard-check-outline"),
    ("ignored", "Ignored migrations", "mdi:minus-circle-outline"),
    ("no_source", "No source data", "mdi:database-off-outline"),
    ("failed", "Failed migrations", "mdi:alert-octagon-outline"),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up diagnostic count sensors."""
    data = hass.data[DOMAIN][entry.entry_id]
    journal: ProcessingJournal = data["journal"]
    timeline: EntityTimeline = data["timeline"]

    async_add_entities(
        [
            MigratorCountSensor(entry, journal, timeline, key, name, icon)
            for key, name, icon in _SENSOR_DEFINITIONS
        ]
    )


class MigratorCountSensor(SensorEntity):
    """One current processing-state counter."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_should_poll = True

    def __init__(
        self,
        entry: ConfigEntry,
        journal: ProcessingJournal,
        timeline: EntityTimeline,
        key: str,
        name: str,
        icon: str,
    ) -> None:
        self._journal = journal
        self._timeline = timeline
        self._key = key
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_name = name
        self._attr_icon = icon
        self._attr_native_value = 0

    async def async_update(self) -> None:
        """Refresh the count from persistent journal state."""
        self._attr_native_value = self._journal.summary(self._timeline)[self._key]
