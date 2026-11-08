"""Backend registry for Entity History Migrator."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from ..const import (
    BACKEND_INFLUXDB,
    BACKEND_INFLUXDB_FLUX,
    CONF_BACKEND_CONFIG,
    CONF_BACKEND_TYPE,
)
from .base import HistoryBackend
from .influxdb_flux import (
    CONF_INFLUXDB_ENTRY_ID,
    InfluxConnectionSettings,
    InfluxDBFluxBackend,
    get_connection_settings,
)


def linked_influxdb_settings(
    hass: HomeAssistant, entry: ConfigEntry,
) -> InfluxConnectionSettings | None:
    """Resolve the InfluxDB connection selected at installation."""
    config = entry.data.get(CONF_BACKEND_CONFIG, {})
    if not isinstance(config, dict):
        return None
    settings = get_connection_settings(hass, config.get(CONF_INFLUXDB_ENTRY_ID))
    if settings is None:
        return None
    # A legacy debug configuration can only inherit the same database.
    # Never redirect its existing journal to a different history store.
    if config.get("connection_mode") == "custom" and any(
        config.get(key) != settings[key] for key in ("url", "organization", "bucket")
    ):
        return None
    return settings


def create_backend(hass: HomeAssistant, entry: ConfigEntry) -> HistoryBackend | None:
    """Create the configured history backend."""
    backend_type = entry.data.get(CONF_BACKEND_TYPE, BACKEND_INFLUXDB)

    if backend_type in {BACKEND_INFLUXDB, BACKEND_INFLUXDB_FLUX}:
        settings = linked_influxdb_settings(hass, entry)
        return InfluxDBFluxBackend(settings) if settings is not None else None

    return None
