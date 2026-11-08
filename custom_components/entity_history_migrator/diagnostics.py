"""Diagnostics support for Entity History Migrator."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import CONF_BACKEND_TYPE, DOMAIN, VERSION

_TO_REDACT = {"token"}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    """Return a compact, credential-redacted diagnostic snapshot."""
    loaded = hass.data.get(DOMAIN, {}).get(entry.entry_id, {})
    timeline = loaded.get("timeline")
    journal = loaded.get("journal")
    backend = loaded.get("backend")

    return {
        "version": VERSION,
        "backend_type": getattr(backend, "backend_type", entry.data.get(CONF_BACKEND_TYPE)),
        "backend_target": getattr(backend, "target_name", None),
        "write_enabled": getattr(backend, "write_enabled", False),
        "config_entry_data": async_redact_data(dict(entry.data), _TO_REDACT),
        "options": dict(entry.options),
        "timeline_events": len(timeline.entries()) if timeline is not None else None,
        "journal_revisions": len(journal.entries()) if journal is not None else None,
        "current_summary": journal.summary(timeline)
        if journal is not None and timeline is not None
        else None,
    }
