"""Config flow for Entity History Migrator."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components.influxdb.const import CONF_BUCKET
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResult

from .backends import linked_influxdb_settings
from .backends.influxdb_flux import (
    CONF_INFLUXDB_ENTRY_ID,
    InfluxConnectionSettings,
    get_connection_settings,
    home_assistant_influxdb_entries,
    validate_connection_data,
)
from .const import (
    BACKEND_INFLUXDB_FLUX,
    CONF_BACKEND_CONFIG,
    CONF_BACKEND_TYPE,
    CONF_CREATE_DELETION_REPAIR,
    CONF_MIGRATION_DELAY,
    CONF_TRACE_LOG_ENABLED,
    DEFAULT_CREATE_DELETION_REPAIR,
    DEFAULT_MIGRATION_DELAY,
    DEFAULT_TRACE_LOG_ENABLED,
    DOMAIN,
)


def _connection_summary(settings: InfluxConnectionSettings | None) -> str:
    """Describe the inherited connection without exposing the token."""
    if settings is None:
        return "No linked Home Assistant InfluxDB v2 connection is available."
    return (
        f"URL: {settings['url']}\n"
        f"Organization: {settings['organization']}\n"
        f"Bucket: {settings['bucket']}\n"
        f"Verify SSL: {'Yes' if settings['verify_ssl'] else 'No'}"
    )


def _settling_delay_schema():
    """Accept delays from zero seconds to one hour."""
    return vol.All(vol.Coerce(int), vol.Range(min=0, max=3600))


async def _async_validate_connection(
    hass: HomeAssistant,
    data: dict[str, Any],
) -> dict[str, str]:
    """Validate URL, authentication, organization and readable bucket.

    This is deliberately read-only and verifies that exactly one selected bucket
    is visible to the supplied InfluxDB v2 credentials.
    """
    bucket = data[CONF_BUCKET]

    def _test_connection() -> None:
        validate_connection_data(data)

    try:
        await hass.async_add_executor_job(_test_connection)
    except LookupError:
        return {"base": "invalid_bucket"}
    except ConnectionError as err:
        message = str(err).lower()
        if (
            "organization" in message
            or "could not find org" in message
            or "org not found" in message
        ) and "not found" in message:
            return {"base": "invalid_organization"}
        if "authorization" in message or "unauthorized" in message or "token" in message:
            return {"base": "invalid_auth"}
        return {"base": "cannot_connect"}
    except Exception as err:  # noqa: BLE001 - map library-specific validation errors
        message = str(err).lower()
        if (
            "organization" in message
            or "could not find org" in message
            or "org not found" in message
        ) and "not found" in message:
            return {"base": "invalid_organization"}
        if (
            "authorization" in message
            or "unauthorized" in message
            or "forbidden" in message
            or "token" in message
            or "401" in message
            or "403" in message
        ):
            return {"base": "invalid_auth"}
        return {"base": "unknown"}

    return {}


class EntityHistoryMigratorConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Set up the migrator using the Home Assistant InfluxDB integration."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        connections = home_assistant_influxdb_entries(self.hass)
        if not connections:
            return self.async_abort(reason="no_influxdb_v2")
        if len(connections) > 1:
            return self.async_abort(reason="ambiguous_influxdb_v2")
        influx_entry = connections[0]

        errors = {}
        delay = DEFAULT_MIGRATION_DELAY
        if user_input is not None:
            delay = user_input[CONF_MIGRATION_DELAY]
            errors = await _async_validate_connection(self.hass, dict(influx_entry.data))
            if not errors:
                return self.async_create_entry(
                    title="Entity History Migrator",
                    data={
                        CONF_BACKEND_TYPE: BACKEND_INFLUXDB_FLUX,
                        CONF_BACKEND_CONFIG: {
                            CONF_INFLUXDB_ENTRY_ID: influx_entry.entry_id,
                        },
                    },
                    options={CONF_MIGRATION_DELAY: delay},
                )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {vol.Required(CONF_MIGRATION_DELAY, default=delay): _settling_delay_schema()}
            ),
            errors=errors,
            description_placeholders={
                "current_connection": _connection_summary(
                    get_connection_settings(self.hass, influx_entry.entry_id)
                ),
            },
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> config_entries.OptionsFlow:
        return EntityHistoryMigratorOptionsFlow()


class EntityHistoryMigratorOptionsFlow(config_entries.OptionsFlow):
    """Edit worker and logging preferences."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        if user_input is not None:
            return self.async_create_entry(data=user_input)

        options = self.config_entry.options
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_MIGRATION_DELAY,
                        default=options.get(CONF_MIGRATION_DELAY, DEFAULT_MIGRATION_DELAY),
                    ): _settling_delay_schema(),
                    vol.Required(
                        CONF_TRACE_LOG_ENABLED,
                        default=options.get(CONF_TRACE_LOG_ENABLED, DEFAULT_TRACE_LOG_ENABLED),
                    ): bool,
                    vol.Required(
                        CONF_CREATE_DELETION_REPAIR,
                        default=options.get(
                            CONF_CREATE_DELETION_REPAIR, DEFAULT_CREATE_DELETION_REPAIR
                        ),
                    ): bool,
                }
            ),
            description_placeholders={
                "current_connection": _connection_summary(
                    linked_influxdb_settings(self.hass, self.config_entry)
                ),
            },
        )
