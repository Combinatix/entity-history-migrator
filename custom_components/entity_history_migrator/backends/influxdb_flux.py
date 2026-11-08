"""InfluxDB Flux backend for Entity History Migrator.

The engine intentionally knows nothing about Flux, Influx tags, buckets or the
InfluxDB client. Those details are isolated here so this backend can later be
reimplemented (for example with InfluxQL/line protocol) without changing the
migration engine.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from typing import TypedDict

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from homeassistant.components.influxdb.const import API_VERSION_2, CONF_BUCKET, CONF_ORG
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_API_VERSION, CONF_TOKEN, CONF_URL, CONF_VERIFY_SSL
from homeassistant.core import HomeAssistant

from ..const import (
    BACKEND_INFLUXDB_FLUX,
    MIGRATION_TIMEOUT,
)

from ..helpers import split_entity_id
from .base import BackendCapabilities, HistoryBackend, LedgerEntry

CONF_INFLUXDB_ENTRY_ID = "influxdb_entry_id"

_EPOCH = "1970-01-01T00:00:00.000000Z"
_LEDGER_MEASUREMENT = "_entity_history_migrator_meta"


class InfluxConnectionSettings(TypedDict):
    """Resolved InfluxDB v2 connection settings."""

    url: str
    organization: str
    bucket: str
    token: str
    verify_ssl: bool


def home_assistant_influxdb_entries(hass: HomeAssistant) -> list[ConfigEntry]:
    """Return enabled Home Assistant InfluxDB v2 entries with connection data."""
    return [
        entry
        for entry in hass.config_entries.async_entries("influxdb")
        if entry.disabled_by is None
        and entry.data.get(CONF_API_VERSION) == API_VERSION_2
        and all(entry.data.get(key) for key in (CONF_URL, CONF_ORG, CONF_BUCKET, CONF_TOKEN))
    ]


def get_connection_settings(
    hass: HomeAssistant,
    influxdb_entry_id: str | None,
) -> InfluxConnectionSettings | None:
    """Resolve the linked HA InfluxDB entry without switching targets."""
    influx_entry = next(
        (
            candidate
            for candidate in home_assistant_influxdb_entries(hass)
            if candidate.entry_id == influxdb_entry_id
        ),
        None,
    )
    if influx_entry is None:
        return None

    return {
        "url": influx_entry.data[CONF_URL],
        "organization": influx_entry.data[CONF_ORG],
        "bucket": influx_entry.data[CONF_BUCKET],
        "token": influx_entry.data[CONF_TOKEN],
        "verify_ssl": bool(influx_entry.data.get(CONF_VERIFY_SSL, False)),
    }


def validate_connection_data(data: dict) -> None:
    """Validate a raw InfluxDB v2 connection configuration read-only."""
    client = InfluxDBClient(
        url=data[CONF_URL],
        token=data[CONF_TOKEN],
        org=data[CONF_ORG],
        verify_ssl=data.get(CONF_VERIFY_SSL, False),
        timeout=30_000,
    )
    try:
        result = client.buckets_api().find_buckets(
            org=data[CONF_ORG],
            name=data[CONF_BUCKET],
        )
        buckets = result.buckets or []
        if len(buckets) != 1:
            raise LookupError(data[CONF_BUCKET])
    finally:
        client.close()

def _flux_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _flux_time(timestamp: str) -> str:
    value = datetime.fromisoformat(timestamp).astimezone(UTC)
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _count_query(bucket: str, entity_id: str, *, start: str, stop: str | None) -> str:
    domain, object_id = split_entity_id(entity_id)
    stop_clause = "" if stop is None else f", stop: time(v: {_flux_string(stop)})"
    return (
        f'from(bucket: {_flux_string(bucket)})\n'
        f'  |> range(start: time(v: {_flux_string(start)}){stop_clause})\n'
        f'  |> filter(fn: (r) => '
        f'r.domain == {_flux_string(domain)} and '
        f'r.entity_id == {_flux_string(object_id)})\n'
        f'  |> count(column: "_value")'
    )


def _measurement_count_query(
    bucket: str,
    entity_id: str,
    *,
    start: str,
    stop: str,
) -> str:
    """Count distinct Home Assistant state measurements in one interval.

    Home Assistant writes one state sample as several Flux field records, so
    raw record count cannot be shown as a measurement count. An earlier attempt
    to count unique ``_time`` values across mixed field tables produced wrong
    results. Restrict to primary ``value``/``state`` fields, normalize timestamps
    to one uint schema, then count distinct timestamps server-side.
    """
    domain, object_id = split_entity_id(entity_id)
    return (
        f'from(bucket: {_flux_string(bucket)})\n'
        f'  |> range(start: time(v: {_flux_string(start)}), '
        f'stop: time(v: {_flux_string(stop)}))\n'
        f'  |> filter(fn: (r) => '
        f'r.domain == {_flux_string(domain)} and '
        f'r.entity_id == {_flux_string(object_id)})\n'
        '  |> filter(fn: (r) => r._field == "value" or r._field == "state")\n'
        '  |> map(fn: (r) => ({_value: uint(v: r._time)}))\n'
        '  |> group(columns: [])\n'
        '  |> distinct(column: "_value")\n'
        '  |> count(column: "_value")'
    )


def _history_time_bounds_query(bucket: str, entity_id: str) -> str:
    """Return a Flux query for the first and last sample in full entity history."""
    domain, object_id = split_entity_id(entity_id)
    return (
        f'data = from(bucket: {_flux_string(bucket)})\n'
        f'  |> range(start: time(v: {_flux_string(_EPOCH)}))\n'
        f'  |> filter(fn: (r) => '
        f'r.domain == {_flux_string(domain)} and '
        f'r.entity_id == {_flux_string(object_id)})\n'
        '  |> keep(columns: ["_time"])\n'
        '  |> group(columns: [])\n'
        'first = data |> min(column: "_time") |> set(key: "bound", value: "first")\n'
        'last = data |> max(column: "_time") |> set(key: "bound", value: "last")\n'
        'union(tables: [first, last])\n'
        '  |> keep(columns: ["_time", "bound"])'
    )


def _count_records_and_units(tables) -> tuple[int, set[str]]:
    total = 0
    units: set[str] = set()
    for table in tables:
        for record in table.records:
            value = record.get_value()
            if value is not None:
                total += int(value)
            measurement = record.values.get("_measurement")
            if isinstance(measurement, str) and measurement:
                units.add(measurement)
    return total, units


def _delete_predicate(entity_id: str) -> str:
    domain, object_id = split_entity_id(entity_id)

    def escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    return f'domain="{escape(domain)}" AND entity_id="{escape(object_id)}"'


@dataclass(slots=True)
class InfluxDBFluxBackend(HistoryBackend):
    """InfluxDB v2 history backend implemented with Flux."""

    settings: InfluxConnectionSettings

    backend_type = BACKEND_INFLUXDB_FLUX
    display_name = "InfluxDB (Flux)"
    capabilities = BackendCapabilities(
        supports_copy=True,
        supports_delete=True,
        supports_units=True,
        supports_time_ranges=True,
        supports_ledger=True,
    )

    @property
    def target_name(self) -> str:
        return self.settings["bucket"]

    @property
    def storage_identity(self) -> str:
        """Keep queued deletions tied to this server, organization and bucket."""
        target = json.dumps(
            [self.settings["url"], self.settings["organization"], self.settings["bucket"]]
        )
        return hashlib.sha256(target.encode("utf-8")).hexdigest()

    @property
    def write_enabled(self) -> bool:
        return True

    def _new_client(self) -> InfluxDBClient:
        return InfluxDBClient(
            url=self.settings["url"],
            token=self.settings["token"],
            org=self.settings["organization"],
            verify_ssl=self.settings["verify_ssl"],
            timeout=MIGRATION_TIMEOUT * 1000,
        )

    def verify_connection(self) -> None:
        client = self._new_client()
        try:
            result = client.buckets_api().find_buckets(
                org=self.settings["organization"],
                name=self.settings["bucket"],
            )
            buckets = result.buckets or []
            if len(buckets) != 1:
                raise ConnectionError(
                    f"InfluxDB bucket {self.settings['bucket']!r} was not found uniquely"
                )
        finally:
            client.close()

    def count_records_and_units(
        self,
        entity_id: str,
        *,
        start: str,
        stop: str,
    ) -> tuple[int, set[str]]:
        client = self._new_client()
        try:
            tables = client.query_api().query(
                _count_query(
                    self.settings["bucket"],
                    entity_id,
                    start=_flux_time(start),
                    stop=_flux_time(stop),
                ),
                org=self.settings["organization"],
            )
            return _count_records_and_units(tables)
        finally:
            client.close()

    def count_measurements(
        self,
        entity_id: str,
        *,
        start: str,
        stop: str,
    ) -> int:
        client = self._new_client()
        try:
            tables = client.query_api().query(
                _measurement_count_query(
                    self.settings["bucket"],
                    entity_id,
                    start=_flux_time(start),
                    stop=_flux_time(stop),
                ),
                org=self.settings["organization"],
            )
            total = 0
            for table in tables:
                for record in table.records:
                    value = record.get_value()
                    if value is not None:
                        total += int(value)
            return total
        finally:
            client.close()

    def count_all_history(self, entity_id: str) -> int:
        client = self._new_client()
        try:
            tables = client.query_api().query(
                _count_query(
                    self.settings["bucket"],
                    entity_id,
                    start=_EPOCH,
                    stop=None,
                ),
                org=self.settings["organization"],
            )
            total, _ = _count_records_and_units(tables)
            return total
        finally:
            client.close()

    def history_time_bounds(
        self, entity_id: str
    ) -> tuple[str | None, str | None]:
        """Return full-history first/last sample timestamps in UTC."""
        client = self._new_client()
        try:
            tables = client.query_api().query(
                _history_time_bounds_query(self.settings["bucket"], entity_id),
                org=self.settings["organization"],
            )
            first: str | None = None
            last: str | None = None
            for table in tables:
                for record in table.records:
                    bound = record.values.get("bound")
                    timestamp = record.get_time()
                    if timestamp is None:
                        continue
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=UTC)
                    value = timestamp.astimezone(UTC).isoformat(timespec="seconds")
                    if bound == "first":
                        first = value
                    elif bound == "last":
                        last = value
            return first, last
        finally:
            client.close()

    def _resolve_bucket_ids(self, client: InfluxDBClient) -> tuple[str, str]:
        """Resolve IDs required by the tested Flux ``to()`` write path.

        Using bucket/org names with ``to()`` failed in testing with
        ``could not find org``. Resolving and writing with bucketID/orgID is
        therefore deliberate, not cosmetic.
        """
        result = client.buckets_api().find_buckets(
            org=self.settings["organization"],
            name=self.settings["bucket"],
        )
        buckets = result.buckets or []
        if len(buckets) != 1 or not buckets[0].id or not buckets[0].org_id:
            raise RuntimeError(
                "Could not resolve exactly one InfluxDB bucket/org ID pair for "
                f"{self.settings['bucket']!r}"
            )
        return buckets[0].id, buckets[0].org_id

    def copy_records(
        self,
        source_entity_id: str,
        target_entity_id: str,
        *,
        start: str,
        stop: str,
    ) -> None:
        source_domain, source_object_id = split_entity_id(source_entity_id)
        target_domain, target_object_id = split_entity_id(target_entity_id)

        client = self._new_client()
        try:
            bucket_id, org_id = self._resolve_bucket_ids(client)
            query = (
                f'from(bucket: {_flux_string(self.settings["bucket"])})\n'
                f'  |> range(start: time(v: {_flux_string(_flux_time(start))}), '
                f'stop: time(v: {_flux_string(_flux_time(stop))}))\n'
                f'  |> filter(fn: (r) => '
                f'r.domain == {_flux_string(source_domain)} and '
                f'r.entity_id == {_flux_string(source_object_id)})\n'
                f'  |> set(key: "domain", value: {_flux_string(target_domain)})\n'
                f'  |> set(key: "entity_id", value: {_flux_string(target_object_id)})\n'
                f'  |> to(bucketID: {_flux_string(bucket_id)}, '
                f'orgID: {_flux_string(org_id)}, '
                f'tagColumns: ["domain", "entity_id"])'
            )
            client.query_api().query(query, org=self.settings["organization"])
        finally:
            client.close()

    def delete_records(
        self,
        entity_id: str,
        *,
        start: str,
        stop: str,
    ) -> None:
        client = self._new_client()
        try:
            client.delete_api().delete(
                start=_flux_time(start),
                stop=_flux_time(stop),
                predicate=_delete_predicate(entity_id),
                bucket=self.settings["bucket"],
                org=self.settings["organization"],
            )
        finally:
            client.close()
    def list_ledger_entries(self, registry_entry_id: str) -> list[LedgerEntry]:
        """Read migration ledger records for one stable registry identity."""
        client = self._new_client()
        try:
            query = (
                f'from(bucket: {_flux_string(self.settings["bucket"])})\n'
                f'  |> range(start: time(v: {_flux_string(_EPOCH)}))\n'
                f'  |> filter(fn: (r) => '
                f'r._measurement == {_flux_string(_LEDGER_MEASUREMENT)} and '
                f'r.registry_entry_id == {_flux_string(registry_entry_id)} and '
                'r._field == "record_count")\n'
                '  |> sort(columns: ["_time"])'
            )
            tables = client.query_api().query(
                query, org=self.settings["organization"]
            )
            result: list[LedgerEntry] = []
            seen: set[str] = set()
            for table in tables:
                for record in table.records:
                    values = record.values
                    record_id = str(values.get("record_id", ""))
                    if not record_id or record_id in seen:
                        continue
                    seen.add(record_id)
                    result.append(
                        {
                            "record_id": record_id,
                            "registry_entry_id": str(values.get("registry_entry_id", "")),
                            "timeline_event_id": str(values.get("timeline_event_id", "")),
                            "operation_id": str(values.get("operation_id", "")),
                            "kind": str(values.get("kind", "")),
                            "logical_entity_id": str(values.get("logical_entity_id", "")),
                            "physical_entity_id": str(values.get("physical_entity_id", "")),
                            "start": str(values.get("segment_start", "")),
                            "stop": str(values.get("segment_stop", "")),
                            "record_count": int(record.get_value() or 0),
                            "created_at": str(values.get("created_at", "")),
                        }
                    )
            result.sort(key=lambda item: item["created_at"])
            return result
        finally:
            client.close()

    def append_ledger_entry(self, entry: LedgerEntry) -> None:
        """Append one metadata point; retries with the same record ID are safe."""
        existing = self.list_ledger_entries(entry["registry_entry_id"])
        if any(item["record_id"] == entry["record_id"] for item in existing):
            return

        point = (
            Point(_LEDGER_MEASUREMENT)
            .tag("registry_entry_id", entry["registry_entry_id"])
            .tag("record_id", entry["record_id"])
            .tag("kind", entry["kind"])
            .tag("timeline_event_id", entry["timeline_event_id"])
            .tag("operation_id", entry["operation_id"])
            .tag("logical_entity_id", entry["logical_entity_id"])
            .tag("physical_entity_id", entry["physical_entity_id"])
            .tag("segment_start", entry["start"])
            .tag("segment_stop", entry["stop"])
            .tag("created_at", entry["created_at"])
            .field("record_count", int(entry["record_count"]))
            .time(datetime.fromisoformat(entry["created_at"]))
        )

        client = self._new_client()
        try:
            client.write_api(write_options=SYNCHRONOUS).write(
                bucket=self.settings["bucket"],
                org=self.settings["organization"],
                record=point,
            )
        finally:
            client.close()
