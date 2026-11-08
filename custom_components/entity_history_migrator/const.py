"""Constants for Entity History Migrator."""

from homeassistant.const import Platform

DOMAIN = "entity_history_migrator"
VERSION = "0.7.13"

PLATFORMS = [Platform.SENSOR]

TIMELINE_STORAGE_VERSION = 1
TIMELINE_STORAGE_KEY = f"{DOMAIN}.timeline"

JOURNAL_STORAGE_VERSION = 1
JOURNAL_STORAGE_KEY = f"{DOMAIN}.journal"
JOURNAL_SCHEMA = "timeline-processing-v1"

STATUS_PENDING = "pending"
STATUS_ANALYZING = "analyzing"
STATUS_RETRY = "retry"
STATUS_READY = "ready"
STATUS_MIGRATING = "migrating"
STATUS_COMPLETED = "completed"
STATUS_CONFLICT = "conflict"
STATUS_NO_SOURCE = "no_source"
STATUS_FAILED = "failed"
STATUS_SUPERSEDED = "superseded"
STATUS_APPROVED_MERGE = "approved_merge"
STATUS_APPROVED_DELETE = "approved_delete"
STATUS_APPROVED_CUSTOM_TARGET = "approved_custom_target"
STATUS_IGNORED = "ignored"

CONF_BACKEND_TYPE = "backend_type"
CONF_BACKEND_CONFIG = "backend_config"
BACKEND_INFLUXDB = "influxdb"  # legacy 0.6.0 config value
BACKEND_INFLUXDB_FLUX = "influxdb_flux"

CONF_MIGRATION_DELAY = "migration_delay_seconds"
CONF_TRACE_LOG_ENABLED = "trace_log_enabled"
CONF_CREATE_DELETION_REPAIR = "create_deletion_repair"

DEFAULT_MIGRATION_DELAY = 60
DEFAULT_TRACE_LOG_ENABLED = False
DEFAULT_CREATE_DELETION_REPAIR = False

INTER_JOB_DELAY = 10
WORKER_POLL_INTERVAL = 5
MIGRATION_TIMEOUT = 180


RETRY_BASE_SECONDS = 30
RETRY_MAX_SECONDS = 300

TRACE_LOG_FILENAME = "entity_history_migrator.log"
TRACE_LOG_MAX_BYTES = 2 * 1024 * 1024
