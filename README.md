# Entity History Migrator

A custom integration for [Home Assistant](https://www.home-assistant.io/) that tracks entity lifecycle changes and helps reconcile historical data stored in InfluxDB.

**Current source version: 0.7.13**  
**Status:** Active development. See [`TESTS.md`](TESTS.md) for the distinction between verified runtime behavior, static checks, and pending tests.

## What it does

- Tracks Home Assistant Entity Registry lifecycle events relevant to history ownership, including rename, deletion, and restore.
- Uses the configured Home Assistant InfluxDB v2 connection and the InfluxDB Flux API to inspect historical data.
- Maintains a lifecycle Timeline, processing Journal, and migration Ledger.
- Identifies history that can be attributed to a specific entity lifecycle interval.
- Presents Repairs issues when existing history conflicts with a renamed entity.
- Provides user-approved conflict actions such as Merge, Delete, Rename historical data (archive), and Ignore.
- Can offer a separate deleted-entity history decision when that option is enabled.

The integration does not attempt to infer ownership for history that it cannot prove. Such data is treated as unknown/conflicting history rather than silently overwritten.

## Requirements

- Home Assistant.
- The Home Assistant **InfluxDB** integration configured for an InfluxDB v2 connection.
- Exactly one enabled Home Assistant InfluxDB v2 connection available to this integration. The config flow aborts if none or more than one are found.
- The configured credentials must be able to access the selected organization and bucket.

The integration reuses the existing Home Assistant InfluxDB configuration. It does not ask the user to enter or duplicate the InfluxDB token.

The Python dependency `influxdb-client==1.50.0` is declared in `manifest.json` and is installed by Home Assistant.

## Installation

### Manual installation

1. Copy the `custom_components/entity_history_migrator` directory from this repository into your Home Assistant configuration directory:
   ```text
   /config/custom_components/entity_history_migrator/
   ```
2. Restart Home Assistant.
3. Open **Settings → Devices & services → Add Integration**.
4. Search for **Entity History Migrator** and follow the setup flow.

The repository uses the standard `custom_components` layout. A `hacs.json` metadata file is included, but this repository has not been submitted to or independently validated by HACS.

## Configuration options

The initial setup asks for a settling delay after a rename (0–3600 seconds; default 60 seconds).

The integration options also include:

- **Trace logging** — writes a trace log to `/config/entity_history_migrator.log`.
- **Create a Repair issue when an entity is deleted** — enables the deleted-entity history decision issue.

## Data safety

**This integration can modify and delete historical records in InfluxDB.**

- Back up the target bucket before using migration actions.
- Initially test against a separate, disposable InfluxDB bucket.
- Review the counts, Entity IDs, units, and time ranges shown in Repairs before confirming an operation.
- Do not treat a successful static validation or a previous test as proof that a new database operation is safe in your installation.

Merge is offered only when the integration considers the physical quantities and units compatible. Delete and archive operations have their own confirmation flows.

## Repository layout

```text
.
├── .github/workflows/validate.yml
├── custom_components/
│   └── entity_history_migrator/
├── docs/
│   └── ORPHANIK_RECHECK.md
├── scripts/
│   └── validate.py
├── CHANGELOG.md
├── CONTRIBUTING.md
├── README.md
├── TESTS.md
└── hacs.json
```

## Development and validation

Run the repository's static validation script with Python 3.12 or newer:

```bash
python scripts/validate.py
```

It checks Python syntax, JSON validity, equality of `strings.json` and `translations/en.json`, and consistency between the manifest and Python version constants.

GitHub Actions runs the same checks on pushes and pull requests.

This repository does **not** currently contain a full pytest or Home Assistant integration test suite. Manual runtime test coverage and outstanding checks are recorded in [`TESTS.md`](TESTS.md).

## Roadmap note

Orphanik is a proposed future, separate orphan-detection process. It is not implemented in version 0.7.12. The accepted **Recheck again** requirement is documented in [`docs/ORPHANIK_RECHECK.md`](docs/ORPHANIK_RECHECK.md).

## License

Copyright 2026 Jiri Kosar, Licensed under the Apache License, Version 2.0.

