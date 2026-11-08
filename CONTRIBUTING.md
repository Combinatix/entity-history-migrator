# Contributing

Thank you for helping improve Entity History Migrator.

## Before making changes

- Do not commit Home Assistant secrets, InfluxDB tokens, credentials, `.storage` files, database exports, runtime logs, journal/timeline snapshots, or production entity identifiers.
- Use a disposable InfluxDB bucket for write-operation testing.
- Keep migration-engine changes separate from Repairs wording changes where possible.

## Repairs text and translations

- `strings.json` is the source English text.
- `translations/en.json` must remain byte-identical to `strings.json`.
- Existing named placeholders such as `{source_label}` are supplied by Python. You may reorder existing placeholders in text, but do not rename or invent them without changing and reviewing the corresponding Python flow.
- In JSON strings, encode line breaks as `\n`. Validate the rendered Markdown in Home Assistant Repairs; JSON validity alone does not prove that the layout is correct.

## Version and release housekeeping

When changing the integration version, update both:
- `custom_components/entity_history_migrator/manifest.json`
- `custom_components/entity_history_migrator/const.py`

Also update `CHANGELOG.md` and add or update entries in `TESTS.md`. Keep previous failures and accepted limitations in the test register; do not silently rewrite history.

## Validation

Run:

```bash
python scripts/validate.py
```

This is static validation only. It does not replace Home Assistant runtime testing or verification against InfluxDB.
