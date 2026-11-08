# Changelog

Notable changes to Entity History Migrator are recorded here.

## 0.7.13 — 2026-11-08

- Unified all Repairs date/time displays in the Home Assistant configured local timezone.
- First seen / Last seen and deletion ownership range start / stop now share one formatter.
- Display format: `YYYY-MM-DD HH:MM:SS TZ`; fractional seconds are omitted.
- Invalid timestamps display `Unavailable` instead of raw ISO values.
- Internal timestamps and machine-readable logs remain UTC.

## 0.7.12 — 2026-11-08

- Shortened all Repairs confirmation descriptions: Merge, Delete, Ignore, Archive, deleted-entity Delete and Keep.
- Shortened the Rename historical data form description while retaining archive verification and Archive Entity ID validation requirements.
- Preserved measurement counts, backend record counts, units, relevant Entity IDs, proven interval scope and destructive warnings.
- Fixed the custom archive confirmation Back action to return to the Archive Entity ID form using the existing `back_to_custom` translation and handler.
- Removed unused Repairs translation placeholders.
- Audited Repairs translation keys; retained dynamically selected `deletion_review` and `deletion_failed` steps.
- No migration engine or database-write behavior changed.

## 0.7.11 — 2026-11-08

- Standardized Repairs terminology around `Old Entity ID`, `New Entity ID`, `Old Entity History`, `New Entity History`, `Archive Entity ID`, `Archive Entity History` and `Deleted Entity ID`.
- Removed repeated Source / Target database history blocks from the Rename historical data form.
- No migration engine or database-write behavior changed.

## 0.7.10 — 2026-11-08

- Fixed literal newline escapes in the main conflict Repair description.
- Clarified the destructive Confirm delete warning.
- Used `Not identified` for an unidentified physical quantity and `No unit` only when no unit is configured.
- No migration engine or database-write behavior changed.
