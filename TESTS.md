# Entity History Migrator — Test Register

**Last updated:** 2026-11-08 (0.7.13 package)  
**Latest packaged build:** `0.7.13` (static checks complete; HA runtime validation pending)  
**User-reported installed version before this build:** `0.7.12`  
**Last runtime-validated migration baseline:** `0.7.7`  
**Integration:** `entity_history_migrator`  
**Backend:** InfluxDB Flux  
**Runtime target in the recorded tests:** private production bucket (name redacted)  
**InfluxDB environment:** InfluxDB OSS 2.9.1; organization name omitted

This document is the persistent register of manual, runtime and static tests. It is not an automated test suite. The integration package currently contains no pytest or Home Assistant test suite.

The register deliberately distinguishes a test that was observed in a runtime log from a result confirmed by the user or checked only by static analysis. Do not promote a test to PASS merely because the corresponding feature exists in the source code.

## Status legend

| Status | Meaning |
|---|---|
| **PASS** | Successful runtime result is supported by a log, data export or other direct evidence. |
| **PASS — user confirmed** | The user explicitly ran the scenario and reported success; the available trace may not contain a dedicated event for it. |
| **PASS — static** | Source, schema, translations or isolated algorithm checks passed; this is not a full Home Assistant runtime test. |
| **PASS — historical** | A previous project checkpoint records the test as passed; the original detailed evidence is not included with this register. |
| **PARTIAL** | Part of the scenario passed, but the full behavior was not verified. |
| **FAIL** | The test exposed a defect in the listed version. Keep the failure in the history even if a later version fixes it. |
| **NOT TESTED** | No supporting test evidence is currently available. |
| **ACCEPTED LIMITATION** | The behavior is understood and intentionally not addressed in the current architecture. |
| **DEFERRED — ORPHANIK** | Detection is intentionally assigned to the future independent Orphanik process, not the migration Worker. |
| **COVERED — EH-XXX** | A formerly open follow-up scenario has now been exercised; see the linked runtime-test entry. |

## Runtime tests

| ID | Scenario | Version | Status | Result / evidence |
|---|---|---:|---|---|
| EH-001 | Normal rename and Ready migration | 0.7.2 | **PASS** | `counter.test_entity` → `counter.test_entity_renamed`. 4,410 Influx backend records (735 HA measurements) copied. COPY verification passed; source interval DELETE verification passed; source was empty afterward. Trace: 2026-11-08 09:13 UTC. |
| EH-002 | Rename back with two ownership intervals | 0.7.2 | **PASS** | `_bbb` → original entity. Two intervals, 4,410 + 552 = 4,962 backend records (827 HA measurements). Both intervals passed COPY and DELETE verification. Trace: 2026-11-08 09:20–09:21 UTC. |
| EH-003 | Multi-event timeline containing rename, delete and restore | 0.7.3 | **PASS** | Seven lifecycle events were processed: rename, rename, delete, restore, delete, restore, rename. Five ownership windows were derived: one Ready and four No source. 1,662 records in the Ready window were copied and source deletion verified. Trace: 2026-11-08 11:55 UTC. |
| EH-004 | Delete and restore of the same Registry entry | 0.7.3 | **PASS** | Delete/restore events were recorded against the stable `registry_entry_id`; restored ownership begins at the restore event, leaving the deletion gap unowned. The timeline and worker output show the resulting intervals. Trace: 2026-11-08 09:28–11:31 UTC. |
| EH-005 | Startup reconciliation of lifecycle changes missed while integration was unloaded | 0.7.3 | **PASS** | Runtime trace contains `REGISTRY RESTORE RECONCILED`, `REGISTRY RENAME RECONCILED` and `REGISTRY DELETE RECONCILED`. This verifies the observed individual reconciliation cases; it does **not** prove that multiple different renames while unloaded can be reconstructed. |
| EH-006 | Deletion Repair decision recording | 0.7.5–0.7.6 | **PASS** | Trace records `DELETION DECISION START` followed by `DELETION DECISION DONE` for Delete decisions. This confirms decision processing was completed; it should not be treated as a separate proof of every possible historical-data deletion boundary. |
| EH-007 | Rename analysis after the 0.7.5/0.7.6 worker regression | 0.7.6 | **FAIL** | Rename processing repeatedly failed before history analysis with `TypeError: delete_deletion_history() takes 3 positional arguments but 6 were given`. The same revision reached at least 11 retries. Trace: 2026-11-08 13:35–14:12 UTC. |
| EH-008 | Regression fix and recovery of the pending rename | 0.7.7 | **PASS** | After installing 0.7.7, the pending revision resumed at attempt 12, initialized the ledger, completed timeline analysis and exposed the expected conflict. No further `TypeError` occurred. The subsequent Merge completed. Trace: 2026-11-08 14:22 UTC onward. |
| EH-009 | Unknown target-history conflict detection | 0.7.7 | **PASS** | For `sensor.test_voltage`, analysis found 33,885 proven source records and 460,828 unknown target records. Both sides used unit `V`; result was `conflict`, with Merge allowed. Trace: 2026-11-08 14:22 UTC. |
| EH-010 | Merge of proven source history with existing target history | 0.7.7 | **PASS** | 33,885 source records copied into a target that already contained 460,828 records. Target verification reached 494,713 records; source interval was then deleted and verified empty. Trace: 2026-11-08 14:32 UTC. |
| EH-011 | Merge with multiple ownership windows and a small unknown target segment | 0.7.7 | **PASS** | In a later scenario, 494,713 proven source records were merged while 1,965 unknown target records were retained. A further 4,060-record known interval was also migrated. Final target count was 500,738; source intervals were verified empty. Trace: 2026-11-08 15:29 UTC. |
| EH-012 | Conflict Delete: remove unknown target history, then migrate proven history | 0.7.7 | **PASS** | 2,350 unknown target records were deleted and verified absent. Proven source intervals (496,678 + 4,060 + 90 records) were copied, verified and deleted from their source locations. Final target count was 500,828. Trace: 2026-11-08 15:31 UTC. |
| EH-013 | Archive conflicting history to a custom target | 0.7.7 | **PASS** | 41,380 unknown target records were copied to `sensor.test_voltage_archive`, verified, then removed from the original target. Proven source history was migrated to the selected target. Final target count: 500,898; archive count: 41,380. Trace: 2026-11-08 15:33 UTC. |
| EH-014 | Rename after archive and subsequent migration of known intervals | 0.7.7 | **PASS** | After the archive operation, a rename to the archive alias exposed the retained 41,380 records as unknown conflict history. A subsequent rename back to the original entity produced six Ready windows and migrated 501,008 proven records. Trace: 2026-11-08 15:34–15:36 UTC. |
| EH-015 | Ignore a conflict, then rename the entity again | 0.7.7 | **PASS — user confirmed** | The user tested: rename to an existing entity ID with unknown history → choose Ignore → rename again to a different ID. The user confirmed that ignored unknown history remained untouched and only known history was migrated. The supplied trace supports the later rename/migration behavior, but does not contain a dedicated `IGNORE` log event; the explicit Ignore result is recorded from the user's test report. |
| EH-016 | Measurement counts versus Influx backend record counts | 0.7.2–0.7.7 | **PASS** | Runtime analysis reports these separately. Examples: 4,410 records / 735 measurements; 4,962 / 827; 33,885 / 6,777. These are different quantities and must not be interchanged. |
| EH-017 | Same-unit history compatibility | 0.7.7 | **PASS** | The live Merge analysis compared source and target units `V` and allowed Merge. This verifies the compatible-unit path only. |
| EH-021 | Incompatible physical quantities block Merge; rename-back preserves the conflict | 0.7.7 | **PASS — user confirmed** | Source `sensor.test_current` (Current, `A`, 181 measurements) was renamed onto `sensor.test_voltage_archive` (Voltage, `V`, 8,276 unknown measurements). The Repairs dialog correctly identified physical quantities Current vs Voltage, showed `Units compatible: No`, and did not offer Merge. After renaming back to the original entity ID, the histories separated again and the conflicting target history remained unchanged. Evidence: user confirmation and screenshot a project screenshot (not included in this repository), 2026-11-08. |
| EH-022 | Rename A → B → A while the integration is unloaded | 0.7.7 | **FAIL** | The integration was disabled; `history_migrator_test` was renamed to `history_migrator_test_disaster` and then back to its original ID before the integration was re-enabled. Startup reconciliation did not record either intermediate rename and did not expose the intermediate history as an unknown-history conflict. This is expected to be unrecoverable from the final entity ID alone; the current startup check compares the last timeline alias with the current Registry alias. User-reported runtime test, 2026-11-08. |
| EH-023 | Deletion of an entity while the integration is unloaded | 0.7.7 | **FAIL** | The integration was disabled and `history_migrator_test2` was deleted. After re-enabling the integration, no deletion event was reconciled. Source review found that startup reconciliation skips deleted Registry entries when `timeline.latest_event(deleted_entry.id)` is `None`; the entity’s prior Timeline state has not yet been confirmed. User-reported runtime test, 2026-11-08. |
| EH-024 | Repairs confirmation flow: conflict actions and deleted-entity decisions | 0.7.8 | **NOT TESTED** | Verify Main dialog → confirmation dialog → Confirm executes the action and closes the flow without a third dialog. Cover Merge, Delete, Ignore, custom archive confirmation, deletion Delete and deletion Keep. Cancel must not save a decision or enqueue a write. |
| EH-025 | First seen / Last seen for every physical entity ID involved in a Repair | 0.7.8 | **NOT TESTED** | Verify timestamps come from complete InfluxDB history, are displayed in HA local time, and are shown per physical source alias plus target. Check conflict and deletion Repairs, including multiple source/physical IDs and entities with no history. Verify count format `181 measurements (1,234 records) · A`. |
| EH-026 | 0.7.10 Repairs text rendering and Confirm delete warning | 0.7.10 | **PARTIAL** | User deployed and reviewed 0.7.10. No further objections were reported except that the Rename historical data form still displayed the repeated Source / Target database history blocks. The user reported the rest of the dialog changes as acceptable. |
| EH-027 | 0.7.11 Repairs Entity ID / Entity History terminology and Rename historical data form cleanup | 0.7.11 | **PARTIAL — user confirmed** | The user deployed 0.7.11 and supplied screenshots confirming Old Entity ID / New Entity ID, the Old/New Entity History wording, the Archive Entity ID input, removal of Source / Target database history from the Rename historical data form, and correct visible line breaks. The deleted-entity Repair and custom-field validation were not explicitly exercised. |
| EH-028 | 0.7.12 concise confirmation-dialog wording and layout | 0.7.12 | **PARTIAL — user confirmed** | User reviewed the deleted-entity Confirm deletion dialog and approved its concise structure, counts, Entity ID, warning and Confirm / Back actions. The screenshot exposed that the proven time range was still displayed as raw UTC ISO timestamps; corrected in 0.7.13. Other confirmation dialogs were not all reviewed. |
| EH-029 | 0.7.12 custom archive confirmation Back navigation | 0.7.12 | **NOT TESTED** | From the final archive confirmation, select `← Back`. It must return to the Archive Entity ID form and retain the previously entered value. |
| EH-030 | 0.7.12 concise Rename historical data form description | 0.7.12 | **NOT TESTED** | Verify the custom archive form clearly describes archiving the existing target history, migrating source history to the target, archive verification/deletion order, and Archive Entity ID constraints. Confirm the source and target IDs remain on separate lines and the form field/validation are unchanged. |
| EH-031 | 0.7.13 local-time formatting for all Repairs date/time values | 0.7.13 | **NOT TESTED** | Review first/last-seen values and deletion ownership ranges on the initial issue, deletion review/failure, Confirm delete and Confirm keep screens. All must use the Home Assistant configured timezone and `YYYY-MM-DD HH:MM:SS TZ`, with no fractional seconds. Check both CET and CEST if practical. |
| EH-018 | Repairs first-screen rendering and labels | 0.7.3 | **PASS** | The user confirmed that after a browser refresh (F5), the deletion Repair displayed its title, description and readable action labels. The test dialog showed 2,296 measurements, 13,776 backend records, four ownership intervals and the Delete/Keep choices. The 0.7.4 confirmation-screen experiment was discarded and was not deployed. |
| EH-019 | Ownership algorithm scenarios | 0.7.7 audit | **PASS — static** | Separate algorithm checks covered normal rename, rename-back, delete/restore and invalid timeline continuity. These were isolated checks, not full Home Assistant integration tests. |
| EH-020 | Restart recovery after COPY-before-DELETE | 0.6.x | **PASS — historical** | The previous checkpoint records successful restart recovery for Ready and Merge writes after COPY but before source DELETE. This has not been independently repeated against 0.7.7. |

## Static and package checks

| ID | Check | Version | Status | Result / evidence |
|---|---|---:|---|---|
| ST-001 | Python syntax compilation | 0.7.7 | **PASS — static** | All 16 Python files passed `compileall`. |
| ST-002 | JSON validity | 0.7.7 | **PASS — static** | `manifest.json`, `strings.json` and `translations/en.json` parse as valid JSON. |
| ST-003 | Translation consistency | 0.7.7 | **PASS — static** | `strings.json` and `translations/en.json` are identical; required Repairs placeholders are present. |
| ST-004 | Version consistency | 0.7.7 | **PASS — static** | `manifest.json` and `const.py` both declare `0.7.7`. |
| ST-005 | Package contents | 0.7.7 | **PASS — static** | Expected integration modules and brand assets are present in the installation ZIP. |
| ST-006 | TODO/FIXME/XXX scan | 0.7.7 | **PASS — static** | No such markers were found. The audit did note one apparently unused import: `STATUS_FAILED` in `engine/worker.py`. |
| ST-007 | Full Home Assistant import/integration test suite | 0.7.7 | **NOT TESTED** | The audit environment did not contain Home Assistant Core, `influxdb-client` or `voluptuous`. Full imports and HA integration tests could not be run. |
| ST-008 | Full-history timestamp query and local-time formatting | 0.7.8 | **PASS — static** | Added a read-only backend method that obtains min/max `_time` for each exact physical entity ID across the full bucket history; Repair formatting converts UTC ISO timestamps to the HA local timezone. No live InfluxDB query was available in the build environment. |
| ST-009 | Repairs flow and translation consistency | 0.7.8 | **PASS — static** | All conflict actions now have an explicit confirmation screen; deleted-entity Keep now also has a confirmation screen. Confirmation buttons are labelled `Confirm` and cancellation returns to the main Repair menu. The accepted action closes the flow; translations are present and `strings.json` matches `translations/en.json`. Runtime behavior remains EH-024. |
| ST-010 | Python syntax, JSON and version consistency | 0.7.8 | **PASS — static** | `compileall` passed; `manifest.json`, `strings.json` and `translations/en.json` parse; translation files are byte-identical; manifest and `const.py` both declare `0.7.8`. |

| ST-011 | 0.7.9 Repairs wording, confirmation history-block removal and Back navigation | 0.7.9 | **FAIL** | The static checks missed literal `\n` sequences in the main conflict dialog description. Home Assistant displayed the escape sequences instead of line breaks. Corrected in 0.7.10. |
| ST-012 | 0.7.11 Repairs terminology, custom-dialog cleanup and translation consistency | 0.7.11 | **PASS — static** | `Old Entity ID`, `New Entity ID`, `Old Entity History`, `New Entity History`, `Archive Entity ID`, `Archive Entity History` and `Deleted Entity ID` are used consistently in the relevant Repairs text. The Rename historical data description contains no Source / Target database history blocks or their placeholders. JSON parses and English translation files are identical. Runtime display remains EH-027. |
| ST-013 | 0.7.12 Python syntax, JSON, translation, placeholder and package checks | 0.7.12 | **PASS — static** | All Python files compile; `manifest.json`, `strings.json` and `translations/en.json` parse; the English files are byte-identical; manifest and `const.py` agree on 0.7.12; all revised confirmation placeholders are supplied; ZIP integrity passes. |
| ST-014 | 0.7.12 Repairs translation-key and menu-option audit | 0.7.12 | **PASS — static** | The custom archive confirmation now requests `back_to_custom`, matching its translation and `async_step_back_to_custom` handler. `deletion_review` and `deletion_failed` are retained because the flow selects them dynamically from persisted request status. No unused Repairs menu-option translation was found after the correction. |
| ST-015 | 0.7.13 Repairs timestamp display consistency | 0.7.13 | **PASS — static** | First/last-seen and deletion range start/stop placeholders all use the same `_format_repairs_timestamp` helper, which parses ISO values, converts them with Home Assistant `dt_util.as_local`, and formats `%Y-%m-%d %H:%M:%S %Z`. Invalid values display `Unavailable`; internal UTC storage and logs are unchanged. |

## Follow-up regression tests

These are intentionally not marked as failures. They are gaps in the current test evidence or future capabilities. A row marked `COVERED` has since been exercised and points to its completed runtime-test entry above.

| ID | Scenario | Status | Notes |
|---|---|---|---|
| FUT-001 | Orphanik: enumerate every physical `domain/entity_id` in InfluxDB and compare it with the current Entity Registry | **NOT TESTED** | Orphanik is only a proposal; no implementation exists yet. |
| FUT-002 | Orphanik: retrieve actual first and last sample timestamps for each physical entity | **NOT TESTED** | Must use actual Influx `_time` values, not ownership boundaries or ledger `created_at`. |
| FUT-003 | Storage identity mismatch between an existing journal and a newly selected InfluxDB target | **NOT TESTED** | Rename journal entries are not yet bound to the selected storage identity. This is an audit finding and requires a future fix/test. |
| FUT-004 | Multiple different renames while the integration is unloaded | **ACCEPTED LIMITATION** | Tested A → B → A while unloaded. The final Registry state cannot reveal the intermediate alias. The user explicitly chose to retain this limitation and not complicate the main Worker. |
| FUT-005 | Merge blocked for incompatible physical units | **COVERED — EH-021** | Confirmed in 0.7.7: Current (`A`) versus Voltage (`V`) blocked Merge; rename-back preserved the conflicting history unchanged. |
| FUT-006 | Restart recovery during Delete, conflict Delete and Archive writes | **NOT TESTED** | Historical restart recovery evidence covers Ready and Merge only. |
| FUT-007 | Entity Registry deletion/restore racing with a write already in progress | **NOT TESTED** | Audit identified a possible journal-state consistency race; no runtime reproduction is recorded. |
| FUT-008 | Revalidate a custom archive entity ID if Home Assistant claims it after approval but before writing | **NOT TESTED** | Audit identified that the worker rechecks Influx emptiness but not Registry ownership immediately before the first archive write. |
| FUT-009 | User-facing recovery flow for a persisted failed write | **NOT TESTED** | Failed writes are not automatically retried; no dedicated recovery Repair exists yet. |
| FUT-010 | Trace-file failure (disk full / open error) must not interrupt a database operation | **NOT TESTED** | Audit identified that trace I/O errors are not fully isolated. |
| FUT-011 | Malformed individual timeline or journal rows | **NOT TESTED** | Top-level storage validation exists, but per-row schema validation is incomplete. |
| FUT-012 | Enforce all declared backend capabilities before using a backend operation | **NOT TESTED** | The engine checks only some declared capabilities. |
| FUT-013 | Display temporal overlap between history ranges | **NOT TESTED** | Not included in the 0.7.8 UI scope. First seen / Last seen display is implemented in 0.7.8; overlap visualization remains a separate possible enhancement. |
| FUT-014 | Detect orphaned history left by entity deletion during downtime | **DEFERRED — ORPHANIK** | The main Worker will not be changed to reconstruct missed lifecycle events. The future independent Orphanik process will compare InfluxDB entity IDs against the current Entity Registry and report orphaned history. |
| FUT-015 | Orphanik: Recheck again on every Orphan Detection Issue | **DEFERRED — ORPHANIK** | Each Issue must offer a manual recheck that performs a fresh database inventory vs current HA Entity Registry comparison. Update or close the Issue only from the new result; a failed/incomplete scan must not close it. See [`docs/ORPHANIK_RECHECK.md`](docs/ORPHANIK_RECHECK.md). |
| FUT-016 | Rename historical data: fixed Entity ID domain in Archive Entity ID field | **PLANNED — NEXT VERSION** | Match the standard Home Assistant entity-ID editor style: display the domain (e.g. `sensor.`) as a non-editable prefix and allow editing only the object ID. Pre-fill the current suggested archive name; combine the fixed domain and entered object ID before running all existing full-ID, domain, Registry, Timeline and InfluxDB availability checks. Use a native Home Assistant selector if it supports this in a Repairs form, without a custom frontend. Copy / Restore icons are optional polish, not a functional requirement. |

## Version 0.7.9 — Repairs UI refinement build

**Scope:** Follow-up wording and layout refinements requested after reviewing the 0.7.8 screenshots. No migration engine behavior changed.

- Main conflict dialog heading is `Existing history conflicts with the new entity ID`.
- Old/new entity summaries retain the format `measurements (records) · unit` and show First seen / Last seen on separate lines.
- Confirmation dialogs no longer repeat Source / Target / Database history blocks; concise operation descriptions and counts remain.
- Navigation from confirmation screens is labelled `← Back` and returns to the relevant prior screen.
- The concise Merge confirmation wording requested by the user is retained.
- Worker, Timeline, Journal, Ledger and startup reconciliation remain unchanged.
- Runtime validation is pending; see EH-024 and EH-025.

**Scope:** Repairs dialogs and read-only history timestamp display. The migration Worker, Timeline, Journal, Ledger and startup reconciliation were not changed.

- Standardized conflict and deletion decision flows as Main dialog → confirmation dialog → Confirm/Cancel.
- Added an explicit confirmation step for keeping deleted-entity history; it no longer records Keep immediately from the main menu.
- Confirmation actions close the Repair flow after the decision is accepted; no additional success dialog is requested by the integration.
- Added per-physical-entity First seen / Last seen values from the full InfluxDB history, displayed in the Home Assistant local timezone.
- Added measurement and backend record counts in the form `181 measurements (1,234 records) · A`.
- Custom archive selection still requires its entity-ID input form, followed by a final confirmation dialog.
- Live Home Assistant / InfluxDB runtime validation is pending in EH-024 and EH-025.

## Version 0.7.10 — Repairs wording and terminology correction

**Scope:** Repairs UI text and labels only. No migration algorithm or database write behavior changed.

- Fixed the main conflict dialog description: literal `\\n` sequences are now actual line breaks.
- Reworded the conflict **Confirm delete** warning: pressing **Confirm** explicitly deletes the existing conflicting history and then migrates the source history.
- Replaced ambiguous physical-quantity `Unknown` labels with `Not identified`.
- Displayed `No unit` when the backend metadata confirms that no unit is configured. Missing unit metadata remains `Not identified`.
- Added `Dimensionless` for recognized percentage/ratio/dimensionless unit classes and `Count` for an explicitly identified count unit. A missing unit is never treated as proof of a dimensionless quantity.
- Kept the existing `← Back` navigation unchanged.
- Worker, Timeline, Journal, Ledger, backend write workflows and startup reconciliation remain unchanged.
- Orphanik `Recheck again` remains deferred design work; it is documented separately and is not implemented in this build.
- The user subsequently deployed and reviewed 0.7.10. The main dialog wording and Confirm delete text were accepted; the Rename historical data form still displayed Source / Target database history, recorded as the remaining issue in EH-026 and corrected in 0.7.11.

## Version 0.7.11 — Repairs terminology and custom-dialog cleanup

**Scope:** Repairs wording only. No migration algorithm, database write behavior or lifecycle processing changed.

- Replaced the ambiguous `Old name` / `New name` labels with `Old Entity ID` / `New Entity ID`.
- Standardized history terminology as `Old Entity History`, `New Entity History` and `Archive Entity History` where history is named explicitly.
- Renamed the custom archive input label and related validation wording to `Archive Entity ID`.
- Changed deleted-entity Repair headings to `Deleted Entity ID`.
- Removed the repeated `Target database history` and `Source database history` blocks from the **Rename historical data** form. The operation summary, custom Archive Entity ID input, validation and final confirmation remain.
- Preserved the approved line-break layout and the existing `← Back` navigation.
- Worker, Timeline, Journal, Ledger, backend write workflows and startup reconciliation remain unchanged.
- Orphanik and its `Recheck again` action remain deferred and are not implemented in this build.
- Home Assistant runtime validation is pending in EH-027.

## Version 0.7.12 — concise confirmation dialogs and translation cleanup

**Scope:** Repairs confirmation wording and one custom-archive navigation correction. No migration algorithm, database write behavior, Worker, Timeline, Journal, Ledger or startup-reconciliation behavior changed.

- Shortened Confirm merge, Confirm delete, Confirm ignore, Confirm archive and migration, deleted-entity Confirm delete, and deleted-entity Confirm keep.
- Kept measurement counts, backend record counts, units, relevant Entity IDs, proven interval counts/time spans and destructive warnings.
- Removed redundant prose such as repeated `Old Entity ID` / `New Entity ID` labels when the ID is already shown on its own line.
- Corrected the custom archive confirmation menu to use `back_to_custom`, matching the existing translation and `async_step_back_to_custom` handler. Back now returns to the Archive Entity ID input form; runtime verification is EH-029.
- Removed unused translation placeholders from the Repairs flow; the required `registry_entry_id` remains in deletion issue data and queued deletion requests.
- Audited Repairs translation keys. `deletion_review` and `deletion_failed` are valid dynamic step translations, not unused strings.
- Updated `strings.json` and `translations/en.json` together; they are byte-identical.
- No Orphanik or storage-identity GUID implementation is included.
- Home Assistant runtime validation of the 0.7.12 Repairs screens is pending (EH-028 and EH-029).

## Version 0.7.13 — consistent local timestamps in Repairs

**Scope:** User-facing date/time formatting only. No migration, ownership, InfluxDB query/write, Worker, Timeline, Journal, or Ledger behavior changed.

- Unified all Repairs timestamp displays through `_format_repairs_timestamp`.
- First seen / Last seen and deletion ownership range start / stop now use Home Assistant's configured local timezone.
- Display format is `YYYY-MM-DD HH:MM:SS TZ` (for example, `2026-11-08 22:44:24 CET`); fractional seconds are omitted.
- Invalid or unparsable timestamps display `Unavailable` instead of exposing a raw ISO/UTC value.
- Internal timestamps in the Timeline, Journal, Ledger, InfluxDB backend and machine-readable worker logs remain UTC.
- Static validation passed; Home Assistant runtime review is pending (EH-031).

## Important test-environment note

Some historical write tests used a production bucket rather than a disposable test bucket. The exact bucket and organization names are intentionally omitted. For any new write behavior, test first against a dedicated disposable bucket and only consider a production bucket after verification.

## Evidence files and references

The register was prepared from private runtime logs, journal/timeline snapshots, project audit notes and the user's test reports. Those evidence files are intentionally not included in this public repository because they may contain installation-specific identifiers and operational data.

## How to add future test results

1. Assign the next unused stable ID (`EH-027`, `ST-013`, `FUT-016`, etc.). Do not recycle old IDs.
2. Record the exact integration version and test date.
3. Describe the initial state and action, not just the feature name.
4. Record expected and actual results, including record and measurement counts where relevant.
5. Link or name the supporting log, JSON export, screenshot or user confirmation.
6. Keep previous failures in the register and add a separate row for the version that fixes them.
7. Use **NOT TESTED** when there is no evidence. Do not infer a PASS from static code inspection.

### New test entry template

```markdown
| ID | Scenario | Version | Status | Result / evidence |
|---|---|---:|---|---|
| EH-XXX | Short, reproducible scenario | 0.x.x | NOT TESTED | Expected result; actual result; evidence filename and timestamp. |
```

---

**Current conclusion:** Version 0.7.7 remains the last runtime-validated migration baseline. Version 0.7.11's Repairs terminology and Rename historical data cleanup were visually reviewed by the user (partial coverage in EH-027). Version 0.7.12 shortens Repairs dialogs and corrects custom archive Back navigation. Version 0.7.13 unifies user-facing Repairs timestamps in the Home Assistant local timezone; runtime review is pending (EH-031). The migration Worker, Timeline, Journal, Ledger, backend write workflows and startup reconciliation remain unchanged. Orphanik and storage-identity protection are not implemented.
