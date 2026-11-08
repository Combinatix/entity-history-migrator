# Orphanik — Recheck again

**Status:** Approved design requirement; not implemented in Entity History Migrator 0.7.12.

## Requirement

Every Orphan Detection Repair Issue must provide a **Recheck again** action.

## Why this is needed

Orphanik may report historical data for an Entity ID that is not currently present in the Home Assistant Entity Registry. Later, the user may resolve a related conflict in the main Worker, for example by choosing **Merge**. The Worker may move the history to the current Entity ID and remove the original records.

An Orphanik issue created by an earlier scan can then become stale.

## Required behavior

When the user selects **Recheck again**, Orphanik must perform a fresh check of the current state:

1. Check whether historical data still exists in the configured database for the affected Entity ID.
2. Compare that Entity ID with the current Home Assistant Entity Registry.
3. Update, keep open, or close the issue according to the fresh result.

**Recheck again must perform a new database and Registry comparison.** It must not merely change the issue state or rely on the result of an earlier scan.

If the check cannot be completed (for example, because the database is unavailable), Orphanik must not automatically close the issue. A failed or incomplete scan must be distinguished from a confirmed result that the Entity ID is no longer orphaned.

## Example scenario

1. The main Worker has an unresolved rename conflict.
2. Orphanik independently scans the database and finds an old Entity ID that is no longer in the Registry. It creates an Orphan Detection issue.
3. The user later resolves the Worker conflict using **Merge**.
4. The Worker moves the history to the new Entity ID and removes the original records.
5. The old Orphanik issue no longer reflects the actual database state.
6. The user selects **Recheck again**.
7. Orphanik scans the database and Registry again and closes the stale issue if the orphan condition no longer exists.

## Scope and boundaries

- Every Orphan Detection issue must provide **Recheck again**.
- This is a user-triggered recheck for a specific issue; it does not replace any periodic scan.
- Orphanik remains a separate process.
- Orphan detection must not use the migration Journal, Timeline, or Ledger to decide whether an Entity ID is currently orphaned.
- This requirement does not authorize Orphanik to delete or move history automatically.
