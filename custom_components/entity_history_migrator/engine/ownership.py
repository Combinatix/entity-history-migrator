"""Pure helpers for deriving proven Entity Registry ownership intervals."""

from __future__ import annotations

from typing import Any, TypedDict

EPOCH = "1970-01-01T00:00:00.000000Z"


class OwnershipInterval(TypedDict):
    """One half-open interval owned by one entity ID."""

    entity_id: str
    start: str
    stop: str | None
    baseline: bool


class OwnershipTimelineError(ValueError):
    """Lifecycle events do not form a continuous ownership history."""


def derive_ownership_intervals(
    events: tuple[dict[str, Any], ...] | list[dict[str, Any]],
) -> tuple[str | None, list[OwnershipInterval]]:
    """Derive closed and current ownership intervals from lifecycle facts.

    A deletion closes an interval. A restore of the same registry entry starts
    a new interval, while retaining the earlier intervals as belonging to the
    same stable identity. The gap while the entity was absent is never owned.
    """
    if not events:
        raise OwnershipTimelineError("Timeline has no lifecycle events")

    ordered = tuple(sorted(events, key=lambda item: str(item["event_timestamp"])))
    first = ordered[0]
    first_type = str(first.get("event_type", "rename"))

    if first_type == "rename":
        active_entity_id = str(first.get("previous_entity_id", first.get("old_entity_id", "")))
        active_start = str(first.get("ownership_start") or EPOCH)
        baseline = first.get("ownership_start") is None
    elif first_type == "delete":
        active_entity_id = str(first.get("entity_id", ""))
        active_start = str(first.get("ownership_start") or EPOCH)
        baseline = False
    else:
        raise OwnershipTimelineError(
            f"Timeline cannot start with lifecycle event {first_type!r}"
        )

    if not active_entity_id:
        raise OwnershipTimelineError("Timeline does not identify its initial entity")

    intervals: list[OwnershipInterval] = []
    previous_timestamp: str | None = None
    previous_event: dict[str, Any] | None = None

    for event in ordered:
        event_type = str(event.get("event_type", "rename"))
        timestamp = str(event["event_timestamp"])

        if previous_timestamp is not None and timestamp < previous_timestamp:
            raise OwnershipTimelineError(
                "Lifecycle timestamps are not chronological"
            )

        if event_type == "rename":
            previous_entity_id = str(
                event.get("previous_entity_id", event.get("old_entity_id", ""))
            )
            new_entity_id = str(event.get("entity_id", event.get("new_entity_id", "")))
            if active_entity_id is None or previous_entity_id != active_entity_id:
                raise OwnershipTimelineError(
                    "Timeline discontinuity: expected current alias "
                    f"{active_entity_id!r}, got rename source {previous_entity_id!r}"
                )
            if active_start > timestamp:
                raise OwnershipTimelineError("Ownership interval starts after rename")
            intervals.append(
                {
                    "entity_id": active_entity_id,
                    "start": active_start,
                    "stop": timestamp,
                    "baseline": baseline,
                }
            )
            active_entity_id = new_entity_id
            active_start = timestamp
            baseline = False

        elif event_type == "delete":
            deleted_entity_id = str(event["entity_id"])
            if active_entity_id != deleted_entity_id:
                raise OwnershipTimelineError(
                    "Timeline deletion does not match the active entity: "
                    f"{deleted_entity_id!r} != {active_entity_id!r}"
                )
            if active_start > timestamp:
                raise OwnershipTimelineError("Ownership interval starts after deletion")
            intervals.append(
                {
                    "entity_id": active_entity_id,
                    "start": active_start,
                    "stop": timestamp,
                    "baseline": baseline,
                }
            )
            active_entity_id = None
            active_start = None
            baseline = False

        elif event_type == "restore":
            restored_entity_id = str(event["entity_id"])
            if active_entity_id is not None:
                raise OwnershipTimelineError(
                    "Restore encountered while an entity ownership interval is active"
                )
            if previous_event is None or str(previous_event.get("event_type")) != "delete":
                raise OwnershipTimelineError(
                    "Restore does not immediately follow a deletion for this registry identity"
                )
            active_entity_id = restored_entity_id
            active_start = timestamp
            baseline = False

        else:
            raise OwnershipTimelineError(
                f"Unsupported lifecycle event type: {event_type!r}"
            )

        previous_timestamp = timestamp
        previous_event = event

    if active_entity_id is not None and active_start is not None:
        intervals.append(
            {
                "entity_id": active_entity_id,
                "start": active_start,
                "stop": None,
                "baseline": baseline,
            }
        )

    return active_entity_id, intervals


def closed_ownership_intervals(
    events: tuple[dict[str, Any], ...] | list[dict[str, Any]],
) -> list[OwnershipInterval]:
    """Return only closed ownership intervals for database operations."""
    _, intervals = derive_ownership_intervals(events)
    return [item for item in intervals if item["stop"] is not None]
