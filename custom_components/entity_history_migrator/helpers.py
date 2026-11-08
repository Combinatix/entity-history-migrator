"""Small backend-neutral helpers."""

from __future__ import annotations


def split_entity_id(entity_id: str) -> tuple[str, str]:
    """Split a valid Home Assistant entity ID into domain and object ID."""
    domain, object_id = entity_id.split(".", 1)
    return domain, object_id
