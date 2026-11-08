#!/usr/bin/env python3
"""Run lightweight static checks for the repository."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components" / "entity_history_migrator"


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(path: Path) -> Any:
    return json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=_no_duplicate_keys,
    )


def check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)
    print(f"PASS  {message}")


def main() -> int:
    required = [
        COMPONENT / "__init__.py",
        COMPONENT / "manifest.json",
        COMPONENT / "const.py",
        COMPONENT / "strings.json",
        COMPONENT / "translations" / "en.json",
    ]
    for path in required:
        check(path.is_file(), f"required file exists: {path.relative_to(ROOT)}")

    manifest_path = COMPONENT / "manifest.json"
    strings_path = COMPONENT / "strings.json"
    translation_path = COMPONENT / "translations" / "en.json"

    manifest = load_json(manifest_path)
    load_json(strings_path)
    load_json(translation_path)
    load_json(ROOT / "hacs.json")
    check(True, "all repository JSON files parse without duplicate keys")

    check(
        strings_path.read_bytes() == translation_path.read_bytes(),
        "strings.json and translations/en.json are byte-identical",
    )

    domain = manifest.get("domain")
    check(
        domain == COMPONENT.name,
        "manifest domain matches the custom component directory",
    )

    version = manifest.get("version")
    const_text = (COMPONENT / "const.py").read_text(encoding="utf-8")
    match = re.search(r"^VERSION\s*=\s*['\"]([^'\"]+)['\"]", const_text, re.MULTILINE)
    check(match is not None, "const.py declares VERSION")
    check(match.group(1) == version, "manifest.json and const.py versions match")

    repairs_text = (COMPONENT / "repairs.py").read_text(encoding="utf-8")
    check(
        'def _format_repairs_timestamp(' in repairs_text
        and 'dt_util.as_local(parsed).strftime("%Y-%m-%d %H:%M:%S %Z")' in repairs_text,
        "Repairs timestamp formatter uses HA local timezone and consistent display format",
    )
    check(
        '"start": _format_repairs_timestamp(str(summary["start"]))' in repairs_text
        and '"stop": _format_repairs_timestamp(str(summary["stop"]))' in repairs_text,
        "deletion range start/stop use the shared Repairs timestamp formatter",
    )
    check(
        "_format_repairs_timestamp(first)" in repairs_text
        and "_format_repairs_timestamp(last)" in repairs_text,
        "first/last-seen values use the shared Repairs timestamp formatter",
    )

    python_files = sorted(COMPONENT.rglob("*.py")) + [Path(__file__)]
    for path in python_files:
        source = path.read_text(encoding="utf-8")
        compile(source, str(path), "exec")
    check(True, f"Python syntax compiles ({len(python_files)} files)")

    print(f"\nStatic validation passed for Entity History Migrator {version}.")
    print("This does not replace Home Assistant runtime or InfluxDB write testing.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as err:  # noqa: BLE001
        print(f"FAIL  {err}", file=sys.stderr)
        raise SystemExit(1) from err
