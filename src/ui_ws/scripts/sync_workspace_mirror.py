#!/usr/bin/env python3
"""
Synchronize/verify nested workspace mirror links.

Layout this script manages:
- Top-level source packages:   <workspace>/src/<pkg>
- Nested mirror workspace:     <workspace>/src/ui_ws/src/<pkg>  (symlink -> ../../<pkg>)

This prevents silent skew when the nested mirror drifts from top-level packages.
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path
from typing import Dict, List, Tuple


def _discover_top_packages(top_src: Path) -> Dict[str, Path]:
    packages: Dict[str, Path] = {}
    if not top_src.exists():
        return packages
    for entry in sorted(top_src.iterdir()):
        if not entry.is_dir():
            continue
        name = entry.name
        if name.startswith(".") or name == "ui_ws":
            continue
        if (entry / "package.xml").is_file():
            packages[name] = entry.resolve()
    return packages


def _expected_link_target(link_path: Path, package_path: Path) -> str:
    return os.path.relpath(str(package_path), str(link_path.parent))


def _check_mirror(
    top_packages: Dict[str, Path],
    nested_src: Path,
    allow_missing_mirror: bool,
) -> Tuple[bool, List[str]]:
    messages: List[str] = []
    if not nested_src.exists():
        if allow_missing_mirror:
            messages.append(f"Nested mirror missing (allowed): {nested_src}")
            return True, messages
        messages.append(f"Nested mirror missing: {nested_src}")
        return False, messages

    ok = True
    for pkg_name, pkg_path in top_packages.items():
        mirror_link = nested_src / pkg_name
        if not mirror_link.exists() and not mirror_link.is_symlink():
            ok = False
            messages.append(f"Missing mirror link: {mirror_link}")
            continue
        if not mirror_link.is_symlink():
            ok = False
            messages.append(f"Mirror entry is not a symlink: {mirror_link}")
            continue

        expected = _expected_link_target(mirror_link, pkg_path)
        actual = os.readlink(str(mirror_link))
        if actual != expected:
            ok = False
            messages.append(
                f"Mirror link target mismatch: {mirror_link} -> {actual} (expected {expected})"
            )

    for entry in sorted(nested_src.iterdir()):
        if entry.name.startswith(".") or entry.name == "COLCON_IGNORE":
            continue
        if entry.name not in top_packages:
            ok = False
            messages.append(f"Unexpected mirror entry: {entry}")
    return ok, messages


def _sync_mirror(top_packages: Dict[str, Path], nested_src: Path, force: bool) -> List[str]:
    messages: List[str] = []
    nested_src.mkdir(parents=True, exist_ok=True)

    for pkg_name, pkg_path in top_packages.items():
        mirror_link = nested_src / pkg_name
        expected = _expected_link_target(mirror_link, pkg_path)

        if mirror_link.is_symlink():
            actual = os.readlink(str(mirror_link))
            if actual == expected:
                messages.append(f"OK   {mirror_link} -> {actual}")
                continue
            mirror_link.unlink()
        elif mirror_link.exists():
            if not force:
                raise RuntimeError(
                    f"Refusing to replace non-symlink mirror entry: {mirror_link} "
                    "(re-run with --force to replace)"
                )
            if mirror_link.is_dir():
                shutil.rmtree(mirror_link)
            else:
                mirror_link.unlink()

        mirror_link.symlink_to(expected)
        messages.append(f"LINK {mirror_link} -> {expected}")

    for entry in sorted(nested_src.iterdir()):
        if entry.name.startswith(".") or entry.name == "COLCON_IGNORE":
            continue
        if entry.name in top_packages:
            continue

        if entry.is_symlink() or entry.is_file():
            entry.unlink()
            messages.append(f"DROP {entry}")
            continue
        if entry.is_dir():
            if not force:
                raise RuntimeError(
                    f"Refusing to remove unexpected mirror directory: {entry} "
                    "(re-run with --force to replace)"
                )
            shutil.rmtree(entry)
            messages.append(f"DROP {entry}")
            continue
        raise RuntimeError(f"Unsupported mirror entry type: {entry}")

    return messages


def _set_colcon_ignore(nested_src: Path, mode: str) -> str:
    marker = nested_src / "COLCON_IGNORE"
    if mode == "top-level":
        nested_src.mkdir(parents=True, exist_ok=True)
        marker.write_text(
            "# Auto-managed by scripts/sync_workspace_mirror.py\n"
            "# Prevent duplicate package discovery when building from top-level workspace.\n",
            encoding="utf-8",
        )
        return f"Set {marker} (top-level mode)"
    if mode == "nested":
        if marker.exists():
            marker.unlink()
            return f"Removed {marker} (nested mode)"
        return f"{marker} already absent (nested mode)"
    return "COLCON_IGNORE unchanged"


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync/check nested workspace mirror links.")
    parser.add_argument(
        "--ui-ws-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to ui_ws package root (default: script parent)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check mirror consistency and fail on mismatches",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Create/update mirror symlinks",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow replacing non-symlink mirror entries during --sync",
    )
    parser.add_argument(
        "--allow-missing-mirror",
        action="store_true",
        help="Do not fail --check if nested mirror directory is missing",
    )
    parser.add_argument(
        "--colcon-ignore-mode",
        choices=("unchanged", "top-level", "nested"),
        default="unchanged",
        help="Manage nested src/COLCON_IGNORE marker",
    )
    args = parser.parse_args()

    ui_ws_root = Path(args.ui_ws_root).resolve()
    top_src = ui_ws_root.parent
    nested_src = ui_ws_root / "src"

    top_packages = _discover_top_packages(top_src)
    if not top_packages:
        print(f"No top-level packages discovered under: {top_src}")
        return 1

    print(f"ui_ws_root:  {ui_ws_root}")
    print(f"top_src:     {top_src}")
    print(f"nested_src:  {nested_src}")
    print(f"packages:    {', '.join(sorted(top_packages.keys()))}")

    if args.sync:
        try:
            messages = _sync_mirror(top_packages, nested_src, force=args.force)
        except Exception as exc:
            print(f"Sync failed: {exc}")
            return 1
        for line in messages:
            print(line)
        print(_set_colcon_ignore(nested_src, args.colcon_ignore_mode))

    if args.check:
        ok, messages = _check_mirror(
            top_packages=top_packages,
            nested_src=nested_src,
            allow_missing_mirror=bool(args.allow_missing_mirror),
        )
        for line in messages:
            print(line)
        if not ok:
            return 1
        print("Mirror check OK")

    if not args.sync and not args.check:
        print("No action selected. Use --sync and/or --check.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
