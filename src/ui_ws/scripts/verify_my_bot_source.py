#!/usr/bin/env python3
"""
Validate that the ugv_bringup package has a usable source tree and detect install drift.

Why this exists:
- Source must remain the single editable truth (version-controlled).
- install/<pkg>/share/<pkg> is a build artifact and should match source payload.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


PAYLOAD_SUBDIRS: Tuple[str, ...] = ("config", "launch", "maps", "scripts", "urdf", "worlds")
REQUIRED_SOURCE_PATHS: Tuple[str, ...] = ("package.xml", "CMakeLists.txt", "config", "launch", "urdf", "maps")
EXCLUDED_REL_PATHS = {
    "config/active_map_config.yaml",  # runtime state file
    "config/zones.yaml",              # runtime state file (if present)
    "zones.yaml",                     # legacy runtime state file location
}
EXCLUDED_GLOB_PATTERNS = (
    "config/path_metadata.yaml",                    # runtime state file
    "config/robot_profiles/registry.yaml",          # runtime state file
    "config/robot_profiles/bringup_reports/**",     # runtime generated reports
)
EXCLUDED_DIR_NAMES = {"__pycache__", ".pytest_cache"}
EXCLUDED_SUFFIXES = (".pyc", ".pyo", "~")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _iter_payload_files(root: Path) -> Iterable[Tuple[str, Path]]:
    for subdir in PAYLOAD_SUBDIRS:
        base = root / subdir
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            parts = set(path.relative_to(root).parts)
            if parts & EXCLUDED_DIR_NAMES:
                continue
            if _is_excluded_rel(rel):
                continue
            if rel.endswith(EXCLUDED_SUFFIXES):
                continue
            yield rel, path


def _is_excluded_rel(rel: str) -> bool:
    if rel in EXCLUDED_REL_PATHS:
        return True
    for pattern in EXCLUDED_GLOB_PATTERNS:
        if fnmatch.fnmatch(rel, pattern):
            return True
    return False


def _collect_hashes(root: Path) -> Dict[str, str]:
    output: Dict[str, str] = {}
    for rel, path in _iter_payload_files(root):
        output[rel] = _sha256(path)
    return output


def _read_package_name(package_xml: Path) -> str:
    try:
        root = ET.parse(package_xml).getroot()
    except Exception:
        return ""
    name_node = root.find("name")
    if name_node is None:
        return ""
    return str(name_node.text or "").strip()


def _infer_install_root(source_root: Path, package_name: str) -> Optional[Path]:
    candidates = [
        source_root.parent.parent / "install" / package_name / "share" / package_name,
        source_root.parent / "install" / package_name / "share" / package_name,
        source_root / "install" / package_name / "share" / package_name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return None


def _validate_source_tree(source_root: Path) -> List[str]:
    errors: List[str] = []
    if not source_root.exists():
        return [f"Source root does not exist: {source_root}"]
    if not source_root.is_dir():
        return [f"Source root is not a directory: {source_root}"]

    for rel in REQUIRED_SOURCE_PATHS:
        path = source_root / rel
        if not path.exists():
            errors.append(f"Missing required source path: {rel}")

    package_xml = source_root / "package.xml"
    if package_xml.exists():
        pkg_name = _read_package_name(package_xml)
        if not pkg_name:
            errors.append("package.xml is present but package name could not be read")
    return errors


def _format_items(header: str, items: List[str], limit: int = 40) -> str:
    if not items:
        return ""
    shown = items[:limit]
    lines = [header]
    lines.extend([f"  - {item}" for item in shown])
    if len(items) > limit:
        lines.append(f"  - ... and {len(items) - limit} more")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify ugv_bringup source-of-truth layout and install drift.")
    parser.add_argument(
        "--source-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to ugv_bringup source package root",
    )
    parser.add_argument(
        "--install-root",
        default="",
        help="Path to install/<pkg>/share/<pkg> (optional; inferred when omitted)",
    )
    parser.add_argument(
        "--allow-missing-install",
        action="store_true",
        help="Do not fail if install root is absent",
    )
    args = parser.parse_args()

    source_root = Path(args.source_root).resolve()
    errors = _validate_source_tree(source_root)
    if errors:
        print("ugv_bringup source validation failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    package_name = _read_package_name(source_root / "package.xml")
    if not package_name:
        print("ugv_bringup source validation failed: unable to resolve package name from package.xml")
        return 1

    if args.install_root:
        install_root = Path(args.install_root).resolve()
    else:
        inferred = _infer_install_root(source_root, package_name)
        install_root = inferred if inferred is not None else Path()

    if not install_root or not install_root.exists():
        if args.allow_missing_install:
            print(f"Source tree OK for package '{package_name}'. Install root not found (allowed).")
            return 0
        print(
            "Install root not found. Provide --install-root or run after build.\n"
            f"Expected package: {package_name}\n"
            f"Source root: {source_root}"
        )
        return 1

    if not install_root.is_dir():
        print(f"Install root is not a directory: {install_root}")
        return 1

    src_hashes = _collect_hashes(source_root)
    inst_hashes = _collect_hashes(install_root)

    missing_in_install = sorted(set(src_hashes) - set(inst_hashes))
    extra_in_install = sorted(set(inst_hashes) - set(src_hashes))
    changed = sorted([rel for rel in set(src_hashes) & set(inst_hashes) if src_hashes[rel] != inst_hashes[rel]])

    if missing_in_install or extra_in_install or changed:
        print("Source/install drift detected for ugv_bringup package payload.")
        chunks = [
            _format_items("Missing in install:", missing_in_install),
            _format_items("Extra in install:", extra_in_install),
            _format_items("Content mismatch:", changed),
        ]
        for chunk in chunks:
            if chunk:
                print(chunk)
        return 1

    print(
        f"ugv_bringup source integrity OK.\n"
        f"- package: {package_name}\n"
        f"- source:  {source_root}\n"
        f"- install: {install_root}\n"
        f"- checked files: {len(src_hashes)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
