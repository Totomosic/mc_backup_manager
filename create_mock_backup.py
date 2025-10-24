#!/usr/bin/env python3
"""
Utility to create a mock Minecraft backup archive using the current timestamp.

Useful for testing the backup manager without waiting for the server to
generate real backups.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Iterable, Tuple
from zipfile import ZIP_DEFLATED, ZipFile

try:
    from backup_manager import BACKUP_FORMAT
except ImportError:  # pragma: no cover - fallback when module is unavailable
    BACKUP_FORMAT = "%Y-%m-%d-%H-%M-%S"


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a mock Minecraft backup ZIP with the current timestamp."
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        required=True,
        help="Directory where the backup ZIP should be placed.",
    )
    parser.add_argument(
        "--world-name",
        default="world",
        help="Name of the world folder to mimic inside the archive.",
    )
    parser.add_argument(
        "--extra-file",
        action="append",
        default=[],
        metavar="PATH=CONTENT",
        help=(
            "Additional file entries to include inside the archive, "
            "formatted as relative_path=content. Can be passed multiple times."
        ),
    )
    return parser.parse_args(argv)


def parse_extra_files(entries: Iterable[str]) -> Iterable[Tuple[str, bytes]]:
    for entry in entries:
        if "=" not in entry:
            raise ValueError(f"Invalid --extra-file entry (missing '='): {entry}")
        path, content = entry.split("=", 1)
        path = path.strip()
        if not path:
            raise ValueError(f"Invalid --extra-file entry (empty path): {entry}")
        yield path, content.encode("utf-8")


def make_mock_backup(
    backup_dir: Path, world_name: str, extra_files: Iterable[Tuple[str, bytes]]
) -> Path:
    timestamp = datetime.now().strftime(BACKUP_FORMAT)
    backup_dir.mkdir(parents=True, exist_ok=True)

    zip_path = backup_dir / f"{timestamp}.zip"

    default_files = [
        (f"{world_name}/level.dat", b"mock level data"),
        (
            f"{world_name}/region/r.0.0.mca",
            b"region header mock",
        ),
        (
            f"{world_name}/playerdata/00000000-0000-0000-0000-000000000000.dat",
            b"player data",
        ),
    ]

    with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED) as zip_file:
        for relative_path, content in default_files:
            zip_file.writestr(relative_path, content)
        for relative_path, content in extra_files:
            zip_file.writestr(relative_path, content)

    return zip_path


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        extra_files = list(parse_extra_files(args.extra_file))
    except ValueError as error:
        print(f"Error: {error}")
        return 2

    zip_path = make_mock_backup(
        backup_dir=args.backup_dir.resolve(),
        world_name=args.world_name,
        extra_files=extra_files,
    )
    print(f"Created mock backup: {zip_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
