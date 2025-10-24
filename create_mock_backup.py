#!/usr/bin/env python3
"""
Utility to create a mock Minecraft backup archive using the current timestamp.

Useful for testing the backup manager without waiting for the server to
generate real backups.
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta
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
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of backups to create (default: 1).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to wait between creating each backup file.",
    )
    parser.add_argument(
        "--timestamp-step",
        type=str,
        help="Increment to apply to the backup timestamp for each successive backup (e.g. 30m, 1h).",
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


def parse_duration(value: str) -> timedelta:
    units = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
    }

    normalized = value.strip().lower()
    if not normalized:
        raise ValueError("Duration value must not be empty.")

    suffix = normalized[-1]
    if suffix in units:
        number_part = normalized[:-1]
        multiplier = units[suffix]
    else:
        number_part = normalized
        multiplier = 1

    if not number_part:
        raise ValueError(f"Invalid duration value: {value}")

    try:
        number = int(number_part)
    except ValueError as error:
        raise ValueError(f"Invalid duration value: {value}") from error

    if number <= 0:
        raise ValueError("Duration value must be positive.")

    return timedelta(seconds=number * multiplier)


def make_mock_backup(
    backup_dir: Path,
    world_name: str,
    extra_files: Iterable[Tuple[str, bytes]],
    timestamp: datetime | None = None,
) -> Path:
    timestamp = (timestamp or datetime.now()).strftime(BACKUP_FORMAT)
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

    backup_dir = args.backup_dir.resolve()

    timestamp_step: timedelta | None = None
    if args.timestamp_step:
        try:
            timestamp_step = parse_duration(args.timestamp_step)
        except ValueError as error:
            print(f"Error: {error}")
            return 2

    if args.count <= 0:
        print("Error: --count must be a positive integer.")
        return 2

    current_timestamp = datetime.now()

    for index in range(args.count):
        zip_path = make_mock_backup(
            backup_dir=backup_dir,
            world_name=args.world_name,
            extra_files=extra_files,
            timestamp=current_timestamp if timestamp_step else None,
        )
        print(f"Created mock backup: {zip_path}")

        if timestamp_step:
            current_timestamp += timestamp_step
        if args.sleep and index < args.count - 1:
            time.sleep(args.sleep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
