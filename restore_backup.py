#!/usr/bin/env python3
"""
Restore a Minecraft backup archive onto an existing server directory.

For each top-level directory inside the ZIP archive this script:
1. Moves the current directory in the server tree to <name>_Old (with a suffix
   when necessary to avoid collisions).
2. Replaces it with the version from the archive so the operation can be
   reverted manually if needed.
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
from zipfile import ZipFile, is_zipfile


logger = logging.getLogger(__name__)


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore a Minecraft backup ZIP into a server directory."
    )
    parser.add_argument(
        "archive",
        type=Path,
        help="Path to the backup ZIP archive to restore.",
    )
    parser.add_argument(
        "server_dir",
        type=Path,
        help="Path to the root of the Minecraft server directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned actions without modifying any files.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity (default: INFO).",
    )
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))


def _next_old_path(destination: Path) -> Path:
    base = destination.with_name(f"{destination.name}_Old")
    if not base.exists():
        return base

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    candidate = destination.with_name(f"{destination.name}_Old_{timestamp}")
    counter = 1
    while candidate.exists():
        candidate = destination.with_name(
            f"{destination.name}_Old_{timestamp}_{counter}"
        )
        counter += 1
    return candidate


def restore_backup_archive(
    archive_path: Path, server_dir: Path, *, dry_run: bool = False
) -> None:
    archive_path = archive_path.expanduser().resolve()
    server_dir = server_dir.expanduser().resolve()

    if not archive_path.exists():
        raise FileNotFoundError(f"Archive {archive_path} does not exist.")
    if not archive_path.is_file():
        raise ValueError(f"Archive {archive_path} is not a file.")
    if not is_zipfile(archive_path):
        raise ValueError(f"Archive {archive_path} is not a valid ZIP file.")

    if not server_dir.exists():
        raise FileNotFoundError(f"Server directory {server_dir} does not exist.")
    if not server_dir.is_dir():
        raise NotADirectoryError(f"Server directory {server_dir} is not a directory.")

    logger.info("Restoring %s into %s", archive_path, server_dir)

    with tempfile.TemporaryDirectory() as temp_dir:
        extraction_root = Path(temp_dir)
        with ZipFile(archive_path) as zip_file:
            zip_file.extractall(extraction_root)

        top_level_dirs = sorted(
            path for path in extraction_root.iterdir() if path.is_dir()
        )

        if not top_level_dirs:
            logger.warning("No directories found in archive %s", archive_path)

        for extracted_dir in top_level_dirs:
            relative_name = extracted_dir.name
            destination_dir = server_dir / relative_name

            if destination_dir.exists():
                old_path = _next_old_path(destination_dir)
                logger.info("Moving existing %s to %s", destination_dir, old_path)
                if not dry_run:
                    destination_dir.rename(old_path)
            else:
                logger.debug("Directory %s does not exist in server, nothing to move.", destination_dir)

            logger.info("Restoring %s into %s", extracted_dir, destination_dir)
            if not dry_run:
                if destination_dir.exists():
                    shutil.rmtree(destination_dir)
                shutil.move(str(extracted_dir), str(destination_dir))


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    try:
        restore_backup_archive(
            archive_path=args.archive,
            server_dir=args.server_dir,
            dry_run=args.dry_run,
        )
    except Exception as error:  # pragma: no cover - top-level error handler
        logger.error("Restore failed: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
