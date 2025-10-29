import zipfile
from pathlib import Path
from typing import Dict

import pytest

import restore_backup


def create_zip_with_dirs(zip_path: Path, entries: Dict[str, Dict[str, bytes]]) -> None:
    with zipfile.ZipFile(zip_path, "w") as zf:
        for directory, files in entries.items():
            for filename, content in files.items():
                zf.writestr(str(Path(directory) / filename), content)


def test_restore_backup_replaces_existing_directories(tmp_path: Path) -> None:
    server_dir = tmp_path / "server"
    server_dir.mkdir()

    world_dir = server_dir / "world"
    world_dir.mkdir()
    (world_dir / "old.txt").write_text("old world")

    nether_dir = server_dir / "world_nether"
    nether_dir.mkdir()
    (nether_dir / "old.txt").write_text("old nether")

    archive = tmp_path / "backup.zip"
    create_zip_with_dirs(
        archive,
        {
            "world": {"new.txt": b"new world"},
            "world_nether": {"new.txt": b"new nether"},
        },
    )

    restore_backup.restore_backup_archive(archive, server_dir)

    old_world = server_dir / "world_Old"
    assert old_world.exists()
    assert (old_world / "old.txt").read_text() == "old world"

    restored_world = server_dir / "world"
    assert restored_world.exists()
    assert (restored_world / "new.txt").read_bytes() == b"new world"

    old_nether = server_dir / "world_nether_Old"
    assert old_nether.exists()
    assert (old_nether / "old.txt").read_text() == "old nether"

    restored_nether = server_dir / "world_nether"
    assert restored_nether.exists()
    assert (restored_nether / "new.txt").read_bytes() == b"new nether"


def test_restore_backup_dry_run(tmp_path: Path) -> None:
    server_dir = tmp_path / "server"
    server_dir.mkdir()
    existing_dir = server_dir / "world"
    existing_dir.mkdir()
    (existing_dir / "file.txt").write_text("live")

    archive = tmp_path / "backup.zip"
    create_zip_with_dirs(archive, {"world": {"file.txt": b"archive"}})

    restore_backup.restore_backup_archive(archive, server_dir, dry_run=True)

    assert not (server_dir / "world_Old").exists()
    assert (existing_dir / "file.txt").read_text() == "live"
