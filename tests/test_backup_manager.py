import sys
import types
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

import backup_manager
from backup_manager import (
    BACKUP_FORMAT,
    BackupConfig,
    StorageTarget,
    process_backups,
    upload_to_s3,
)


def make_backup(path: Path, name: str, content: bytes) -> Path:
    file_path = path / name
    file_path.write_bytes(content)
    return file_path


def build_config(
    *,
    backup_dir: Path,
    storage: StorageTarget,
    dry_run: bool = False,
    retention_checkpoints: Optional[List[int]] = None,
) -> BackupConfig:
    return BackupConfig(
        backup_dir=backup_dir,
        storage=storage,
        aws_profile=None,
        aws_region=None,
        loop=False,
        poll_interval=60,
        dry_run=dry_run,
        retention_checkpoints=list(retention_checkpoints or []),
    )


def setup_fake_boto3(monkeypatch: pytest.MonkeyPatch, *, object_exists: bool) -> Tuple[List[Tuple[str, str, str]], Dict[str, str]]:
    uploads: List[Tuple[str, str, str]] = []
    session_kwargs: Dict[str, str] = {}

    class FakeClient:
        def upload_file(self, filename: str, bucket: str, key: str) -> None:
            uploads.append((filename, bucket, key))

        def head_object(self, *, Bucket: str, Key: str) -> None:
            if not object_exists:
                raise fake_client_error(
                    {
                        "ResponseMetadata": {"HTTPStatusCode": 404},
                        "Error": {"Code": "404"},
                    }
                )

    class FakeSession:
        def __init__(self, **kwargs) -> None:
            session_kwargs.clear()
            session_kwargs.update(kwargs)

        def client(self, service_name: str) -> FakeClient:
            assert service_name == "s3"
            return FakeClient()

    def fake_client_error(response: dict) -> Exception:
        return FakeClientError(response, "HeadObject")

    class FakeClientError(Exception):
        def __init__(self, error_response: dict, operation_name: str) -> None:
            super().__init__("fake error")
            self.response = error_response
            self.operation_name = operation_name

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.Session = FakeSession  # type: ignore[attr-defined]

    fake_botocore = types.ModuleType("botocore")
    fake_botocore_exceptions = types.ModuleType("botocore.exceptions")
    fake_botocore_exceptions.ClientError = FakeClientError  # type: ignore[attr-defined]
    fake_botocore.exceptions = fake_botocore_exceptions  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setitem(sys.modules, "botocore", fake_botocore)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", fake_botocore_exceptions)

    return uploads, session_kwargs


def test_process_backups_local_storage_copies_and_prunes(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    destination = tmp_path / "storage"

    old_name = "2024-01-01-12-00-00.zip"
    mid_name = "2024-01-01-13-00-00.zip"
    latest_name = "2024-01-01-14-00-00.zip"

    make_backup(backup_dir, old_name, b"old")
    make_backup(backup_dir, mid_name, b"mid")
    latest_file = make_backup(backup_dir, latest_name, b"latest")

    config = build_config(
        backup_dir=backup_dir,
        storage=StorageTarget(kind="local", path=destination),
    )

    exit_code, last_uploaded = process_backups(config, last_uploaded=None)

    assert exit_code == 0
    assert last_uploaded == latest_name

    assert not (backup_dir / old_name).exists()
    assert not (backup_dir / mid_name).exists()
    assert (backup_dir / latest_name).exists()

    copied_file = destination / latest_name
    assert copied_file.exists()
    assert copied_file.read_bytes() == latest_file.read_bytes()


def test_process_backups_applies_retention_checkpoints(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    destination = tmp_path / "storage"
    destination.mkdir()

    names = [
        "2024-01-01-00-00-00.zip",
        "2024-01-01-06-00-00.zip",
        "2024-01-01-12-00-00.zip",
        "2024-01-01-18-00-00.zip",
        "2024-01-02-00-00-00.zip",
        "2024-01-02-06-00-00.zip",
        "2024-01-02-12-00-00.zip",
        "2024-01-02-18-00-00.zip",
        "2024-01-03-00-00-00.zip",
    ]

    for index, name in enumerate(names):
        make_backup(backup_dir, name, f"payload-{index}".encode())
        make_backup(destination, name, f"payload-{index}".encode())

    config = build_config(
        backup_dir=backup_dir,
        storage=StorageTarget(kind="local", path=destination),
        retention_checkpoints=[24 * 60 * 60],
    )

    exit_code, last_uploaded = process_backups(config, last_uploaded=None)

    assert exit_code == 0
    assert last_uploaded == names[-1]

    backup_dir_remaining = sorted(path.name for path in backup_dir.iterdir())
    assert backup_dir_remaining == [names[-1]]

    storage_remaining = sorted(path.name for path in destination.iterdir())
    assert storage_remaining == [
        "2024-01-01-18-00-00.zip",
        "2024-01-02-00-00-00.zip",
        "2024-01-02-06-00-00.zip",
        "2024-01-02-12-00-00.zip",
        "2024-01-02-18-00-00.zip",
        "2024-01-03-00-00-00.zip",
    ]


def test_process_backups_applies_multiple_retention_tiers(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    destination = tmp_path / "storage"
    destination.mkdir()

    base_time = datetime(2024, 3, 1, 0, 0)
    offsets = [
        timedelta(days=35),
        timedelta(days=32),
        timedelta(days=28),
        timedelta(days=20),
        timedelta(days=14),
        timedelta(days=8),
        timedelta(days=6, hours=12),
        timedelta(days=6),
        timedelta(days=5),
        timedelta(days=4, hours=12),
        timedelta(days=3, hours=12),
        timedelta(days=3),
        timedelta(days=2, hours=12),
        timedelta(days=1, hours=18),
        timedelta(hours=18),
        timedelta(hours=12),
        timedelta(hours=6),
        timedelta(),
    ]

    names: List[str] = []
    for index, delta in enumerate(offsets):
        timestamp = base_time - delta
        name = timestamp.strftime(BACKUP_FORMAT) + ".zip"
        names.append(name)
        make_backup(backup_dir, name, f"local-{index}".encode())
        make_backup(destination, name, f"storage-{index}".encode())

    config = build_config(
        backup_dir=backup_dir,
        storage=StorageTarget(kind="local", path=destination),
        retention_checkpoints=[
            24 * 60 * 60,
            7 * 24 * 60 * 60,
            30 * 24 * 60 * 60,
        ],
    )

    exit_code, last_uploaded = process_backups(config, last_uploaded=None)

    assert exit_code == 0
    assert last_uploaded == names[-1]

    expected_storage = [
        "2024-01-29-00-00-00.zip",
        "2024-02-02-00-00-00.zip",
        "2024-02-10-00-00-00.zip",
        "2024-02-16-00-00-00.zip",
        "2024-02-22-00-00-00.zip",
        "2024-02-23-12-00-00.zip",
        "2024-02-24-00-00-00.zip",
        "2024-02-25-12-00-00.zip",
        "2024-02-26-12-00-00.zip",
        "2024-02-27-12-00-00.zip",
        "2024-02-28-06-00-00.zip",
        "2024-02-29-06-00-00.zip",
        "2024-02-29-12-00-00.zip",
        "2024-02-29-18-00-00.zip",
        "2024-03-01-00-00-00.zip",
    ]

    storage_remaining = sorted(path.name for path in destination.iterdir())
    assert storage_remaining == expected_storage

    backup_dir_remaining = sorted(path.name for path in backup_dir.iterdir())
    assert backup_dir_remaining == [expected_storage[-1]]


def test_process_backups_hourly_with_multi_tier_retention(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    destination = tmp_path / "storage"
    destination.mkdir()

    base_time = datetime(2024, 4, 1, 0, 0)
    total_hours = 31 * 24
    names: List[str] = []

    for index, hours in enumerate(range(total_hours - 1, -1, -1)):
        timestamp = base_time - timedelta(hours=hours)
        name = timestamp.strftime(BACKUP_FORMAT) + ".zip"
        names.append(name)
        make_backup(backup_dir, name, f"local-hour-{index}".encode())
        make_backup(destination, name, f"storage-hour-{index}".encode())

    retention = [
        24 * 60 * 60,
        7 * 24 * 60 * 60,
        30 * 24 * 60 * 60,
    ]
    expected_storage = [
        "2024-03-01-23-00-00.zip",
        "2024-03-06-23-00-00.zip",
        "2024-03-13-23-00-00.zip",
        "2024-03-20-23-00-00.zip",
        "2024-03-24-23-00-00.zip",
        "2024-03-25-23-00-00.zip",
        "2024-03-26-23-00-00.zip",
        "2024-03-27-23-00-00.zip",
        "2024-03-28-23-00-00.zip",
        "2024-03-29-23-00-00.zip",
        "2024-03-30-23-00-00.zip",
        "2024-03-31-00-00-00.zip",
        "2024-03-31-01-00-00.zip",
        "2024-03-31-02-00-00.zip",
        "2024-03-31-03-00-00.zip",
        "2024-03-31-04-00-00.zip",
        "2024-03-31-05-00-00.zip",
        "2024-03-31-06-00-00.zip",
        "2024-03-31-07-00-00.zip",
        "2024-03-31-08-00-00.zip",
        "2024-03-31-09-00-00.zip",
        "2024-03-31-10-00-00.zip",
        "2024-03-31-11-00-00.zip",
        "2024-03-31-12-00-00.zip",
        "2024-03-31-13-00-00.zip",
        "2024-03-31-14-00-00.zip",
        "2024-03-31-15-00-00.zip",
        "2024-03-31-16-00-00.zip",
        "2024-03-31-17-00-00.zip",
        "2024-03-31-18-00-00.zip",
        "2024-03-31-19-00-00.zip",
        "2024-03-31-20-00-00.zip",
        "2024-03-31-21-00-00.zip",
        "2024-03-31-22-00-00.zip",
        "2024-03-31-23-00-00.zip",
        "2024-04-01-00-00-00.zip",
    ]

    config = build_config(
        backup_dir=backup_dir,
        storage=StorageTarget(kind="local", path=destination),
        retention_checkpoints=retention,
    )

    exit_code, last_uploaded = process_backups(config, last_uploaded=None)

    assert exit_code == 0
    assert last_uploaded == names[-1]

    storage_remaining = sorted(path.name for path in destination.iterdir())
    assert storage_remaining == expected_storage

    backup_dir_remaining = sorted(path.name for path in backup_dir.iterdir())
    assert backup_dir_remaining == [expected_storage[-1]]


def test_process_backups_applies_retention_to_s3_storage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()

    names = [
        "2024-02-01-00-00-00.zip",
        "2024-02-05-00-00-00.zip",
        "2024-02-10-00-00-00.zip",
        "2024-03-01-00-00-00.zip",
        "2024-03-05-00-00-00.zip",
        "2024-03-05-12-00-00.zip",
        "2024-03-09-00-00-00.zip",
        "2024-03-09-12-00-00.zip",
        "2024-03-10-00-00-00.zip",
    ]

    for index, name in enumerate(names):
        make_backup(backup_dir, name, f"payload-{index}".encode())

    config = build_config(
        backup_dir=backup_dir,
        storage=StorageTarget(kind="s3", bucket="my-bucket", prefix="world"),
        retention_checkpoints=[24 * 60 * 60, 7 * 24 * 60 * 60, 30 * 24 * 60 * 60],
    )

    uploads: List[str] = []

    def fake_upload(cfg: BackupConfig, path: Path) -> None:
        uploads.append(path.name)

    monkeypatch.setattr(backup_manager, "upload_backup", fake_upload)

    s3_backups = [
        (
            datetime.strptime(name[:-4], BACKUP_FORMAT),
            f"world/{name}",
        )
        for name in names
    ]

    def fake_list_s3_backups(
        *,
        bucket: Optional[str],
        prefix: Optional[str],
        aws_profile: Optional[str],
        aws_region: Optional[str],
    ) -> List[Tuple[datetime, str]]:
        assert bucket == "my-bucket"
        assert prefix == "world"
        return s3_backups

    monkeypatch.setattr(backup_manager, "list_s3_backups", fake_list_s3_backups)

    deletions: List[str] = []

    def fake_delete_s3_backups(
        keys: Iterable[str],
        *,
        bucket: Optional[str],
        aws_profile: Optional[str],
        aws_region: Optional[str],
        dry_run: bool,
    ) -> None:
        assert bucket == "my-bucket"
        assert not dry_run
        deletions.extend(keys)

    monkeypatch.setattr(backup_manager, "delete_s3_backups", fake_delete_s3_backups)

    exit_code, last_uploaded = process_backups(config, last_uploaded=None)

    assert exit_code == 0
    assert last_uploaded == names[-1]
    assert uploads == [names[-1]]
    assert deletions == [
        "world/2024-03-05-00-00-00.zip",
        "world/2024-02-01-00-00-00.zip",
    ]

    remaining_local = sorted(path.name for path in backup_dir.iterdir())
    assert remaining_local == [names[-1]]

def test_process_backups_applies_retention_checkpoints_2(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    destination = tmp_path / "storage"
    destination.mkdir()

    names = [
        "2024-01-01-00-00-00.zip",
        "2024-01-01-06-00-00.zip",
        "2024-01-01-12-00-00.zip",
        "2024-01-01-18-00-00.zip",
        "2024-01-02-00-00-00.zip",
        "2024-01-02-06-00-00.zip",
        "2024-01-02-12-00-00.zip",
        "2024-01-02-18-00-00.zip",
        "2024-01-03-00-00-00.zip",
        "2024-01-03-06-00-00.zip",
        "2024-01-04-00-00-00.zip",
    ]

    for index, name in enumerate(names):
        make_backup(backup_dir, name, f"payload-{index}".encode())
        make_backup(destination, name, f"payload-{index}".encode())

    config = build_config(
        backup_dir=backup_dir,
        storage=StorageTarget(kind="local", path=destination),
        retention_checkpoints=[24 * 60 * 60],
    )

    exit_code, last_uploaded = process_backups(config, last_uploaded=None)

    assert exit_code == 0
    assert last_uploaded == names[-1]

    backup_dir_remaining = sorted(path.name for path in backup_dir.iterdir())
    assert backup_dir_remaining == [names[-1]]

    storage_remaining = sorted(path.name for path in destination.iterdir())
    assert storage_remaining == [
        "2024-01-01-18-00-00.zip",
        "2024-01-02-18-00-00.zip",
        "2024-01-03-00-00-00.zip",
        "2024-01-03-06-00-00.zip",
        "2024-01-04-00-00-00.zip",
    ]


def test_copy_to_local_skips_existing_file(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    destination = tmp_path / "storage"
    destination.mkdir()

    latest_name = "2024-01-01-14-00-00.zip"

    backup_file = make_backup(backup_dir, latest_name, b"latest")
    existing_file = destination / latest_name
    existing_file.write_bytes(b"existing")

    config = build_config(
        backup_dir=backup_dir,
        storage=StorageTarget(kind="local", path=destination),
    )

    exit_code, last_uploaded = process_backups(config, last_uploaded=None)

    assert exit_code == 0
    assert last_uploaded == latest_name
    assert existing_file.read_bytes() == b"existing"
    assert (backup_dir / latest_name).exists()


def test_process_backups_skips_without_new_backup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    destination = tmp_path / "dest"

    latest_name = "2024-02-10-10-00-00.zip"
    make_backup(backup_dir, latest_name, b"first")

    config = build_config(
        backup_dir=backup_dir,
        storage=StorageTarget(kind="local", path=destination),
    )

    calls = []

    def fake_upload(cfg: BackupConfig, path: Path) -> None:
        calls.append(path.name)
        backup_manager.copy_to_local(path, destination=cfg.storage.path, dry_run=cfg.dry_run)

    monkeypatch.setattr(backup_manager, "upload_backup", fake_upload)

    _, last_uploaded = process_backups(config, last_uploaded=None)
    assert calls == [latest_name]

    exit_code, next_last = process_backups(config, last_uploaded=last_uploaded)
    assert exit_code == 0
    assert next_last == last_uploaded
    assert calls == [latest_name]


def test_upload_to_s3_uses_boto3(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    uploads, session_kwargs = setup_fake_boto3(monkeypatch, object_exists=False)

    backup_file = tmp_path / "2024-03-01-00-00-00.zip"
    backup_file.write_bytes(b"data")

    upload_to_s3(
        backup_file,
        bucket="my-bucket",
        prefix="minecraft/world",
        aws_profile="profile",
        aws_region="us-east-1",
        dry_run=False,
    )

    assert session_kwargs == {"profile_name": "profile", "region_name": "us-east-1"}
    assert uploads == [(str(backup_file), "my-bucket", "minecraft/world/2024-03-01-00-00-00.zip")]


def test_upload_to_s3_skips_existing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    uploads, _ = setup_fake_boto3(monkeypatch, object_exists=True)

    backup_file = tmp_path / "2024-03-02-00-00-00.zip"
    backup_file.write_bytes(b"data")

    upload_to_s3(
        backup_file,
        bucket="existing-bucket",
        prefix="minecraft",
        aws_profile=None,
        aws_region=None,
        dry_run=False,
    )

    assert uploads == []
