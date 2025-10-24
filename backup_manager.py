#!/usr/bin/env python3
"""
Minecraft backup manager.

Uploads the most recent backup archive to a configured storage target
and removes older backups from the local backup directory.
"""
from __future__ import annotations

import argparse
import configparser
import logging
import time
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generic, Iterable, List, Optional, Set, Tuple, TypeVar, Union, cast
from urllib.parse import urlparse


BACKUP_FORMAT = "%Y-%m-%d-%H-%M-%S"
CONFIG_SECTION = "backup"
NOISY_LOGGERS = ("boto", "boto3", "botocore", "urllib3", "s3transfer")


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""


@dataclass
class StorageTarget:
    kind: str  # 's3' or 'local'
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    path: Optional[Path] = None


@dataclass
class BackupConfig:
    backup_dir: Path
    storage: StorageTarget
    aws_profile: Optional[str] = None
    aws_region: Optional[str] = None
    loop: bool = False
    poll_interval: int = 60
    dry_run: bool = False
    retention_checkpoints: List[int] = field(default_factory=list)  # seconds


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload the latest Minecraft backup and prune older archives."
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        help="Path to an INI config file containing backup parameters.",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        help="Directory where the Minecraft server writes backup archives.",
    )
    parser.add_argument(
        "--storage",
        dest="storage_uri",
        help="Storage target URI (e.g. s3://bucket/prefix or /path/to/storage).",
    )
    parser.add_argument(
        "--aws-profile",
        help="Named AWS shared credentials profile to use for uploads.",
    )
    parser.add_argument(
        "--aws-region",
        help="AWS region when creating the S3 client.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity (default: INFO).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned actions without uploading or deleting files.",
    )
    parser.add_argument(
        "--loop",
        dest="loop",
        action="store_true",
        help="Run continuously, checking for new backups at a set interval.",
    )
    parser.add_argument(
        "--no-loop",
        dest="loop",
        action="store_false",
        help=argparse.SUPPRESS,
    )
    parser.set_defaults(loop=None)
    parser.add_argument(
        "--poll-interval",
        type=int,
        help="Seconds between checks when running in loop mode (default: 60).",
    )
    parser.add_argument(
        "--retention-checkpoints",
        help=(
            "Comma-separated list of duration checkpoints like 24h,7d,30d "
            "to retain representative backups for older periods."
        ),
    )
    return parser.parse_args(argv)


def read_config_file(config_path: Path) -> Dict[str, str]:
    parser = configparser.ConfigParser()
    read_files = parser.read(config_path)
    if not read_files:
        raise ConfigurationError(f"Config file {config_path} could not be read.")
    if CONFIG_SECTION not in parser:
        raise ConfigurationError(
            f"Config file {config_path} is missing the [{CONFIG_SECTION}] section."
        )
    return {k: v for k, v in parser[CONFIG_SECTION].items()}


def merge_config(
    args: argparse.Namespace, file_config: Optional[Dict[str, str]]
) -> BackupConfig:
    file_cfg = file_config or {}

    backup_dir_value = args.backup_dir or file_cfg.get("backup_dir")
    storage_value = args.storage_uri or file_cfg.get("storage_uri")
    aws_profile = args.aws_profile if args.aws_profile is not None else file_cfg.get("aws_profile")
    aws_region = args.aws_region if args.aws_region is not None else file_cfg.get("aws_region")

    loop_value = file_cfg.get("loop")
    if args.loop is not None:
        loop = args.loop
    elif loop_value is not None:
        loop = parse_bool(loop_value)
    else:
        loop = False

    poll_value: Optional[int]
    if args.poll_interval is not None:
        poll_value = args.poll_interval
    elif "poll_interval" in file_cfg:
        poll_value = parse_int(file_cfg["poll_interval"], "poll_interval")
    else:
        poll_value = 60

    retention_value: List[int]
    if args.retention_checkpoints is not None:
        retention_value = parse_duration_list(args.retention_checkpoints)
    elif "retention_checkpoints" in file_cfg:
        retention_value = parse_duration_list(file_cfg["retention_checkpoints"])
    else:
        retention_value = []

    if poll_value <= 0:
        raise ConfigurationError("poll_interval must be a positive integer.")

    if not backup_dir_value:
        raise ConfigurationError("backup_dir must be supplied via CLI or config file.")
    if not storage_value:
        raise ConfigurationError("storage must be supplied via CLI or config file.")

    backup_dir = Path(backup_dir_value).expanduser().resolve()
    storage = parse_storage(storage_value)

    return BackupConfig(
        backup_dir=backup_dir,
        storage=storage,
        aws_profile=aws_profile,
        aws_region=aws_region,
        loop=loop,
        poll_interval=poll_value,
        dry_run=args.dry_run,
        retention_checkpoints=retention_value,
    )


def parse_storage(storage_uri: str) -> StorageTarget:
    parsed = urlparse(storage_uri)
    scheme = parsed.scheme.lower()

    if len(parsed.scheme) == 1 and storage_uri[1:2] == ":":
        # Handle Windows drive letter paths (e.g. C:\backups)
        scheme = ""

    if scheme == "s3":
        if not parsed.netloc:
            raise ConfigurationError("S3 URI must include a bucket name.")
        prefix = parsed.path.lstrip("/")
        return StorageTarget(kind="s3", bucket=parsed.netloc, prefix=prefix or None)

    if scheme in ("", "file"):
        path_str = parsed.path if scheme == "file" else storage_uri
        path = Path(path_str).expanduser().resolve()
        return StorageTarget(kind="local", path=path)

    raise ConfigurationError(f"Unsupported storage scheme: {scheme}")


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"Invalid boolean value: {value}")


def parse_int(value: str, name: str) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer.") from error


def _quiet_external_loggers() -> None:
    for name in NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def format_duration(seconds: int) -> str:
    units = [
        (7 * 24 * 60 * 60, "week"),
        (24 * 60 * 60, "day"),
        (60 * 60, "hour"),
        (60, "minute"),
        (1, "second"),
    ]
    for unit_seconds, label in units:
        if seconds >= unit_seconds and seconds % unit_seconds == 0:
            value = seconds // unit_seconds
            name = label if value == 1 else f"{label}s"
            return f"{value} {name}"
    return f"{seconds} seconds"


def parse_duration(value: str) -> int:
    units = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
    }

    normalized = value.strip().lower()
    if not normalized:
        raise ConfigurationError("Duration values must not be empty.")

    suffix = normalized[-1]
    if suffix.isalpha():
        if suffix not in units:
            raise ConfigurationError(
                "Unsupported duration suffix. Use one of s, m, h, d, w."
            )
        number_part = normalized[:-1]
    else:
        suffix = "s"
        number_part = normalized

    if not number_part:
        raise ConfigurationError(f"Missing numeric value for duration: {value}")

    try:
        amount = int(number_part)
    except ValueError as error:
        raise ConfigurationError(f"Invalid duration value: {value}") from error

    seconds = amount * units[suffix]
    if seconds <= 0:
        raise ConfigurationError(f"Duration must be positive: {value}")
    return seconds


def parse_duration_list(value: str) -> List[int]:
    if not value:
        return []

    parts = [part.strip() for part in value.split(",")]
    durations = [parse_duration(part) for part in parts if part]

    if not durations:
        return []

    for previous, current in zip(durations, durations[1:]):
        if current <= previous:
            raise ConfigurationError(
                "retention_checkpoints must be strictly increasing."
            )

    return durations


def find_backups(backup_dir: Path) -> List[Tuple[datetime, Path]]:
    backups: List[Tuple[datetime, Path]] = []
    for candidate in backup_dir.glob("*.zip"):
        if not candidate.is_file():
            continue
        try:
            timestamp = datetime.strptime(candidate.stem, BACKUP_FORMAT)
        except ValueError:
            logging.debug("Skipping non-conforming backup file %s", candidate.name)
            continue
        backups.append((timestamp, candidate))
    backups.sort(key=lambda item: item[0])
    return backups


def upload_backup(config: BackupConfig, backup_path: Path) -> None:
    if config.storage.kind == "s3":
        upload_to_s3(
            backup_path,
            bucket=config.storage.bucket,
            prefix=config.storage.prefix,
            aws_profile=config.aws_profile,
            aws_region=config.aws_region,
            dry_run=config.dry_run,
        )
    elif config.storage.kind == "local":
        copy_to_local(
            backup_path, destination=config.storage.path, dry_run=config.dry_run
        )
    else:
        raise ConfigurationError(f"Unsupported storage kind: {config.storage.kind}")


def upload_to_s3(
    backup_path: Path,
    *,
    bucket: Optional[str],
    prefix: Optional[str],
    aws_profile: Optional[str],
    aws_region: Optional[str],
    dry_run: bool,
) -> None:
    if bucket is None:
        raise ConfigurationError("S3 uploads require a bucket.")

    object_key = f"{prefix.rstrip('/') + '/' if prefix else ''}{backup_path.name}"
    logging.info(
        "Uploading %s to s3://%s/%s", backup_path.name, bucket, object_key
    )

    if dry_run:
        return

    s3_client = create_s3_client(aws_profile=aws_profile, aws_region=aws_region)

    if s3_object_exists(s3_client, bucket, object_key):
        logging.info("Backup %s already present in s3://%s/%s, skipping upload", backup_path.name, bucket, object_key)
        return

    s3_client.upload_file(str(backup_path), bucket, object_key)


def s3_object_exists(client, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        error_code = error.response.get("Error", {}).get("Code")
        if error_code in ("404", "NotFound", "NoSuchKey"):
            return False
        raise


def create_s3_client(
    *, aws_profile: Optional[str], aws_region: Optional[str]
):
    try:
        import boto3
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "boto3 is required for S3 operations. Install with `pip install boto3`."
        ) from exc
    _quiet_external_loggers()
    session_kwargs = {}
    if aws_profile:
        session_kwargs["profile_name"] = aws_profile
    if aws_region:
        session_kwargs["region_name"] = aws_region
    session = boto3.Session(**session_kwargs)
    return session.client("s3")


def list_s3_backups(
    *,
    bucket: Optional[str],
    prefix: Optional[str],
    aws_profile: Optional[str],
    aws_region: Optional[str],
) -> List[Tuple[datetime, str]]:
    if bucket is None:
        raise ConfigurationError("S3 storage requires a bucket.")

    client = create_s3_client(aws_profile=aws_profile, aws_region=aws_region)

    list_kwargs = {"Bucket": bucket}
    if prefix:
        list_kwargs["Prefix"] = prefix.rstrip("/") + "/"

    paginator = client.get_paginator("list_objects_v2")
    backups: List[Tuple[datetime, str]] = []

    for page in paginator.paginate(**list_kwargs):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = Path(key).name
            if not name.endswith(".zip"):
                continue
            try:
                timestamp = datetime.strptime(Path(name).stem, BACKUP_FORMAT)
            except ValueError:
                logging.debug("Skipping non-conforming backup object %s", key)
                continue
            backups.append((timestamp, key))

    backups.sort(key=lambda item: item[0])
    return backups


def delete_s3_backups(
    keys: Iterable[str],
    *,
    bucket: Optional[str],
    aws_profile: Optional[str],
    aws_region: Optional[str],
    dry_run: bool,
    suppress_logging: bool = False,
) -> None:
    if bucket is None:
        raise ConfigurationError("S3 storage requires a bucket.")

    keys_list = list(keys)
    if not keys_list:
        return

    for key in keys_list:
        if not suppress_logging:
            logging.info("Deleting old backup s3://%s/%s", bucket, key)

    if dry_run:
        return

    client = create_s3_client(aws_profile=aws_profile, aws_region=aws_region)
    for key in keys_list:
        client.delete_object(Bucket=bucket, Key=key)


def copy_to_local(backup_path: Path, *, destination: Optional[Path], dry_run: bool) -> None:
    if destination is None:
        raise ConfigurationError("Local storage requires a destination path.")

    target_file = destination / backup_path.name
    logging.info("Copying %s to %s", backup_path.name, target_file)

    if dry_run:
        return

    if target_file.exists():
        logging.info("Backup %s already present at %s, skipping copy", backup_path.name, target_file)
        return

    destination.mkdir(parents=True, exist_ok=True)
    from shutil import copy2

    copy2(backup_path, target_file)


def _timestamp_slot(timestamp: datetime, granularity_seconds: int) -> int:
    if granularity_seconds <= 0:
        raise ValueError("granularity_seconds must be positive.")
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    as_utc = timestamp.replace(tzinfo=timezone.utc)
    seconds = int((as_utc - epoch).total_seconds())
    return seconds // granularity_seconds


BackupPath = TypeVar("BackupPath")


@dataclass
class RetentionDeletion(Generic[BackupPath]):
    target: BackupPath
    reason: str


def determine_backups_to_delete(
    backups: List[Tuple[datetime, BackupPath]], retention_checkpoints: List[int]
) -> List[RetentionDeletion[BackupPath]]:
    if len(backups) <= 1:
        return []

    if not retention_checkpoints:
        return [
            RetentionDeletion(
                path,
                "Retention policy has no checkpoints configured; keeping only the most recent backup.",
            )
            for _, path in backups[:-1]
        ]

    durations = retention_checkpoints
    to_delete: List[RetentionDeletion[BackupPath]] = []
    seen_slots: Set[Tuple[int, int]] = set()
    reference_time = backups[-1][0]

    for timestamp, path in reversed(backups[:-1]):
        age_seconds = (reference_time - timestamp).total_seconds()
        if age_seconds <= durations[0]:
            continue

        bucket_index = 0
        while bucket_index < len(durations) and age_seconds > durations[bucket_index]:
            bucket_index += 1

        index_for_label = min(max(bucket_index - 1, 0), len(durations) - 1)
        granularity_seconds = durations[index_for_label]
        slot_key = (bucket_index, _timestamp_slot(timestamp, granularity_seconds))

        if slot_key in seen_slots:
            reason = (
                f"Retention checkpoint {format_duration(granularity_seconds)}: "
                "a newer backup already covers this interval."
            )
            to_delete.append(RetentionDeletion(path, reason))
        else:
            seen_slots.add(slot_key)

    return to_delete


def delete_old_backups(backups: Iterable[Path], *, dry_run: bool, suppress_logging: bool = False) -> None:
    for backup in backups:
        if not suppress_logging:
            logging.info("Deleting old backup %s", backup)
        if dry_run:
            continue
        backup.unlink(missing_ok=True)


def find_storage_backups(
    config: BackupConfig,
) -> List[Tuple[datetime, Union[Path, str]]]:
    if config.storage.kind == "local":
        storage_path = config.storage.path
        if storage_path is None or not storage_path.exists():
            return []
        return find_backups(storage_path)
    if config.storage.kind == "s3":
        backups = list_s3_backups(
            bucket=config.storage.bucket,
            prefix=config.storage.prefix,
            aws_profile=config.aws_profile,
            aws_region=config.aws_region,
        )
        return backups
    return []


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), None)
    if level is None:
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(level=level)
    _quiet_external_loggers()


def process_backups(
    config: BackupConfig, *, last_uploaded: Optional[str]
) -> Tuple[int, Optional[str]]:
    backups = find_backups(config.backup_dir)
    if not backups:
        log_func = logging.debug if config.loop else logging.info
        log_func("No backup archives found in %s.", config.backup_dir)
        return 0, last_uploaded

    latest_timestamp, latest_backup = backups[-1]
    if last_uploaded and latest_backup.name == last_uploaded:
        logging.debug("No new backup since %s.", latest_backup.name)
        return 0, last_uploaded

    logging.info(
        "Latest backup is %s from %s", latest_backup.name, latest_timestamp.isoformat()
    )

    try:
        upload_backup(config, latest_backup)
    except Exception as error:  # keep generic to avoid leaving folder empty
        logging.error("Failed to upload %s: %s", latest_backup.name, error)
        return 1, last_uploaded

    local_deletions = [path for _, path in backups[:-1]]
    delete_old_backups(local_deletions, dry_run=config.dry_run)

    storage_backups = find_storage_backups(config)
    if storage_backups:
        deletions = determine_backups_to_delete(
            storage_backups, config.retention_checkpoints
        )
        if deletions:
            action = "Would delete" if config.dry_run else "Deleting"
            if config.storage.kind == "local":
                local_paths: List[Path] = []
                for candidate in deletions:
                    path = cast(Path, candidate.target)
                    logging.info(
                        "%s storage backup %s: %s",
                        action,
                        path,
                        candidate.reason,
                    )
                    local_paths.append(path)
                delete_old_backups(
                    local_paths, dry_run=config.dry_run, suppress_logging=True
                )
            elif config.storage.kind == "s3":
                bucket = config.storage.bucket or ""
                s3_keys: List[str] = []
                for candidate in deletions:
                    key = cast(str, candidate.target)
                    logging.info(
                        "%s storage backup s3://%s/%s: %s",
                        action,
                        bucket,
                        key,
                        candidate.reason,
                    )
                    s3_keys.append(key)
                delete_s3_backups(
                    s3_keys,
                    bucket=config.storage.bucket,
                    aws_profile=config.aws_profile,
                    aws_region=config.aws_region,
                    dry_run=config.dry_run,
                    suppress_logging=True,
                )

    return 0, latest_backup.name


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    try:
        configure_logging(args.log_level)
    except ValueError as error:
        logging.error("%s", error)
        return 2

    file_config: Optional[Dict[str, str]] = None
    if args.config:
        file_config = read_config_file(args.config)

    try:
        config = merge_config(args, file_config)
    except ConfigurationError as error:
        logging.error("%s", error)
        return 2

    if not config.backup_dir.exists():
        logging.error(
            "Backup directory %s does not exist.", config.backup_dir
        )
        return 2

    last_uploaded: Optional[str] = None
    while True:
        exit_code, last_uploaded = process_backups(
            config, last_uploaded=last_uploaded
        )
        if not config.loop:
            return exit_code
        time.sleep(config.poll_interval)


if __name__ == "__main__":
    sys.exit(main())
