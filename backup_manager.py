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
    drive_folder_id: Optional[str] = None


@dataclass
class StoragePolicy:
    target: StorageTarget
    retention_checkpoints: List[int] = field(default_factory=list)


@dataclass(frozen=True)
class DriveFileRef:
    file_id: str
    name: str


@dataclass
class BackupConfig:
    backup_dir: Path
    storages: List[StoragePolicy]
    aws_profile: Optional[str] = None
    aws_region: Optional[str] = None
    gdrive_credentials: Optional[Path] = None
    loop: bool = False
    poll_interval: int = 60
    dry_run: bool = False


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
        dest="storage_uris",
        action="append",
        metavar="URI[|RETENTION]",
        help=(
            "Storage target definition. Repeat to configure multiple storages. "
            "Optionally append '|retention_checkpoints' to override the global "
            "retention policy (e.g. s3://bucket/backups|24h,7d,30d)."
        ),
    )
    parser.add_argument(
        "--storage-uri",
        dest="storage_uris",
        action="append",
        help=argparse.SUPPRESS,
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
        "--gdrive-credentials",
        type=Path,
        help="Path to a Google service account JSON credentials file for Google Drive uploads.",
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
    aws_profile = args.aws_profile if args.aws_profile is not None else file_cfg.get("aws_profile")
    aws_region = args.aws_region if args.aws_region is not None else file_cfg.get("aws_region")
    gdrive_credentials_value: Optional[Union[Path, str]]
    if args.gdrive_credentials is not None:
        gdrive_credentials_value = args.gdrive_credentials
    else:
        gdrive_credentials_value = file_cfg.get("gdrive_credentials")

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

    cli_storage_defs = args.storage_uris or []
    storage_entries: List[Tuple[str, Optional[str]]] = []
    if cli_storage_defs:
        storage_entries = [_parse_storage_cli_value(entry) for entry in cli_storage_defs]
    else:
        storage_entries = _collect_config_storage_entries(file_cfg)

        legacy_storage = file_cfg.get("storage_uri")
        if legacy_storage:
            if storage_entries:
                raise ConfigurationError(
                    "Cannot mix legacy storage_uri with storage.<name>.* entries."
                )
            storage_entries = [(legacy_storage, file_cfg.get("retention_checkpoints"))]

    if not storage_entries:
        raise ConfigurationError("At least one storage definition must be supplied.")

    backup_dir = Path(backup_dir_value).expanduser().resolve()
    default_retention = retention_value

    storages: List[StoragePolicy] = []
    for uri, retention_override in storage_entries:
        target = parse_storage(uri)
        retention_policy = _resolve_retention_policy(
            retention_override, default_retention
        )
        storages.append(
            StoragePolicy(target=target, retention_checkpoints=retention_policy)
        )

    gdrive_credentials: Optional[Path]
    if gdrive_credentials_value:
        if isinstance(gdrive_credentials_value, Path):
            gdrive_credentials = gdrive_credentials_value.expanduser().resolve()
        else:
            gdrive_credentials = Path(gdrive_credentials_value).expanduser().resolve()
    else:
        gdrive_credentials = None

    return BackupConfig(
        backup_dir=backup_dir,
        storages=storages,
        aws_profile=aws_profile,
        aws_region=aws_region,
        gdrive_credentials=gdrive_credentials,
        loop=loop,
        poll_interval=poll_value,
        dry_run=args.dry_run,
    )


def _parse_storage_cli_value(value: str) -> Tuple[str, Optional[str]]:
    raw = value.strip()
    if not raw:
        raise ConfigurationError("Storage definition cannot be empty.")

    parts = raw.split("|", 1)
    uri = parts[0].strip()
    if not uri:
        raise ConfigurationError(
            "Storage definition must include a URI before the retention separator."
        )

    if len(parts) == 1:
        return uri, None

    retention = parts[1].strip()
    return uri, retention


def _collect_config_storage_entries(
    file_cfg: Dict[str, str],
) -> List[Tuple[str, Optional[str]]]:
    grouped: Dict[str, Dict[str, str]] = {}
    order: List[str] = []
    for key, value in file_cfg.items():
        if not key.startswith("storage."):
            continue
        segments = key.split(".")
        if len(segments) < 3:
            raise ConfigurationError(
                "Config storage entries must use the storage.<name>.<field> format."
            )
        name = segments[1]
        field = ".".join(segments[2:])
        if name not in grouped:
            grouped[name] = {}
            order.append(name)
        grouped[name][field] = value

    entries: List[Tuple[str, Optional[str]]] = []
    for name in order:
        fields = grouped[name]
        uri = (
            fields.get("uri")
            or fields.get("path")
            or fields.get("location")
            or fields.get("storage_uri")
        )
        if uri is None:
            raise ConfigurationError(
                f"storage.{name}.uri (or .path/.location) must be provided."
            )
        retention = fields.get("retention_checkpoints") or fields.get("retention")
        entries.append((uri, retention))

    return entries


def _resolve_retention_policy(
    override: Optional[str], default_retention: List[int]
) -> List[int]:
    if override is None:
        return list(default_retention)

    text = override.strip()
    if not text:
        return []

    lowered = text.lower()
    if lowered in {"default", "inherit"}:
        return list(default_retention)
    if lowered in {"none", "off", "disable", "disabled"}:
        return []

    return parse_duration_list(text)


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

    if scheme == "gdrive":
        folder_id_parts = [part for part in (parsed.netloc, parsed.path.strip("/")) if part]
        if not folder_id_parts:
            raise ConfigurationError("Google Drive URI must include a folder identifier.")
        if len(folder_id_parts) > 1:
            raise ConfigurationError(
                "Google Drive URIs should use the form gdrive://<folder_id>."
            )
        return StorageTarget(kind="gdrive", drive_folder_id=folder_id_parts[0])

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
    for policy in config.storages:
        target = policy.target
        if target.kind == "s3":
            upload_to_s3(
                backup_path,
                target=target,
                aws_profile=config.aws_profile,
                aws_region=config.aws_region,
                dry_run=config.dry_run,
            )
        elif target.kind == "local":
            copy_to_local(
                backup_path, destination=target.path, dry_run=config.dry_run
            )
        elif target.kind == "gdrive":
            upload_to_gdrive(
                backup_path,
                target=target,
                credentials_path=config.gdrive_credentials,
                dry_run=config.dry_run,
            )
        else:
            raise ConfigurationError(f"Unsupported storage kind: {target.kind}")


def upload_to_s3(
    backup_path: Path,
    *,
    target: StorageTarget,
    aws_profile: Optional[str],
    aws_region: Optional[str],
    dry_run: bool,
) -> None:
    if target.kind != "s3":
        raise ConfigurationError("upload_to_s3 requires an S3 storage target.")

    bucket = target.bucket
    prefix = target.prefix

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
    target: StorageTarget,
    aws_profile: Optional[str],
    aws_region: Optional[str],
) -> List[Tuple[datetime, str]]:
    if target.kind != "s3":
        raise ConfigurationError("list_s3_backups requires an S3 storage target.")

    bucket = target.bucket
    prefix = target.prefix

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
    target: StorageTarget,
    aws_profile: Optional[str],
    aws_region: Optional[str],
    dry_run: bool,
    suppress_logging: bool = False,
) -> None:
    if target.kind != "s3":
        raise ConfigurationError("delete_s3_backups requires an S3 storage target.")

    bucket = target.bucket

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


_GDRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]


def create_gdrive_service(credentials_path: Optional[Path]):
    try:
        from googleapiclient.discovery import build  # type: ignore[import]
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "google-api-python-client is required for Google Drive operations. "
            "Install with `pip install google-api-python-client google-auth`."
        ) from exc

    try:
        from google.oauth2.service_account import Credentials  # type: ignore[import]
        import google.auth  # type: ignore[import]
        from google.auth.exceptions import DefaultCredentialsError  # type: ignore[import]
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "google-auth is required for Google Drive operations. "
            "Install with `pip install google-auth`."
        ) from exc

    if credentials_path:
        creds = Credentials.from_service_account_file(
            str(credentials_path), scopes=_GDRIVE_SCOPES
        )
    else:
        try:
            creds, _ = google.auth.default(scopes=_GDRIVE_SCOPES)
        except DefaultCredentialsError as exc:
            raise RuntimeError(
                "Google Drive operations require credentials. "
                "Provide a service account JSON file via --gdrive-credentials "
                "or set GOOGLE_APPLICATION_CREDENTIALS."
            ) from exc

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def upload_to_gdrive(
    backup_path: Path,
    *,
    target: StorageTarget,
    credentials_path: Optional[Path],
    dry_run: bool,
) -> None:
    if target.kind != "gdrive":
        raise ConfigurationError("upload_to_gdrive requires a Google Drive storage target.")
    folder_id = target.drive_folder_id
    if not folder_id:
        raise ConfigurationError("Google Drive storage requires a folder identifier.")

    logging.info(
        "Uploading %s to gdrive://%s/%s", backup_path.name, folder_id, backup_path.name
    )

    if dry_run:
        return

    service = create_gdrive_service(credentials_path)

    existing = _gdrive_find_existing(service, folder_id, backup_path.name)
    if existing is not None:
        logging.info(
            "Backup %s already present in gdrive://%s/%s, skipping upload",
            backup_path.name,
            folder_id,
            backup_path.name,
        )
        return

    try:
        from googleapiclient.http import MediaFileUpload  # type: ignore[import]
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "google-api-python-client is required for Google Drive uploads. "
            "Install with `pip install google-api-python-client`."
        ) from exc

    media = MediaFileUpload(str(backup_path), resumable=False)
    file_metadata = {"name": backup_path.name, "parents": [folder_id]}
    service.files().create(body=file_metadata, media_body=media, fields="id").execute()


def _gdrive_find_existing(service, folder_id: str, name: str):
    escaped_name = name.replace("'", "\\'")
    query = (
        f"'{folder_id}' in parents and name = '{escaped_name}' and trashed = false"
    )
    response = (
        service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id,name)",
            pageSize=1,
        )
        .execute()
    )
    files = response.get("files", [])
    if not files:
        return None
    return files[0]


def list_gdrive_backups(
    *,
    target: StorageTarget,
    credentials_path: Optional[Path],
) -> List[Tuple[datetime, DriveFileRef]]:
    if target.kind != "gdrive":
        raise ConfigurationError("list_gdrive_backups requires a Google Drive storage target.")
    folder_id = target.drive_folder_id
    if not folder_id:
        raise ConfigurationError("Google Drive storage requires a folder identifier.")

    service = create_gdrive_service(credentials_path)

    backups: List[Tuple[datetime, DriveFileRef]] = []
    page_token: Optional[str] = None
    query = f"'{folder_id}' in parents and trashed = false"

    while True:
        response = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                pageSize=1000,
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
            )
            .execute()
        )
        for file_info in response.get("files", []):
            name = file_info.get("name", "")
            if not name.endswith(".zip"):
                continue
            try:
                timestamp = datetime.strptime(Path(name).stem, BACKUP_FORMAT)
            except ValueError:
                logging.debug(
                    "Skipping non-conforming Google Drive backup %s", name
                )
                continue
            backups.append(
                (timestamp, DriveFileRef(file_id=file_info["id"], name=name))
            )
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    backups.sort(key=lambda item: item[0])
    return backups


def delete_gdrive_backups(
    files: Iterable[DriveFileRef],
    *,
    target: StorageTarget,
    credentials_path: Optional[Path],
    dry_run: bool,
) -> None:
    if target.kind != "gdrive":
        raise ConfigurationError("delete_gdrive_backups requires a Google Drive storage target.")
    folder_id = target.drive_folder_id
    if not folder_id:
        raise ConfigurationError("Google Drive storage requires a folder identifier.")

    file_list = list(files)
    if not file_list:
        return

    if dry_run:
        return

    service = create_gdrive_service(credentials_path)
    for file_ref in file_list:
        service.files().delete(fileId=file_ref.file_id).execute()


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
    config: BackupConfig, policy: StoragePolicy
) -> List[Tuple[datetime, Union[Path, str]]]:
    target = policy.target

    if target.kind == "local":
        storage_path = target.path
        if storage_path is None or not storage_path.exists():
            return []
        return find_backups(storage_path)
    if target.kind == "s3":
        backups = list_s3_backups(
            target=target,
            aws_profile=config.aws_profile,
            aws_region=config.aws_region,
        )
        return backups
    if target.kind == "gdrive":
        backups = list_gdrive_backups(
            target=target,
            credentials_path=config.gdrive_credentials,
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

    total_deletions = 0
    for policy in config.storages:
        storage_backups = find_storage_backups(config, policy)
        if not storage_backups:
            continue

        deletions = determine_backups_to_delete(
            storage_backups, policy.retention_checkpoints
        )
        total_deletions += len(deletions)

        if not deletions:
            continue

        action = "Would delete" if config.dry_run else "Deleting"
        target = policy.target

        if target.kind == "local":
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
        elif target.kind == "s3":
            bucket = target.bucket or ""
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
                target=target,
                aws_profile=config.aws_profile,
                aws_region=config.aws_region,
                dry_run=config.dry_run,
                suppress_logging=True,
            )
        elif target.kind == "gdrive":
            folder_id = target.drive_folder_id or ""
            drive_refs: List[DriveFileRef] = []
            for candidate in deletions:
                ref = cast(DriveFileRef, candidate.target)
                logging.info(
                    "%s storage backup gdrive://%s/%s: %s",
                    action,
                    folder_id,
                    ref.name,
                    candidate.reason,
                )
                drive_refs.append(ref)
            delete_gdrive_backups(
                drive_refs,
                target=target,
                credentials_path=config.gdrive_credentials,
                dry_run=config.dry_run,
            )
        else:
            logging.warning(
                "Skipping retention pruning for unsupported storage kind %s",
                target.kind,
            )

    if not config.dry_run and not config.loop:
        if total_deletions:
            logging.info(
                "Completed backup cycle: uploaded %s; pruned %d storage backups across %d target(s).",
                latest_backup.name,
                total_deletions,
                len(config.storages),
            )
        else:
            logging.info(
                "Completed backup cycle: uploaded %s; no storage pruning required.",
                latest_backup.name,
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
