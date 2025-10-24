# MCBackupManager

MCBackupManager is a lightweight utility for managing Minecraft world backup archives. It can copy or upload the most recent backup to a storage target (local filesystem or Amazon S3) and automatically prune older archives according to a configurable retention policy.

## Features

- Detects the latest ZIP archive in a backup directory and uploads or copies it to the configured destination.
- Supports both local filesystem targets and S3 buckets (including AWS profile and region selection).
- Retains only the newest backup locally while keeping a sequence of storage “checkpoints” (e.g. hourly, daily, weekly, monthly).
- Optional loop mode for continuous monitoring with a configurable poll interval.
- Dry-run support to preview actions without copying or deleting files.

## Requirements

- Python 3.8+
- `boto3` (only when using S3 targets) – install via `pip install -r requirements.txt`

## Installation & Setup

1. (Optional) Create and activate a virtual environment.
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Copy `config.example.ini` and adjust it to match your environment:

   ```bash
   cp config.example.ini config.ini
   ```

4. Edit the configuration to point at your Minecraft backup directory and desired storage target. When using S3, ensure that the configured AWS credentials have permission to upload and delete objects.

## Configuration Highlights

Key settings inside the `[backup]` section:

- `backup_dir`: Path containing ZIP archives produced by the Minecraft server.
- `storage_uri`: Either a filesystem path (`/mnt/backups/world`) or an S3 URI (`s3://bucket/prefix`).
- `aws_profile` / `aws_region`: Optional AWS overrides when targeting S3.
- `loop` and `poll_interval`: Enable continuous monitoring of the backup directory.
- `retention_checkpoints`: Comma-separated durations (e.g. `24h,7d,30d`) that define how the pruning logic collapses older backups into broader time buckets. Durations accept `s`, `m`, `h`, `d`, or `w` suffixes.

All settings can also be passed via CLI flags (run `./backup_manager.py --help` for the full list) to override values loaded from the config file.

### Understanding Retention Checkpoints

Each checkpoint acts like a “bucket size” for progressively older backups. The table below shows how many backups are kept in each age band for a policy such as `3h,24h,7d,30d`:

| Backup Age Range          | Keeper Rule                               |
|---------------------------|-------------------------------------------|
| 0 → 3 hours               | Keep every backup.                        |
| 3 hours → 24 hours        | Keep at most 1 backup per 3-hour bucket.  |
| 24 hours → 7 days         | Keep at most 1 backup per 24-hour bucket. |
| 7 days → 30 days          | Keep at most 1 backup per 7-day bucket.   |
| Beyond the last checkpoint| Keep at most 1 backup per 30-day bucket.  |

This means that even if a server produces hourly backups, the policy quickly collapses older data while preserving recent granularity. Adjust the checkpoints to reflect how much detail you need at various ages.

## Usage

Run a single backup cycle:

```bash
./backup_manager.py --config config.ini
```

Continuously monitor for new backups every 5 minutes:

```bash
./backup_manager.py --config config.ini --loop --poll-interval 300
```

Simulate actions without copying or deleting files:

```bash
./backup_manager.py --config config.ini --dry-run
```

## Testing

Execute the test suite (uses `pytest`):

```bash
pytest
```

When running inside environments with restrictive `/tmp` permissions, set `TMPDIR` to a writable location (the tests already do this in CI scripts):

```bash
TMPDIR=$(pwd)/.tmp pytest
```

## Project Structure

- `backup_manager.py` – main executable module containing CLI and backup logic.
- `config.example.ini` – sample configuration file.
- `tests/` – unit tests covering backup discovery, pruning logic, and S3 interactions.
- `create_mock_backup.py` – helper for generating sample backup archives (useful in testing or demos).
