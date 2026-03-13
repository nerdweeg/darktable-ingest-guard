#!/usr/bin/env python3
"""
darktable-ingest-guard: Sync guard for the darktable ingest workflow.

Verifies every file in a source folder (e.g. Image Capture temp folder)
has been properly handled:
  - If the file's hash is found in the destination (darktable archive):
    the source copy is deleted.
  - If the file's hash is NOT found in the destination:
    the file is copied to <destination>/<yyyy>/<mm>/ using the date from
    the file's embedded metadata (EXIF for photos, container metadata for
    videos; falls back to file modification time).

Note: darktable only imports photos, never videos.  Videos are therefore
always copied to the destination archive by this tool.

After a successful run the source folder should be empty.

Usage:
    python darktable_ingest_guard.py --source /tmp/import --dest ~/Pictures/darktable
    python darktable_ingest_guard.py --source /tmp/import --dest ~/Pictures/darktable --dry-run
"""

import argparse
import hashlib
import logging
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Optional third-party dependencies
# ---------------------------------------------------------------------------
try:
    from PIL import Image as _PilImage  # type: ignore
    from PIL.ExifTags import TAGS as _EXIF_TAGS  # type: ignore
    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False

try:
    import hachoir.parser as _hachoir_parser  # type: ignore
    import hachoir.metadata as _hachoir_meta  # type: ignore
    HAS_HACHOIR = True
except ImportError:
    HAS_HACHOIR = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PHOTO_EXTENSIONS = {
    ".jpg", ".jpeg", ".heic", ".heif", ".png", ".tif", ".tiff",
    ".dng", ".cr2", ".cr3", ".nef", ".arw", ".orf", ".rw2",
    ".pef", ".srw", ".raf", ".raw",
}
VIDEO_EXTENSIONS = {
    ".mov", ".mp4", ".m4v", ".avi", ".mkv", ".mts", ".m2ts",
    ".3gp", ".3g2",
}
HASH_BLOCK_SIZE = 65536  # 64 KiB

# Module-level logger used by utility functions (before the run-time logger
# is configured).  The run-time logger created in setup_logging() shares the
# same name so any handlers added later are automatically picked up.
_log = logging.getLogger("ingest_guard")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Path) -> logging.Logger:
    """Configure a logger that writes to both stderr and a dated log file."""
    log_dir.mkdir(parents=True, exist_ok=True)

    # ISO 8601 timestamp with ms precision, filesystem-safe (: → -)
    ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S.%f")[:-3]  # trim to ms
    log_file = log_dir / f"{ts}_ingest_guard.log"

    logger = logging.getLogger("ingest_guard")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # File handler — DEBUG and above
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler — INFO and above
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logger.info("Log file: %s", log_file)
    return logger


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(HASH_BLOCK_SIZE):
            h.update(chunk)
    return h.hexdigest()


def get_photo_date(path: Path) -> Optional[datetime]:
    """Extract DateTimeOriginal (or DateTime) from EXIF data using Pillow."""
    if not HAS_PILLOW:
        return None
    try:
        with _PilImage.open(path) as img:
            exif_data = img._getexif()  # type: ignore[attr-defined]
        if not exif_data:
            return None
        for tag_id, value in exif_data.items():
            tag = _EXIF_TAGS.get(tag_id, tag_id)
            if tag in ("DateTimeOriginal", "DateTimeDigitized", "DateTime"):
                # EXIF date format: "YYYY:MM:DD HH:MM:SS"
                dt = datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
                return dt
    except Exception as exc:  # noqa: BLE001
        _log.debug("Failed to extract EXIF from %s: %s", path, exc)
    return None


def get_video_date(path: Path) -> Optional[datetime]:
    """Extract creation date from video container metadata using hachoir."""
    if not HAS_HACHOIR:
        return None
    try:
        parser = _hachoir_parser.createParser(str(path))
        if parser is None:
            return None
        with parser:
            metadata = _hachoir_meta.extractMetadata(parser)
        if metadata is None:
            return None
        # hachoir exposes 'creation_date' as a datetime object
        creation_date = metadata.get("creation_date")
        if creation_date:
            if isinstance(creation_date, datetime):
                return creation_date
            # Some versions return a date object
            return datetime(creation_date.year, creation_date.month, creation_date.day)
    except Exception as exc:  # noqa: BLE001
        _log.debug("Failed to extract video metadata from %s: %s", path, exc)
    return None


def get_file_date(path: Path) -> datetime:
    """
    Return the best available date for *path*.

    Priority:
      1. EXIF DateTimeOriginal (photos)
      2. Video container creation_date (videos via hachoir)
      3. File modification time (universal fallback)
    """
    ext = path.suffix.lower()
    dt: Optional[datetime] = None

    if ext in PHOTO_EXTENSIONS:
        dt = get_photo_date(path)
    elif ext in VIDEO_EXTENSIONS:
        dt = get_video_date(path)

    if dt is None:
        # Universal fallback: file modification time
        dt = datetime.fromtimestamp(path.stat().st_mtime)

    return dt


# ---------------------------------------------------------------------------
# Destination hash index
# ---------------------------------------------------------------------------

def build_dest_hash_index(dest_folder: Path) -> dict[str, Path]:
    """
    Compute SHA-256 for every file inside *dest_folder* and return a
    mapping  {hex_hash: first_matching_path}.

    This is called lazily for individual yyyy/mm subfolders to avoid
    hashing the entire archive upfront.
    """
    index: dict[str, Path] = {}
    if not dest_folder.exists():
        return index
    for item in dest_folder.iterdir():
        if item.is_file():
            try:
                index[sha256_file(item)] = item
            except OSError:
                pass
    return index


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

class IngestGuard:
    """Orchestrates the source-to-destination ingest verification and copy."""

    def __init__(
        self,
        source: Path,
        dest: Path,
        *,
        dry_run: bool = False,
        log_dir: Path,
        logger: logging.Logger,
    ) -> None:
        self.source = source
        self.dest = dest
        self.dry_run = dry_run
        self.log_dir = log_dir
        self.log = logger

        # Stats: stats[ext][action] = count
        # action ∈ {"found_in_dest", "copied", "error"}
        self.stats: defaultdict[str, defaultdict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )

        # Cache: dest_folder → hash_index to avoid re-hashing
        self._dest_index_cache: dict[Path, dict[str, Path]] = {}

    def _get_dest_index(self, dest_folder: Path) -> dict[str, Path]:
        """Return (cached) hash index for a specific destination subfolder."""
        if dest_folder not in self._dest_index_cache:
            self._dest_index_cache[dest_folder] = build_dest_hash_index(dest_folder)
        return self._dest_index_cache[dest_folder]

    def _dest_folder_for(self, path: Path) -> Path:
        """Compute the destination yyyy/mm folder based on file date metadata."""
        dt = get_file_date(path)
        return self.dest / f"{dt.year:04d}" / f"{dt.month:02d}"

    def _process_file(self, source_file: Path) -> None:
        ext = source_file.suffix.lower()
        try:
            dest_folder = self._dest_folder_for(source_file)
            source_hash = sha256_file(source_file)
            dest_index = self._get_dest_index(dest_folder)

            if source_hash in dest_index:
                # File already in destination archive — remove source copy
                matched = dest_index[source_hash]
                self.log.info(
                    "FOUND     %s  →  already in %s", source_file.name, matched
                )
                self.stats[ext]["found_in_dest"] += 1
                if not self.dry_run:
                    source_file.unlink()
                    self.log.debug("Deleted source: %s", source_file)
            else:
                # File not in destination — copy it there
                dest_file = dest_folder / source_file.name
                # Handle filename collision in destination (shouldn't be common)
                if dest_file.exists():
                    stem = source_file.stem
                    suffix = source_file.suffix
                    counter = 1
                    while dest_file.exists():
                        dest_file = dest_folder / f"{stem}_{counter}{suffix}"
                        counter += 1

                self.log.info(
                    "COPY      %s  →  %s", source_file.name, dest_folder
                )
                self.stats[ext]["copied"] += 1
                if not self.dry_run:
                    dest_folder.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source_file, dest_file)
                    source_file.unlink()
                    self.log.debug(
                        "Copied %s → %s and deleted source", source_file, dest_file
                    )
                    # Invalidate dest cache for this folder
                    self._dest_index_cache.pop(dest_folder, None)

        except Exception as exc:  # noqa: BLE001
            self.log.error("ERROR     %s  —  %s", source_file, exc)
            self.stats[ext]["error"] += 1

    def _collect_source_files(self) -> list[Path]:
        """Recursively collect all files from source (sorted for determinism)."""
        files: list[Path] = []
        for root, _dirs, filenames in os.walk(self.source):
            for name in filenames:
                p = Path(root) / name
                if p.is_file():
                    files.append(p)
        return sorted(files)

    def run(self) -> None:
        self.log.info(
            "Starting ingest guard  source=%s  dest=%s  dry_run=%s",
            self.source, self.dest, self.dry_run,
        )

        if not self.source.is_dir():
            self.log.error("Source directory does not exist: %s", self.source)
            sys.exit(1)
        if not self.dest.is_dir():
            self.log.error("Destination directory does not exist: %s", self.dest)
            sys.exit(1)

        source_files = self._collect_source_files()
        total = len(source_files)
        self.log.info("Found %d file(s) in source", total)

        for i, f in enumerate(source_files, 1):
            self.log.debug("[%d/%d] Processing %s", i, total, f)
            self._process_file(f)

        # Remove empty directories from source (bottom-up)
        if not self.dry_run:
            self._remove_empty_dirs(self.source)

        self._print_summary(total)

    def _remove_empty_dirs(self, root: Path) -> None:
        """Remove all empty subdirectories under *root* (leaves root itself)."""
        for dirpath, dirnames, filenames in os.walk(root, topdown=False):
            p = Path(dirpath)
            if p == root:
                continue
            if not any(p.iterdir()):
                try:
                    p.rmdir()
                    self.log.debug("Removed empty directory: %s", p)
                except OSError:
                    pass

    def _print_summary(self, total: int) -> None:
        """Print a multiline summary table to stdout."""
        found = sum(v["found_in_dest"] for v in self.stats.values())
        copied = sum(v["copied"] for v in self.stats.values())
        errors = sum(v["error"] for v in self.stats.values())

        dry_tag = "  [DRY RUN — no changes written]" if self.dry_run else ""
        separator = "─" * 62

        lines = [
            "",
            separator,
            f"  darktable-ingest-guard  —  summary{dry_tag}",
            separator,
            f"  {'Total files processed':<30} {total:>6}",
            f"  {'Already in darktable archive':<30} {found:>6}   (source deleted)",
            f"  {'Copied to archive':<30} {copied:>6}",
            f"  {'Errors':<30} {errors:>6}",
            separator,
        ]

        if self.stats:
            lines.append("  By file type:")
            for ext in sorted(self.stats.keys()):
                counts = self.stats[ext]
                ext_total = sum(counts.values())
                kind = (
                    "photo" if ext in PHOTO_EXTENSIONS
                    else "video" if ext in VIDEO_EXTENSIONS
                    else "other"
                )
                lines.append(
                    f"    {ext or '(no ext)':<10}  ({kind:<5})  "
                    f"found: {counts['found_in_dest']:>4}  "
                    f"copied: {counts['copied']:>4}  "
                    f"errors: {counts['error']:>4}"
                )
            lines.append(separator)

        lines.append("")

        output = "\n".join(lines)
        print(output)
        self.log.info("Summary:\n%s", output)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="darktable_ingest_guard",
        description=(
            "Verify that all files in SOURCE have been ingested into DEST "
            "(the darktable archive).  Files found in DEST are deleted from "
            "SOURCE; files not found are copied to DEST/<yyyy>/<mm>/ using "
            "embedded metadata dates."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source", "-s",
        required=True,
        type=Path,
        help="Source folder (e.g. Image Capture temp folder).",
    )
    parser.add_argument(
        "--dest", "-d",
        required=True,
        type=Path,
        help="Destination folder (darktable archive root, contains yyyy/mm sub-dirs).",
    )
    parser.add_argument(
        "--log-dir", "-l",
        type=Path,
        default=Path(__file__).parent / "logs",
        help="Directory where log files are written (default: ./logs).",
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        default=False,
        help="Simulate actions without modifying any files.",
    )
    parser.add_argument(
        "--version", "-V",
        action="version",
        version="darktable-ingest-guard 1.0.0",
    )

    ns = parser.parse_args(argv)
    ns.source = ns.source.expanduser().resolve()
    ns.dest = ns.dest.expanduser().resolve()
    ns.log_dir = ns.log_dir.expanduser().resolve()
    return ns


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)

    if not HAS_PILLOW:
        print(
            "WARNING: Pillow is not installed.  EXIF date extraction for photos "
            "will be unavailable; file modification time will be used instead.\n"
            "  Install with:  pip install Pillow",
            file=sys.stderr,
        )
    if not HAS_HACHOIR:
        print(
            "WARNING: hachoir is not installed.  Video creation-date extraction "
            "will be unavailable; file modification time will be used instead.\n"
            "  Install with:  pip install hachoir",
            file=sys.stderr,
        )

    logger = setup_logging(args.log_dir)
    guard = IngestGuard(
        source=args.source,
        dest=args.dest,
        dry_run=args.dry_run,
        log_dir=args.log_dir,
        logger=logger,
    )
    guard.run()


if __name__ == "__main__":
    main()
