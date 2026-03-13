#!/usr/bin/env python3
"""
darktable-ingest-guard: Ingest guard and importer for the darktable workflow.

Two operating modes
-------------------
Guard mode (default)
    Verifies every file in a source folder (e.g. Image Capture temp folder)
    has been properly handled by darktable:
      - If the file's hash is found in the destination (darktable archive):
        the source copy is deleted.
      - If the file's hash is NOT found in the destination:
        the file is copied to <destination>/<yyyy>/<mm>/ using the date from
        the file's embedded metadata (EXIF for photos, container metadata for
        videos; falls back to file modification time).

CLI-import mode  (enabled with --darktable-cli)
    Instead of relying on the darktable GUI, the tool calls darktable-cli to
    import each photo directly.  Videos (which darktable-cli cannot process)
    are always copied verbatim.  After import the result is verified and the
    source file is deleted.

In both modes the source folder should be empty after a successful run.

Usage:
    # Guard mode — verify darktable GUI already imported everything
    python darktable_ingest_guard.py --source /tmp/import --dest ~/Pictures/darktable

    # CLI-import mode — let the tool run darktable-cli itself
    python darktable_ingest_guard.py \\
        --source /tmp/import --dest ~/Pictures/darktable \\
        --darktable-cli /usr/bin/darktable-cli

    # Dry-run preview of CLI-import mode
    python darktable_ingest_guard.py \\
        --source /tmp/import --dest ~/Pictures/darktable \\
        --darktable-cli /usr/bin/darktable-cli --dry-run
"""

import argparse
import hashlib
import logging
import os
import shutil
import subprocess
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
    ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S.%f")[:-3]  # trim microseconds to milliseconds
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
# darktable-cli helpers
# ---------------------------------------------------------------------------

class DarktableCLIError(Exception):
    """Raised when a darktable-cli invocation fails or produces no output."""


def find_output_file(dest_folder: Path, source_stem: str) -> Optional[Path]:
    """
    Search *dest_folder* for a file whose stem matches *source_stem*
    (case-insensitive).

    darktable-cli keeps the original filename stem but may change the
    extension (e.g. ``IMG_1234.CR2`` → ``IMG_1234.jpg``).  This helper
    locates that output file so the caller can verify it was created.

    Returns the first matching :class:`~pathlib.Path`, or ``None`` if no
    match is found.
    """
    if not dest_folder.exists():
        return None
    needle = source_stem.lower()
    for candidate in dest_folder.iterdir():
        if candidate.is_file() and candidate.stem.lower() == needle:
            return candidate
    return None


def run_darktable_cli(
    darktable_cli: Path,
    source_file: Path,
    output_dir: Path,
    *,
    extra_args: Optional[list[str]] = None,
) -> "subprocess.CompletedProcess[str]":
    """
    Invoke ``darktable-cli`` to process *source_file* into *output_dir*.

    The command executed is::

        darktable-cli <source_file> <output_dir> [extra_args…]

    Returns the completed-process object so the caller can inspect
    ``returncode``, ``stdout``, and ``stderr``.

    Raises :class:`OSError` if the executable cannot be launched (e.g. not
    found on PATH), so callers should catch that separately.
    """
    cmd: list[str] = [str(darktable_cli), str(source_file), str(output_dir)]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd, capture_output=True, text=True)  # noqa: S603


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

class IngestGuard:
    """Orchestrates the source-to-destination ingest verification and copy.

    **Guard mode** (``darktable_cli=None``)
        Assumes darktable GUI has already imported photos.  For every file in
        the source folder the SHA-256 hash is compared against files already
        present in the corresponding ``<dest>/<yyyy>/<mm>/`` directory.
        Matches are deleted from source; missing files are copied.

    **CLI-import mode** (``darktable_cli`` set to a valid path)
        Calls ``darktable-cli`` to import each photo in the source folder.
        Videos (which darktable-cli cannot handle) are copied verbatim via
        the same hash-based logic used in guard mode.  After a successful
        import the source file is deleted.
    """

    def __init__(
        self,
        source: Path,
        dest: Path,
        *,
        dry_run: bool = False,
        log_dir: Path,
        logger: logging.Logger,
        darktable_cli: Optional[Path] = None,
        darktable_cli_args: Optional[list[str]] = None,
    ) -> None:
        self.source = source
        self.dest = dest
        self.dry_run = dry_run
        self.log_dir = log_dir
        self.log = logger
        self.darktable_cli = darktable_cli
        self.darktable_cli_args: list[str] = darktable_cli_args or []

        # Stats: stats[ext][action] = count
        # action ∈ {"found_in_dest", "copied", "imported", "error"}
        self.stats: defaultdict[str, defaultdict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )

        # Cache: dest_folder → hash_index to avoid re-hashing
        self._dest_index_cache: dict[Path, dict[str, Path]] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_dest_index(self, dest_folder: Path) -> dict[str, Path]:
        """Return (cached) hash index for a specific destination subfolder."""
        if dest_folder not in self._dest_index_cache:
            self._dest_index_cache[dest_folder] = build_dest_hash_index(dest_folder)
        return self._dest_index_cache[dest_folder]

    def _dest_folder_for(self, path: Path) -> Path:
        """Compute the destination yyyy/mm folder based on file date metadata."""
        dt = get_file_date(path)
        return self.dest / f"{dt.year:04d}" / f"{dt.month:02d}"

    # ------------------------------------------------------------------
    # Guard-mode file processing (hash comparison)
    # ------------------------------------------------------------------

    def _process_file_guard_mode(self, source_file: Path) -> None:
        """Hash-based guard: verify or copy a single file."""
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

    # ------------------------------------------------------------------
    # CLI-import mode file processing
    # ------------------------------------------------------------------

    def _import_photo_via_darktable_cli(self, source_file: Path) -> None:
        """Import a single photo using darktable-cli.

        1. Determine destination folder from file metadata.
        2. If a file with the same stem already exists in the destination,
           treat it as already imported and delete the source.
        3. Otherwise call ``darktable-cli`` to process and export the photo.
        4. Verify the output file was created and is non-empty.
        5. Delete the source file on success.
        """
        ext = source_file.suffix.lower()
        dest_folder = self._dest_folder_for(source_file)

        # Check if this photo was already imported (stem match in destination)
        existing = find_output_file(dest_folder, source_file.stem)
        if existing is not None:
            self.log.info(
                "SKIP      %s  →  already imported as %s",
                source_file.name, existing.name,
            )
            self.stats[ext]["found_in_dest"] += 1
            if not self.dry_run:
                source_file.unlink()
                self.log.debug("Deleted source: %s", source_file)
            return

        self.log.info("IMPORT    %s  →  %s", source_file.name, dest_folder)
        self.stats[ext]["imported"] += 1

        if self.dry_run:
            return

        dest_folder.mkdir(parents=True, exist_ok=True)
        try:
            result = run_darktable_cli(
                self.darktable_cli,  # type: ignore[arg-type]
                source_file,
                dest_folder,
                extra_args=self.darktable_cli_args or None,
            )
        except OSError as exc:
            self.log.error(
                "ERROR     %s  —  could not run darktable-cli: %s",
                source_file, exc,
            )
            self.stats[ext]["imported"] -= 1
            self.stats[ext]["error"] += 1
            return

        if result.returncode != 0:
            self.log.error(
                "ERROR     %s  —  darktable-cli exited %d: %s",
                source_file, result.returncode, result.stderr.strip(),
            )
            self.stats[ext]["imported"] -= 1
            self.stats[ext]["error"] += 1
            return

        # Verify output file exists and is non-empty
        output_file = find_output_file(dest_folder, source_file.stem)
        if output_file is None or output_file.stat().st_size == 0:
            self.log.error(
                "ERROR     %s  —  darktable-cli produced no output in %s",
                source_file, dest_folder,
            )
            self.stats[ext]["imported"] -= 1
            self.stats[ext]["error"] += 1
            return

        self.log.debug("Imported %s → %s", source_file, output_file)
        source_file.unlink()

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _process_file(self, source_file: Path) -> None:
        """Route a single source file to the appropriate processing method."""
        ext = source_file.suffix.lower()
        if self.darktable_cli is not None and ext in PHOTO_EXTENSIONS:
            # CLI-import mode: use darktable-cli for photos
            self._import_photo_via_darktable_cli(source_file)
        else:
            # Guard mode, or videos in CLI-import mode
            self._process_file_guard_mode(source_file)

    def _collect_source_files(self) -> list[Path]:
        """Recursively collect all files from source (sorted for determinism)."""
        files: list[Path] = []
        for root, _dirs, filenames in os.walk(self.source):
            for name in filenames:
                p = Path(root) / name
                if p.is_file():
                    files.append(p)
        return sorted(files)

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        mode_label = (
            f"CLI-import  darktable-cli={self.darktable_cli}"
            if self.darktable_cli
            else "guard"
        )
        self.log.info(
            "Starting ingest guard  source=%s  dest=%s  mode=%s  dry_run=%s",
            self.source, self.dest, mode_label, self.dry_run,
        )

        if not self.source.is_dir():
            self.log.error("Source directory does not exist: %s", self.source)
            sys.exit(1)
        if not self.dest.is_dir():
            self.log.error("Destination directory does not exist: %s", self.dest)
            sys.exit(1)

        if self.darktable_cli is not None and not self.darktable_cli.is_file():
            self.log.error(
                "darktable-cli executable not found: %s", self.darktable_cli
            )
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
        imported = sum(v["imported"] for v in self.stats.values())
        copied = sum(v["copied"] for v in self.stats.values())
        errors = sum(v["error"] for v in self.stats.values())

        dry_tag = "  [DRY RUN — no changes written]" if self.dry_run else ""
        mode_tag = "  [CLI-import mode]" if self.darktable_cli else ""
        separator = "─" * 62

        lines = [
            "",
            separator,
            f"  darktable-ingest-guard  —  summary{mode_tag}{dry_tag}",
            separator,
            f"  {'Total files processed':<30} {total:>6}",
            f"  {'Already in archive (skipped)':<30} {found:>6}   (source deleted)",
        ]

        if self.darktable_cli:
            lines.append(
                f"  {'Imported via darktable-cli':<30} {imported:>6}"
            )
        else:
            lines.append(
                f"  {'Copied to archive':<30} {copied:>6}"
            )

        lines += [
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
                if self.darktable_cli:
                    lines.append(
                        f"    {ext or '(no ext)':<10}  ({kind:<5})  "
                        f"skip: {counts['found_in_dest']:>4}  "
                        f"imported: {counts['imported']:>4}  "
                        f"copied: {counts['copied']:>4}  "
                        f"errors: {counts['error']:>4}"
                    )
                else:
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
            "Guard or perform the darktable ingest workflow.\n\n"
            "Guard mode (default): verify that all files in SOURCE have been\n"
            "ingested into DEST by the darktable GUI.  Files found in DEST are\n"
            "deleted from SOURCE; files not found are copied to\n"
            "DEST/<yyyy>/<mm>/ using embedded metadata dates.\n\n"
            "CLI-import mode (--darktable-cli): call darktable-cli to import\n"
            "each photo in SOURCE into DEST directly.  Videos (which\n"
            "darktable-cli cannot process) are copied verbatim."
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
        "--darktable-cli",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Path to the darktable-cli executable.  When provided the tool "
            "switches to CLI-import mode: photos are processed by darktable-cli "
            "and videos are copied directly.  When omitted the tool runs in "
            "guard mode (hash-based verification of a darktable GUI import)."
        ),
    )
    parser.add_argument(
        "--darktable-cli-args",
        nargs=argparse.REMAINDER,
        default=[],
        metavar="ARGS",
        help=(
            "Extra arguments forwarded verbatim to darktable-cli "
            "(e.g. --style my_style --out-ext tif).  "
            "Place these after all other options."
        ),
    )
    parser.add_argument(
        "--version", "-V",
        action="version",
        version="darktable-ingest-guard 1.1.0",
    )

    ns = parser.parse_args(argv)
    ns.source = ns.source.expanduser().resolve()
    ns.dest = ns.dest.expanduser().resolve()
    ns.log_dir = ns.log_dir.expanduser().resolve()
    if ns.darktable_cli is not None:
        ns.darktable_cli = ns.darktable_cli.expanduser().resolve()
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
        darktable_cli=args.darktable_cli,
        darktable_cli_args=args.darktable_cli_args,
    )
    guard.run()


if __name__ == "__main__":
    main()
