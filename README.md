# darktable-ingest-guard

A robust command-line tool that guards your **iPhone → darktable** ingest
workflow.

## Workflow

```
iPhone
  │
  │  Image Capture
  ▼
~/Pictures/ImageCapture_import/   ← SOURCE (temp folder)
  │
  │  darktable imports photos → ~/Pictures/darktable/yyyy/mm/
  │
  └─ darktable-ingest-guard ──────────────────────────────────
        For every file in SOURCE:
          • Hash found in DEST?  → delete source copy  (already imported)
          • Hash NOT found?      → copy to DEST/yyyy/mm/ (video or missed photo)
        Result: SOURCE folder is empty
```

> **Note:** darktable only imports photos.  Videos are always copied to the
> destination archive by this tool.

---

## Installation

```bash
pip install -r requirements.txt
```

Both dependencies are **optional**.  Without them the tool still works but
uses file modification time instead of embedded metadata for date-based
folder placement:

| Package  | Purpose |
|----------|---------|
| `Pillow` | EXIF date extraction from photos (JPEG, HEIC, TIFF, RAW …) |
| `hachoir`| Creation-date extraction from video containers (MOV, MP4 …) |

---

## Usage

```
python darktable_ingest_guard.py --source <SOURCE_DIR> --dest <DEST_DIR> [options]
```

### Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--source` | `-s` | *(required)* | Source folder (Image Capture temp folder) |
| `--dest` | `-d` | *(required)* | Destination folder (darktable archive root) |
| `--log-dir` | `-l` | `./logs` | Directory for log files |
| `--dry-run` | `-n` | off | Simulate — print what would happen without changing any files |
| `--version` | `-V` | | Print version and exit |

### Examples

```bash
# Normal run
python darktable_ingest_guard.py \
    --source ~/Pictures/ImageCapture_import \
    --dest   ~/Pictures/darktable

# Dry run (safe preview)
python darktable_ingest_guard.py \
    --source ~/Pictures/ImageCapture_import \
    --dest   ~/Pictures/darktable \
    --dry-run

# Custom log directory
python darktable_ingest_guard.py \
    --source ~/Pictures/ImageCapture_import \
    --dest   ~/Pictures/darktable \
    --log-dir ~/Library/Logs/ingest-guard
```

---

## Personal wrapper script

Copy the included template and fill in your paths:

```bash
cp run_defaults.sh my_run.sh   # or just edit run_defaults.sh locally
chmod +x my_run.sh
```

`run_defaults.sh` is **git-ignored** — your local paths are never committed.

```bash
# After editing SOURCE_DIR and DEST_DIR in run_defaults.sh:
bash run_defaults.sh           # normal run
bash run_defaults.sh --dry-run # dry-run preview
```

---

## Terminal output

After every run a summary table is printed to stdout:

```
──────────────────────────────────────────────────────────────
  darktable-ingest-guard  —  summary
──────────────────────────────────────────────────────────────
  Total files processed              42
  Already in darktable archive       38   (source deleted)
  Copied to archive                   4
  Errors                              0
──────────────────────────────────────────────────────────────
  By file type:
    .heic      (photo)  found:   30  copied:    2  errors:    0
    .jpeg      (photo)  found:    5  copied:    1  errors:    0
    .mov       (video)  found:    0  copied:    1  errors:    0
    .mp4       (video)  found:    3  copied:    0  errors:    0
──────────────────────────────────────────────────────────────
```

---

## Logs

Every run writes a timestamped log file to the `logs/` directory
(ISO 8601 with millisecond precision to prevent filename collisions):

```
logs/
  2024-03-13T20-33-29.497_ingest_guard.log
  2024-03-14T08-15-00.123_ingest_guard.log
```

The `logs/` directory is **git-ignored**.

---

## Destination folder structure

Files are placed in:

```
<DEST>/<yyyy>/<mm>/<filename>
```

The year and month are derived from:
1. **EXIF `DateTimeOriginal`** (photos — most reliable)
2. **Video container creation date** (via `hachoir`)
3. **File modification time** (universal fallback)