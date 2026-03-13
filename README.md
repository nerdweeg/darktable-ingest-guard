# darktable-ingest-guard

A robust command-line tool for the **camera / phone → darktable** ingest workflow.
It operates in two modes:

* **Guard mode** — verifies that the darktable GUI has already imported
  everything and copies any stragglers (videos, missed photos).
* **CLI-import mode** — drives `darktable-cli` itself to import photos,
  copies videos directly, then verifies the result.

---

## Workflows

### Guard mode (default)

```
Camera / phone
   │
   │  Transfer tool (e.g. gphoto2, Image Capture, Windows Photo Import)
   ▼
/path/to/import-temp/          ← SOURCE (temp folder)
   │
   │  darktable GUI imports photos → /path/to/darktable-archive/yyyy/mm/
   │
   └─ darktable-ingest-guard ──────────────────────────────────────
         For every file in SOURCE:
           • Hash found in DEST?  → delete source copy  (already imported)
           • Hash NOT found?      → copy to DEST/yyyy/mm/ (video or missed photo)
         Result: SOURCE folder is empty
```

### CLI-import mode (`--darktable-cli`)

```
Camera / phone
   │
   │  Transfer tool (e.g. gphoto2, Image Capture, Windows Photo Import)
   ▼
/path/to/import-temp/          ← SOURCE (temp folder)
   │
   └─ darktable-ingest-guard --darktable-cli <path-to-darktable-cli> ─────
         For every file in SOURCE:
           • Photo (darktable supports it):
               – Already imported? (stem match in DEST/yyyy/mm/)  → skip
               – Not yet imported?  → run darktable-cli, verify output,
                                      delete source
           • Video (darktable-cli cannot process it):
               – Hash found in DEST?  → delete source copy
               – Hash NOT found?      → copy to DEST/yyyy/mm/, delete source
         Result: SOURCE folder is empty
```

> **Note:** darktable-cli does not process video files.  Videos are always
> handled by a direct copy in both modes.

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

## Finding darktable-cli

The path to `darktable-cli` differs by platform:

| Platform | Typical location |
|----------|-----------------|
| Linux    | `darktable-cli` (if on `$PATH`) or `/usr/bin/darktable-cli` |
| macOS    | `/Applications/darktable.app/Contents/MacOS/darktable-cli` or `/opt/homebrew/bin/darktable-cli` (Homebrew) |
| Windows  | `C:\Program Files\darktable\bin\darktable-cli.exe` |

You can verify the location with `which darktable-cli` (Linux/macOS) or
`where darktable-cli` (Windows).

---

## Usage

```
python darktable_ingest_guard.py --source <SOURCE_DIR> --dest <DEST_DIR> [options]
```

### Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--source` | `-s` | *(required)* | Source folder (temp folder used by your camera transfer tool) |
| `--dest` | `-d` | *(required)* | Destination folder (darktable archive root) |
| `--log-dir` | `-l` | `./logs` | Directory for log files |
| `--dry-run` | `-n` | off | Simulate — print what would happen without changing any files |
| `--darktable-cli` | | off | Path to `darktable-cli` executable — enables CLI-import mode |
| `--darktable-cli-args` | | | Extra arguments forwarded verbatim to `darktable-cli` |
| `--version` | `-V` | | Print version and exit |

### Examples

```bash
# Guard mode — verify darktable GUI already imported everything
python darktable_ingest_guard.py \
    --source ~/import-temp \
    --dest   ~/darktable-archive

# CLI-import mode — Linux / macOS (darktable-cli on PATH)
python darktable_ingest_guard.py \
    --source ~/import-temp \
    --dest   ~/darktable-archive \
    --darktable-cli darktable-cli

# CLI-import mode — macOS (app bundle)
python darktable_ingest_guard.py \
    --source ~/import-temp \
    --dest   ~/darktable-archive \
    --darktable-cli /Applications/darktable.app/Contents/MacOS/darktable-cli

# CLI-import mode — Windows (PowerShell or CMD)
python darktable_ingest_guard.py `
    --source $env:USERPROFILE\import-temp `
    --dest   $env:USERPROFILE\darktable-archive `
    --darktable-cli "C:\Program Files\darktable\bin\darktable-cli.exe"

# CLI-import mode with a specific export style and format
python darktable_ingest_guard.py \
    --source ~/import-temp \
    --dest   ~/darktable-archive \
    --darktable-cli darktable-cli \
    --darktable-cli-args --style my_style --out-ext tif

# Dry-run preview (works with both modes)
python darktable_ingest_guard.py \
    --source ~/import-temp \
    --dest   ~/darktable-archive \
    --darktable-cli darktable-cli \
    --dry-run

# Custom log directory
python darktable_ingest_guard.py \
    --source ~/import-temp \
    --dest   ~/darktable-archive \
    --log-dir ~/logs/ingest-guard
```

---

## Personal wrapper scripts

Wrapper scripts now live in `scripts/`.
They are written for `zsh` on macOS, which matches the default shell setup.

`scripts/run_defaults.sh` is the tracked template.
Create one local wrapper per mode so you can test both workflows with the same
folders:

```zsh
cp scripts/run_defaults.sh scripts/run_guard_local.sh
cp scripts/run_defaults.sh scripts/run_cli_local.sh
chmod +x scripts/run_guard_local.sh scripts/run_cli_local.sh
```

The local wrappers are **git-ignored** so you can keep your own paths there.
They can each point at multiple source folders if needed.

```zsh
./scripts/run_guard_local.sh      # guard mode
./scripts/run_guard_local.sh --dry-run

./scripts/run_cli_local.sh        # CLI-import mode
./scripts/run_cli_local.sh --dry-run
```

---

## Terminal output

After every run a summary table is printed to stdout.

**Guard mode:**
```
──────────────────────────────────────────────────────────────
  darktable-ingest-guard  —  summary
──────────────────────────────────────────────────────────────
  Total files processed              42
  Already in archive (skipped)       38   (source deleted)
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

**CLI-import mode:**
```
──────────────────────────────────────────────────────────────
  darktable-ingest-guard  —  summary  [CLI-import mode]
──────────────────────────────────────────────────────────────
  Total files processed              42
  Already in archive (skipped)        2   (source deleted)
  Imported via darktable-cli         38
  Errors                              2
──────────────────────────────────────────────────────────────
  By file type:
    .cr2       (photo)  skip:   1  imported:   20  copied:    0  errors:    1
    .heic      (photo)  skip:   1  imported:   18  copied:    0  errors:    1
    .mov       (video)  skip:   0  imported:    0  copied:    2  errors:    0
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

---

## darktable-cli reference

See the [darktable-cli documentation](https://docs.darktable.org/usermanual/development/en/special-topics/program-invocation/darktable-cli/)
for available options.  Useful flags to pass via `--darktable-cli-args`:

| Flag | Description |
|------|-------------|
| `--out-ext <ext>` | Output file extension (e.g. `jpg`, `tif`) |
| `--style <name>` | Apply a named darktable style during export |
| `--width <px>` | Limit output width |
| `--height <px>` | Limit output height |
| `--core --library <path>` | Use a specific darktable library (SQLite database) |
