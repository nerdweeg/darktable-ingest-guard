"""
Tests for darktable_ingest_guard — covering both guard mode and CLI-import mode.
"""

import hashlib
import logging
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import darktable_ingest_guard as dig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_source(tmp_path: Path) -> Path:
    """Return an empty source directory."""
    src = tmp_path / "source"
    src.mkdir()
    return src


@pytest.fixture()
def tmp_dest(tmp_path: Path) -> Path:
    """Return an empty destination directory."""
    dst = tmp_path / "dest"
    dst.mkdir()
    return dst


@pytest.fixture()
def logger() -> logging.Logger:
    """Return a silent logger."""
    log = logging.getLogger("test_ingest_guard")
    log.addHandler(logging.NullHandler())
    return log


def _make_guard(source, dest, logger, **kwargs):
    """Helper: construct an IngestGuard with sensible test defaults."""
    return dig.IngestGuard(
        source=source,
        dest=dest,
        dry_run=kwargs.pop("dry_run", False),
        log_dir=source.parent / "logs",
        logger=logger,
        **kwargs,
    )


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# sha256_file
# ---------------------------------------------------------------------------

class TestSha256File:
    def test_known_hash(self, tmp_path):
        data = b"hello world"
        f = tmp_path / "test.txt"
        f.write_bytes(data)
        assert dig.sha256_file(f) == _sha256(data)

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        assert dig.sha256_file(f) == _sha256(b"")


# ---------------------------------------------------------------------------
# find_output_file
# ---------------------------------------------------------------------------

class TestFindOutputFile:
    def test_finds_by_stem(self, tmp_path):
        (tmp_path / "IMG_1234.jpg").write_bytes(b"x")
        result = dig.find_output_file(tmp_path, "IMG_1234")
        assert result is not None
        assert result.name == "IMG_1234.jpg"

    def test_case_insensitive(self, tmp_path):
        (tmp_path / "img_1234.JPG").write_bytes(b"x")
        result = dig.find_output_file(tmp_path, "IMG_1234")
        assert result is not None

    def test_no_match(self, tmp_path):
        (tmp_path / "other.jpg").write_bytes(b"x")
        assert dig.find_output_file(tmp_path, "IMG_1234") is None

    def test_nonexistent_folder(self, tmp_path):
        assert dig.find_output_file(tmp_path / "nope", "stem") is None

    def test_ignores_directories(self, tmp_path):
        subdir = tmp_path / "IMG_1234"
        subdir.mkdir()
        assert dig.find_output_file(tmp_path, "IMG_1234") is None


# ---------------------------------------------------------------------------
# run_darktable_cli
# ---------------------------------------------------------------------------

class TestRunDarktableCli:
    def test_passes_correct_args(self, tmp_path):
        fake_cli = tmp_path / "darktable-cli"
        src = tmp_path / "photo.cr2"
        out = tmp_path / "out"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            dig.run_darktable_cli(fake_cli, src, out)
            mock_run.assert_called_once_with(
                [str(fake_cli), str(src), str(out)],
                capture_output=True,
                text=True,
            )

    def test_passes_extra_args(self, tmp_path):
        fake_cli = tmp_path / "darktable-cli"
        src = tmp_path / "photo.cr2"
        out = tmp_path / "out"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            dig.run_darktable_cli(fake_cli, src, out, extra_args=["--out-ext", "tif"])
            call_args = mock_run.call_args[0][0]
            assert "--out-ext" in call_args
            assert "tif" in call_args

    def test_returns_completed_process(self, tmp_path):
        fake_cli = tmp_path / "darktable-cli"
        src = tmp_path / "photo.cr2"
        out = tmp_path / "out"

        expected = MagicMock(returncode=0, stdout="ok", stderr="")
        with patch("subprocess.run", return_value=expected) as mock_run:
            result = dig.run_darktable_cli(fake_cli, src, out)
            assert result is expected


# ---------------------------------------------------------------------------
# build_dest_hash_index
# ---------------------------------------------------------------------------

class TestBuildDestHashIndex:
    def test_empty_folder(self, tmp_path):
        assert dig.build_dest_hash_index(tmp_path) == {}

    def test_nonexistent_folder(self, tmp_path):
        assert dig.build_dest_hash_index(tmp_path / "nope") == {}

    def test_indexes_files(self, tmp_path):
        data = b"content"
        f = tmp_path / "file.jpg"
        f.write_bytes(data)
        index = dig.build_dest_hash_index(tmp_path)
        assert _sha256(data) in index
        assert index[_sha256(data)] == f


# ---------------------------------------------------------------------------
# IngestGuard — guard mode
# ---------------------------------------------------------------------------

class TestIngestGuardMode:
    def test_file_already_in_dest_is_deleted_from_source(
        self, tmp_source, tmp_dest, logger
    ):
        data = b"photo data"
        src_file = tmp_source / "IMG_001.jpg"
        src_file.write_bytes(data)

        dest_folder = tmp_dest / "2024" / "01"
        dest_folder.mkdir(parents=True)
        (dest_folder / "IMG_001.jpg").write_bytes(data)

        guard = _make_guard(tmp_source, tmp_dest, logger)
        with patch.object(guard, "_dest_folder_for", return_value=dest_folder):
            guard.run()

        assert not src_file.exists(), "Source should be deleted"
        assert guard.stats[".jpg"]["found_in_dest"] == 1

    def test_file_not_in_dest_is_copied(self, tmp_source, tmp_dest, logger):
        data = b"new photo"
        src_file = tmp_source / "IMG_002.jpg"
        src_file.write_bytes(data)

        dest_folder = tmp_dest / "2024" / "02"

        guard = _make_guard(tmp_source, tmp_dest, logger)
        with patch.object(guard, "_dest_folder_for", return_value=dest_folder):
            guard.run()

        assert not src_file.exists(), "Source should be deleted after copy"
        assert (dest_folder / "IMG_002.jpg").exists(), "File should be in dest"
        assert guard.stats[".jpg"]["copied"] == 1

    def test_dry_run_does_not_modify_files(self, tmp_source, tmp_dest, logger):
        data = b"dry run photo"
        src_file = tmp_source / "IMG_003.jpg"
        src_file.write_bytes(data)

        dest_folder = tmp_dest / "2024" / "03"

        guard = _make_guard(tmp_source, tmp_dest, logger, dry_run=True)
        with patch.object(guard, "_dest_folder_for", return_value=dest_folder):
            guard.run()

        assert src_file.exists(), "Source must not be deleted in dry-run"
        assert not dest_folder.exists(), "Destination must not be created in dry-run"

    def test_filename_collision_resolved(self, tmp_source, tmp_dest, logger):
        data = b"unique content"
        src_file = tmp_source / "IMG_001.jpg"
        src_file.write_bytes(data)

        dest_folder = tmp_dest / "2024" / "01"
        dest_folder.mkdir(parents=True)
        # Put a *different* file with the same name in dest
        (dest_folder / "IMG_001.jpg").write_bytes(b"different content")

        guard = _make_guard(tmp_source, tmp_dest, logger)
        with patch.object(guard, "_dest_folder_for", return_value=dest_folder):
            guard.run()

        # The source file should have been copied with a counter suffix
        assert (dest_folder / "IMG_001_1.jpg").exists()
        assert guard.stats[".jpg"]["copied"] == 1

    def test_missing_source_dir_exits(self, tmp_path, tmp_dest, logger):
        guard = _make_guard(tmp_path / "no_source", tmp_dest, logger)
        with pytest.raises(SystemExit):
            guard.run()

    def test_missing_dest_dir_exits(self, tmp_source, tmp_path, logger):
        guard = _make_guard(tmp_source, tmp_path / "no_dest", logger)
        with pytest.raises(SystemExit):
            guard.run()


# ---------------------------------------------------------------------------
# IngestGuard — CLI-import mode (photos)
# ---------------------------------------------------------------------------

class TestIngestGuardCliImportPhotos:
    def _make_cli_guard(self, source, dest, logger, cli_path=None, **kwargs):
        cli = cli_path or (source.parent / "fake-cli")
        cli.write_bytes(b"")
        cli.chmod(0o755)
        return _make_guard(source, dest, logger, darktable_cli=cli, **kwargs)

    def test_photo_imported_successfully(self, tmp_source, tmp_dest, logger):
        src_file = tmp_source / "IMG_001.CR2"
        src_file.write_bytes(b"raw data")

        dest_folder = tmp_dest / "2024" / "01"

        def fake_cli(*_args, **_kwargs):
            # Simulate darktable-cli creating the output file
            dest_folder.mkdir(parents=True, exist_ok=True)
            (dest_folder / "IMG_001.jpg").write_bytes(b"processed")
            return MagicMock(returncode=0, stdout="", stderr="")

        guard = self._make_cli_guard(tmp_source, tmp_dest, logger)
        with (
            patch.object(guard, "_dest_folder_for", return_value=dest_folder),
            patch("darktable_ingest_guard.run_darktable_cli", side_effect=fake_cli),
        ):
            guard.run()

        assert not src_file.exists(), "Source should be deleted after import"
        assert guard.stats[".cr2"]["imported"] == 1
        assert guard.stats[".cr2"]["error"] == 0

    def test_photo_already_imported_skipped(self, tmp_source, tmp_dest, logger):
        src_file = tmp_source / "IMG_002.CR2"
        src_file.write_bytes(b"raw data")

        dest_folder = tmp_dest / "2024" / "02"
        dest_folder.mkdir(parents=True)
        (dest_folder / "IMG_002.jpg").write_bytes(b"already there")

        guard = self._make_cli_guard(tmp_source, tmp_dest, logger)
        with patch.object(guard, "_dest_folder_for", return_value=dest_folder):
            guard.run()

        assert not src_file.exists(), "Source should be deleted (already imported)"
        assert guard.stats[".cr2"]["found_in_dest"] == 1

    def test_darktable_cli_failure_recorded_as_error(
        self, tmp_source, tmp_dest, logger
    ):
        src_file = tmp_source / "bad.CR2"
        src_file.write_bytes(b"bad raw")

        dest_folder = tmp_dest / "2024" / "01"

        def failing_cli(*_args, **_kwargs):
            return MagicMock(returncode=1, stdout="", stderr="import failed")

        guard = self._make_cli_guard(tmp_source, tmp_dest, logger)
        with (
            patch.object(guard, "_dest_folder_for", return_value=dest_folder),
            patch("darktable_ingest_guard.run_darktable_cli", side_effect=failing_cli),
        ):
            guard.run()

        assert src_file.exists(), "Source must not be deleted after cli failure"
        assert guard.stats[".cr2"]["error"] == 1
        assert guard.stats[".cr2"]["imported"] == 0

    def test_darktable_cli_os_error_recorded_as_error(
        self, tmp_source, tmp_dest, logger
    ):
        src_file = tmp_source / "bad.CR2"
        src_file.write_bytes(b"raw")

        dest_folder = tmp_dest / "2024" / "01"

        guard = self._make_cli_guard(tmp_source, tmp_dest, logger)
        with (
            patch.object(guard, "_dest_folder_for", return_value=dest_folder),
            patch(
                "darktable_ingest_guard.run_darktable_cli",
                side_effect=OSError("not found"),
            ),
        ):
            guard.run()

        assert guard.stats[".cr2"]["error"] == 1

    def test_darktable_cli_no_output_recorded_as_error(
        self, tmp_source, tmp_dest, logger
    ):
        src_file = tmp_source / "IMG_003.CR2"
        src_file.write_bytes(b"raw data")

        dest_folder = tmp_dest / "2024" / "01"

        def silent_cli(*_args, **_kwargs):
            # Creates no output file
            dest_folder.mkdir(parents=True, exist_ok=True)
            return MagicMock(returncode=0, stdout="", stderr="")

        guard = self._make_cli_guard(tmp_source, tmp_dest, logger)
        with (
            patch.object(guard, "_dest_folder_for", return_value=dest_folder),
            patch("darktable_ingest_guard.run_darktable_cli", side_effect=silent_cli),
        ):
            guard.run()

        assert src_file.exists(), "Source must not be deleted when no output produced"
        assert guard.stats[".cr2"]["error"] == 1

    def test_dry_run_cli_mode_does_not_call_darktable_cli(
        self, tmp_source, tmp_dest, logger
    ):
        src_file = tmp_source / "IMG_004.CR2"
        src_file.write_bytes(b"raw data")

        dest_folder = tmp_dest / "2024" / "01"

        guard = self._make_cli_guard(tmp_source, tmp_dest, logger, dry_run=True)
        with (
            patch.object(guard, "_dest_folder_for", return_value=dest_folder),
            patch("darktable_ingest_guard.run_darktable_cli") as mock_cli,
        ):
            guard.run()

        mock_cli.assert_not_called()
        assert src_file.exists(), "Source must not be modified in dry-run"

    def test_missing_darktable_cli_executable_exits(
        self, tmp_source, tmp_dest, logger
    ):
        guard = _make_guard(
            tmp_source, tmp_dest, logger,
            darktable_cli=tmp_source.parent / "nonexistent-cli",
        )
        with pytest.raises(SystemExit):
            guard.run()


# ---------------------------------------------------------------------------
# IngestGuard — CLI-import mode (videos always use hash-based copy)
# ---------------------------------------------------------------------------

class TestIngestGuardCliImportVideos:
    def test_video_copied_directly_not_via_darktable_cli(
        self, tmp_source, tmp_dest, logger
    ):
        data = b"video content"
        src_file = tmp_source / "clip.mov"
        src_file.write_bytes(data)

        dest_folder = tmp_dest / "2024" / "01"

        fake_cli = tmp_source.parent / "fake-cli"
        fake_cli.write_bytes(b"")
        fake_cli.chmod(0o755)

        guard = _make_guard(tmp_source, tmp_dest, logger, darktable_cli=fake_cli)
        with (
            patch.object(guard, "_dest_folder_for", return_value=dest_folder),
            patch("darktable_ingest_guard.run_darktable_cli") as mock_cli,
        ):
            guard.run()

        # darktable-cli must NOT be called for videos
        mock_cli.assert_not_called()
        # Video should be copied via the normal hash-based path
        assert (dest_folder / "clip.mov").exists()
        assert guard.stats[".mov"]["copied"] == 1


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_basic_guard_mode(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        ns = dig.parse_args(["--source", str(src), "--dest", str(dst)])
        assert ns.darktable_cli is None
        assert ns.darktable_cli_args == []
        assert not ns.dry_run

    def test_cli_import_mode(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        cli = tmp_path / "darktable-cli"
        src.mkdir()
        dst.mkdir()
        ns = dig.parse_args([
            "--source", str(src),
            "--dest", str(dst),
            "--darktable-cli", str(cli),
        ])
        assert ns.darktable_cli == cli.resolve()

    def test_darktable_cli_args_forwarded(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        cli = tmp_path / "darktable-cli"
        src.mkdir()
        dst.mkdir()
        ns = dig.parse_args([
            "--source", str(src),
            "--dest", str(dst),
            "--darktable-cli", str(cli),
            "--darktable-cli-args", "--out-ext", "tif",
        ])
        assert "--out-ext" in ns.darktable_cli_args
        assert "tif" in ns.darktable_cli_args

    def test_dry_run_flag(self, tmp_path):
        src = tmp_path / "src"
        dst = tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        ns = dig.parse_args([
            "--source", str(src),
            "--dest", str(dst),
            "--dry-run",
        ])
        assert ns.dry_run
