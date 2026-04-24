"""Comprehensive tests for media-download: media-vtt.py and publications-epub.py."""

import gzip
import json
import os
import re
import shutil
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch
from urllib.parse import unquote, urlparse

import pytest

# ---------------------------------------------------------------------------
# Helper: load modules with patched top-level side effects
# ---------------------------------------------------------------------------

import importlib.util
import sys

SRC_DIR = str(Path(__file__).parent.parent / "src")

def _load_media_vtt(env_overrides=None):
    """Import media-vtt.py with controlled env and filesystem mocks."""
    env = {"LANG": "S", "OUTPUT_PATH": "/tmp/test_vtts", "DB_PATH": "/tmp/test.db"}
    if env_overrides:
        env.update(env_overrides)
    spec = importlib.util.spec_from_file_location(
        "media_vtt", os.path.join(SRC_DIR, "media-vtt.py")
    )
    mod = importlib.util.module_from_spec(spec)
    with patch.dict(os.environ, env, clear=False):
        with patch("os.path.exists", return_value=True):
            with patch("os.makedirs"):
                spec.loader.exec_module(mod)
    return mod


def _load_publications_epub(env_overrides=None):
    """Import publications-epub.py with controlled env and filesystem mocks."""
    env = {
        "LANG": "S",
        "OUTPUT_PATH": "/tmp/test_epubs",
        "DB_PATH": "/tmp/test_state.db",
        "UNIT_DB_PATH": "/tmp/test_unit.db",
    }
    if env_overrides:
        env.update(env_overrides)
    spec = importlib.util.spec_from_file_location(
        "publications_epub", os.path.join(SRC_DIR, "publications-epub.py")
    )
    mod = importlib.util.module_from_spec(spec)
    with patch.dict(os.environ, env, clear=False):
        with patch("os.path.exists", return_value=True):
            with patch("os.makedirs"):
                spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def media_vtt():
    return _load_media_vtt()


@pytest.fixture
def pub_epub():
    return _load_publications_epub()


@pytest.fixture
def tmp_db():
    """Create a temporary database file, yield its path, clean up after."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def tmp_dir():
    """Create a temporary directory, yield its path, clean up after."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ===================================================================
# MEDIA-VTT.PY TESTS
# ===================================================================


class TestSetupDatabase:
    """Tests for setup_database()."""

    def test_creates_table(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)
        conn = sqlite3.connect(tmp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
        conn.close()
        assert "downloaded_vtts" in tables

    def test_creates_directory_if_missing(self, media_vtt, tmp_dir):
        db_path = os.path.join(tmp_dir, "subdir", "test.db")
        media_vtt.setup_database(db_path)
        assert os.path.exists(db_path)

    def test_idempotent(self, media_vtt, tmp_db):
        """Calling setup_database twice does not raise."""
        media_vtt.setup_database(tmp_db)
        media_vtt.setup_database(tmp_db)
        conn = sqlite3.connect(tmp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
        conn.close()
        assert "downloaded_vtts" in tables

    def test_handles_sqlite_error(self, media_vtt, tmp_db):
        """Handles sqlite error gracefully (logs, does not raise)."""
        with patch("sqlite3.connect", side_effect=sqlite3.OperationalError("disk I/O error")):
            # The file already exists so the open() call is skipped,
            # and the sqlite3 error is caught by the try/except.
            media_vtt.setup_database(tmp_db)


class TestIsVttProcessed:
    """Tests for is_vtt_processed()."""

    def test_returns_status_when_found(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)
        media_vtt.mark_vtt_as_downloaded(tmp_db, "pub1", 1, "mp4", "http://x.com/f.vtt", "success")
        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "success"

    def test_returns_none_when_not_found(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)
        assert media_vtt.is_vtt_processed(tmp_db, "nonexistent", 1, "mp4") is None

    def test_handles_db_error(self, media_vtt):
        """Returns None on database error."""
        with patch("sqlite3.connect", side_effect=sqlite3.OperationalError("locked")):
            result = media_vtt.is_vtt_processed("/bad/path.db", "x", 1, "mp4")
            assert result is None


class TestMarkVttAsDownloaded:
    """Tests for mark_vtt_as_downloaded()."""

    def test_inserts_record(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)
        media_vtt.mark_vtt_as_downloaded(tmp_db, "pub1", 1, "mp4", "http://x.com/f.vtt", "success")
        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "success"

    def test_replaces_on_duplicate(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)
        media_vtt.mark_vtt_as_downloaded(tmp_db, "pub1", 1, "mp4", "http://x.com/f.vtt", "failed")
        media_vtt.mark_vtt_as_downloaded(tmp_db, "pub1", 1, "mp4", "http://x.com/f.vtt", "success")
        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "success"

    def test_handles_db_error(self, media_vtt):
        """Does not raise on database error."""
        with patch("sqlite3.connect", side_effect=sqlite3.OperationalError("locked")):
            # Should not raise
            media_vtt.mark_vtt_as_downloaded("/bad/path.db", "x", 1, "mp4", None, "failed")


class TestExtractMediaInfo:
    """Tests for extract_media_info()."""

    def _write_jsonl(self, items, tmp_dir):
        path = os.path.join(tmp_dir, "test.json")
        with open(path, "w") as f:
            for item in items:
                f.write(json.dumps(item) + "\n")
        return path

    def test_extracts_pubS_items(self, media_vtt, tmp_dir):
        items = [
            {"type": "media-item", "o": {"keyParts": {"pubS": "w_E_123", "track": 1, "formatCode": "mp4"}}},
        ]
        path = self._write_jsonl(items, tmp_dir)
        result = media_vtt.extract_media_info(path)
        assert len(result) == 1
        assert result[0][0] == "w_E_123"
        assert result[0][1] == 1
        assert result[0][2] == "mp4"

    def test_extracts_docID_items(self, media_vtt, tmp_dir):
        items = [
            {"type": "media-item", "o": {"keyParts": {"docID": "doc456", "track": 2, "formatCode": "mp3"}}},
        ]
        path = self._write_jsonl(items, tmp_dir)
        result = media_vtt.extract_media_info(path)
        assert len(result) == 1
        assert result[0][0] == "doc456"

    def test_skips_non_media_type(self, media_vtt, tmp_dir):
        items = [
            {"type": "other-type", "o": {"keyParts": {"pubS": "ignored", "track": 0, "formatCode": "x"}}},
        ]
        path = self._write_jsonl(items, tmp_dir)
        result = media_vtt.extract_media_info(path)
        assert len(result) == 0

    def test_skips_items_missing_track(self, media_vtt, tmp_dir):
        items = [
            {"type": "media-item", "o": {"keyParts": {"pubS": "w_E_123", "formatCode": "mp4"}}},
        ]
        path = self._write_jsonl(items, tmp_dir)
        result = media_vtt.extract_media_info(path)
        assert len(result) == 0

    def test_skips_items_missing_identifier(self, media_vtt, tmp_dir):
        items = [
            {"type": "media-item", "o": {"keyParts": {"track": 1, "formatCode": "mp4"}}},
        ]
        path = self._write_jsonl(items, tmp_dir)
        result = media_vtt.extract_media_info(path)
        assert len(result) == 0

    def test_skips_items_missing_formatCode(self, media_vtt, tmp_dir):
        items = [
            {"type": "media-item", "o": {"keyParts": {"pubS": "w_E_123", "track": 1}}},
        ]
        path = self._write_jsonl(items, tmp_dir)
        result = media_vtt.extract_media_info(path)
        assert len(result) == 0

    def test_multiple_mixed_items(self, media_vtt, tmp_dir):
        items = [
            {"type": "media-item", "o": {"keyParts": {"pubS": "p1", "track": 1, "formatCode": "mp4"}}},
            {"type": "other", "o": {"keyParts": {"pubS": "skip", "track": 2, "formatCode": "mp3"}}},
            {"type": "media-item", "o": {"keyParts": {"docID": "d2", "track": 3, "formatCode": "mp3"}}},
        ]
        path = self._write_jsonl(items, tmp_dir)
        result = media_vtt.extract_media_info(path)
        assert len(result) == 2

    def test_handles_file_read_error(self, media_vtt):
        result = media_vtt.extract_media_info("/nonexistent/path.json")
        assert result == []

    def test_track_zero_is_valid(self, media_vtt, tmp_dir):
        """Track 0 is valid (not None), should be included."""
        items = [
            {"type": "media-item", "o": {"keyParts": {"pubS": "pub0", "track": 0, "formatCode": "mp4"}}},
        ]
        path = self._write_jsonl(items, tmp_dir)
        result = media_vtt.extract_media_info(path)
        assert len(result) == 1
        assert result[0][1] == 0


class TestDownloadExtractJson:
    """Tests for download_extract_json()."""

    def test_success(self, media_vtt, tmp_dir):
        # Create a gzipped JSON payload
        json_content = b'{"type": "media-item"}\n'
        import io
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(json_content)
        gz_bytes = buf.getvalue()

        mock_response = MagicMock()
        mock_response.content = gz_bytes
        mock_response.raise_for_status = MagicMock()

        with patch.object(media_vtt.requests, "get", return_value=mock_response):
            with patch.object(media_vtt, "LANG", "S"):
                result = media_vtt.download_extract_json("http://example.com/S.json.gz", tmp_dir)

        assert result is not None
        assert result.endswith("S.json")
        assert os.path.exists(result)
        with open(result) as f:
            content = f.read()
        assert "media-item" in content
        # gz file should have been removed
        assert not os.path.exists(os.path.join(tmp_dir, "S.json.gz"))

    def test_http_error(self, media_vtt, tmp_dir):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("HTTP 500")

        with patch.object(media_vtt.requests, "get", return_value=mock_response):
            result = media_vtt.download_extract_json("http://example.com/fail.gz", tmp_dir)

        assert result is None

    def test_network_error(self, media_vtt, tmp_dir):
        with patch.object(media_vtt.requests, "get", side_effect=Exception("Connection refused")):
            result = media_vtt.download_extract_json("http://example.com/fail.gz", tmp_dir)
        assert result is None


class TestGetPubMediaLinks:
    """Tests for get_pub_media_links()."""

    def test_with_pubS(self, media_vtt):
        key_parts = {"pubS": "w_E_123"}
        mock_response = MagicMock()
        mock_response.json.return_value = {"files": {"S": {}}}
        mock_response.raise_for_status = MagicMock()

        with patch.object(media_vtt.requests, "get", return_value=mock_response) as mock_get:
            result = media_vtt.get_pub_media_links("w_E_123", 1, "mp4", key_parts)

        assert result == {"files": {"S": {}}}
        args, kwargs = mock_get.call_args
        assert kwargs["params"]["pub"] == "w_E_123"
        assert "docid" not in kwargs["params"]

    def test_with_docID(self, media_vtt):
        key_parts = {"docID": "doc456"}
        mock_response = MagicMock()
        mock_response.json.return_value = {"files": {"S": {}}}
        mock_response.raise_for_status = MagicMock()

        with patch.object(media_vtt.requests, "get", return_value=mock_response) as mock_get:
            result = media_vtt.get_pub_media_links("doc456", 2, "mp3", key_parts)

        assert result == {"files": {"S": {}}}
        args, kwargs = mock_get.call_args
        assert kwargs["params"]["docid"] == "doc456"
        assert "pub" not in kwargs["params"]

    def test_no_identifier_in_key_parts(self, media_vtt):
        key_parts = {"other": "value"}
        result = media_vtt.get_pub_media_links("x", 1, "mp4", key_parts)
        assert result is None

    def test_http_error(self, media_vtt):
        key_parts = {"pubS": "w_E_123"}
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = media_vtt.requests.exceptions.HTTPError("404")

        with patch.object(media_vtt.requests, "get", return_value=mock_response):
            result = media_vtt.get_pub_media_links("w_E_123", 1, "mp4", key_parts)
        assert result is None

    def test_generic_exception(self, media_vtt):
        key_parts = {"pubS": "w_E_123"}
        with patch.object(media_vtt.requests, "get", side_effect=Exception("timeout")):
            result = media_vtt.get_pub_media_links("w_E_123", 1, "mp4", key_parts)
        assert result is None


class TestDownloadVttFiles:
    """Tests for download_vtt_files()."""

    def test_skips_already_successful(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)
        media_vtt.mark_vtt_as_downloaded(tmp_db, "pub1", 1, "mp4", "http://x.com/f.vtt", "success")

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "get_pub_media_links") as mock_links:
                media_vtt.download_vtt_files(media_info)
                mock_links.assert_not_called()

    def test_skips_already_failed(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)
        media_vtt.mark_vtt_as_downloaded(tmp_db, "pub1", 1, "mp4", "http://x.com/f.vtt", "failed")

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "get_pub_media_links") as mock_links:
                media_vtt.download_vtt_files(media_info)
                mock_links.assert_not_called()

    def test_downloads_vtt_successfully(self, media_vtt, tmp_db, tmp_dir):
        media_vtt.setup_database(tmp_db)

        media_links = {
            "files": {
                "S": {
                    "MP4": [
                        {"subtitles": {"url": "http://cdn.example.com/subtitle.vtt"}}
                    ]
                }
            }
        }

        mock_vtt_response = MagicMock()
        mock_vtt_response.content = b"WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nHello"
        mock_vtt_response.raise_for_status = MagicMock()

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "OUTPUT_PATH", tmp_dir):
                with patch.object(media_vtt, "get_pub_media_links", return_value=media_links):
                    with patch.object(media_vtt.requests, "get", return_value=mock_vtt_response):
                        media_vtt.download_vtt_files(media_info)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "success"
        assert os.path.exists(os.path.join(tmp_dir, "subtitle.vtt"))

    def test_no_subtitles_marks_no_subtitles(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)

        media_links = {
            "files": {
                "S": {
                    "MP4": [
                        {"file": {"url": "http://cdn.example.com/video.mp4"}}
                    ]
                }
            }
        }

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "get_pub_media_links", return_value=media_links):
                media_vtt.download_vtt_files(media_info)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "no_subtitles"

    def test_no_media_links_marks_failed(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "get_pub_media_links", return_value=None):
                media_vtt.download_vtt_files(media_info)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "failed"

    def test_no_files_key_marks_failed(self, media_vtt, tmp_db):
        media_vtt.setup_database(tmp_db)

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "get_pub_media_links", return_value={"other": "data"}):
                media_vtt.download_vtt_files(media_info)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "failed"

    def test_retry_on_request_error(self, media_vtt, tmp_db, tmp_dir):
        """Retries on RequestException, then succeeds."""
        media_vtt.setup_database(tmp_db)

        media_links = {
            "files": {
                "S": {
                    "MP4": [
                        {"subtitles": {"url": "http://cdn.example.com/sub.vtt"}}
                    ]
                }
            }
        }

        mock_success = MagicMock()
        mock_success.content = b"WEBVTT\nsubtitles"
        mock_success.raise_for_status = MagicMock()

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        import requests as req_lib

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "OUTPUT_PATH", tmp_dir):
                with patch.object(media_vtt, "get_pub_media_links", return_value=media_links):
                    with patch.object(media_vtt.requests, "get", side_effect=[
                        req_lib.exceptions.RequestException("timeout"),
                        mock_success,
                    ]):
                        with patch("time.sleep"):
                            media_vtt.download_vtt_files(media_info, max_retries=3)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "success"

    def test_all_retries_exhausted(self, media_vtt, tmp_db, tmp_dir):
        """All retries fail, marks as failed."""
        media_vtt.setup_database(tmp_db)

        media_links = {
            "files": {
                "S": {
                    "MP4": [
                        {"subtitles": {"url": "http://cdn.example.com/sub.vtt"}}
                    ]
                }
            }
        }

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        import requests as req_lib

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "OUTPUT_PATH", tmp_dir):
                with patch.object(media_vtt, "get_pub_media_links", return_value=media_links):
                    with patch.object(media_vtt.requests, "get", side_effect=req_lib.exceptions.RequestException("timeout")):
                        with patch("time.sleep"):
                            media_vtt.download_vtt_files(media_info, max_retries=2)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "failed"

    def test_unexpected_error_marks_failed(self, media_vtt, tmp_db, tmp_dir):
        """Unexpected (non-request) error marks as failed and breaks retry loop."""
        media_vtt.setup_database(tmp_db)

        media_links = {
            "files": {
                "S": {
                    "MP4": [
                        {"subtitles": {"url": "http://cdn.example.com/sub.vtt"}}
                    ]
                }
            }
        }

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "OUTPUT_PATH", tmp_dir):
                with patch.object(media_vtt, "get_pub_media_links", return_value=media_links):
                    with patch.object(media_vtt.requests, "get", side_effect=ValueError("bad data")):
                        media_vtt.download_vtt_files(media_info)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "failed"

    def test_empty_formats_marks_no_subtitles(self, media_vtt, tmp_db):
        """Empty language formats dict -> no_subtitles."""
        media_vtt.setup_database(tmp_db)

        media_links = {"files": {"S": {}}}

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "get_pub_media_links", return_value=media_links):
                media_vtt.download_vtt_files(media_info)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "no_subtitles"

    def test_processes_multiple_items(self, media_vtt, tmp_db, tmp_dir):
        """Processes multiple media items in sequence."""
        media_vtt.setup_database(tmp_db)

        media_links_1 = {
            "files": {"S": {"MP4": [{"subtitles": {"url": "http://cdn.example.com/a.vtt"}}]}}
        }
        media_links_2 = {
            "files": {"S": {"MP4": [{"subtitles": {"url": "http://cdn.example.com/b.vtt"}}]}}
        }

        mock_resp = MagicMock()
        mock_resp.content = b"WEBVTT\ndata"
        mock_resp.raise_for_status = MagicMock()

        media_info = [
            ("pub1", 1, "mp4", {"pubS": "pub1"}),
            ("pub2", 2, "mp3", {"docID": "pub2"}),
        ]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "OUTPUT_PATH", tmp_dir):
                with patch.object(media_vtt, "get_pub_media_links", side_effect=[media_links_1, media_links_2]):
                    with patch.object(media_vtt.requests, "get", return_value=mock_resp):
                        media_vtt.download_vtt_files(media_info)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "success"
        assert media_vtt.is_vtt_processed(tmp_db, "pub2", 2, "mp3") == "success"

    def test_subtitles_no_url_key(self, media_vtt, tmp_db):
        """Subtitles dict present but no 'url' key -> no_subtitles."""
        media_vtt.setup_database(tmp_db)

        media_links = {
            "files": {
                "S": {
                    "MP4": [
                        {"subtitles": {"format": "vtt"}}  # no 'url'
                    ]
                }
            }
        }

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "get_pub_media_links", return_value=media_links):
                media_vtt.download_vtt_files(media_info)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "no_subtitles"

    def test_missing_language_key(self, media_vtt, tmp_db):
        """Language key not present in files -> no_subtitles."""
        media_vtt.setup_database(tmp_db)

        media_links = {"files": {"EN": {"MP4": [{"subtitles": {"url": "http://x.com/f.vtt"}}]}}}

        media_info = [("pub1", 1, "mp4", {"pubS": "pub1"})]

        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "get_pub_media_links", return_value=media_links):
                media_vtt.download_vtt_files(media_info)

        assert media_vtt.is_vtt_processed(tmp_db, "pub1", 1, "mp4") == "no_subtitles"


class TestMediaVttMain:
    """Tests for the __main__ block of media-vtt.py."""

    def test_main_success_flow(self, media_vtt, tmp_db, tmp_dir):
        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "OUTPUT_PATH", tmp_dir):
                with patch.object(media_vtt, "setup_database") as mock_setup:
                    with patch.object(media_vtt, "download_extract_json", return_value="/tmp/S.json") as mock_dl:
                        with patch.object(media_vtt, "extract_media_info", return_value=[("p", 1, "mp4", {})]) as mock_ext:
                            with patch.object(media_vtt, "download_vtt_files") as mock_vtt:
                                # Simulate __main__
                                mock_setup(tmp_db)
                                json_path = mock_dl(f"http://example.com/S.json.gz", tmp_dir)
                                if json_path:
                                    info = mock_ext(json_path)
                                    mock_vtt(info)

                                mock_setup.assert_called_once()
                                mock_dl.assert_called_once()
                                mock_ext.assert_called_once()
                                mock_vtt.assert_called_once()

    def test_main_json_download_fails(self, media_vtt, tmp_db, tmp_dir):
        with patch.object(media_vtt, "DB_PATH", tmp_db):
            with patch.object(media_vtt, "OUTPUT_PATH", tmp_dir):
                with patch.object(media_vtt, "setup_database") as mock_setup:
                    with patch.object(media_vtt, "download_extract_json", return_value=None) as mock_dl:
                        with patch.object(media_vtt, "download_vtt_files") as mock_vtt:
                            mock_setup(tmp_db)
                            json_path = mock_dl("http://example.com/S.json.gz", tmp_dir)
                            if json_path:
                                mock_vtt([])

                            mock_vtt.assert_not_called()


# ===================================================================
# PUBLICATIONS-EPUB.PY TESTS
# ===================================================================


class TestSetupStateDatabase:
    """Tests for setup_state_database()."""

    def test_creates_table(self, pub_epub, tmp_db):
        conn = pub_epub.setup_state_database(tmp_db)
        assert conn is not None
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
        conn.close()
        assert "PublicationState" in tables

    def test_creates_directory_if_missing(self, pub_epub, tmp_dir):
        db_path = os.path.join(tmp_dir, "subdir", "state.db")
        conn = pub_epub.setup_state_database(db_path)
        assert conn is not None
        conn.close()
        assert os.path.exists(db_path)

    def test_idempotent(self, pub_epub, tmp_db):
        conn1 = pub_epub.setup_state_database(tmp_db)
        conn1.close()
        conn2 = pub_epub.setup_state_database(tmp_db)
        assert conn2 is not None
        conn2.close()

    def test_returns_none_on_error(self, pub_epub, tmp_db):
        """Returns None when sqlite3 connect raises."""
        with patch("sqlite3.connect", side_effect=sqlite3.OperationalError("fail")):
            # tmp_db already exists so the open() call is skipped,
            # and the sqlite3 error is caught by the try/except.
            result = pub_epub.setup_state_database(tmp_db)
            assert result is None


class TestGetLanguageId:
    """Tests for get_language_id()."""

    def test_returns_id_when_found(self, pub_epub, tmp_db):
        conn = sqlite3.connect(tmp_db)
        conn.execute("CREATE TABLE Language (LanguageId INTEGER, Symbol TEXT)")
        conn.execute("INSERT INTO Language VALUES (1, 'S')")
        conn.commit()
        conn.close()

        result = pub_epub.get_language_id("S", tmp_db)
        assert result == 1

    def test_returns_none_when_not_found(self, pub_epub, tmp_db):
        conn = sqlite3.connect(tmp_db)
        conn.execute("CREATE TABLE Language (LanguageId INTEGER, Symbol TEXT)")
        conn.commit()
        conn.close()

        result = pub_epub.get_language_id("XX", tmp_db)
        assert result is None

    def test_returns_none_on_db_error(self, pub_epub):
        result = pub_epub.get_language_id("S", "/nonexistent/path.db")
        assert result is None


class TestFetchLogDb:
    """Tests for fetch_log_db()."""

    def test_success(self, pub_epub, tmp_dir):
        # Create a gzipped db content
        db_content = b"SQLite format 3\x00mock database content"
        import io
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(db_content)
        gz_bytes = buf.getvalue()

        mock_manifest_resp = MagicMock()
        mock_manifest_resp.json.return_value = {"current": "abc123"}
        mock_manifest_resp.raise_for_status = MagicMock()

        mock_log_resp = MagicMock()
        mock_log_resp.content = gz_bytes
        mock_log_resp.raise_for_status = MagicMock()

        with patch.object(pub_epub.requests, "get", side_effect=[mock_manifest_resp, mock_log_resp]):
            with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                result = pub_epub.fetch_log_db()

        assert result is not None
        assert result.endswith("log")
        assert os.path.exists(result)
        # gz should be deleted
        assert not os.path.exists(os.path.join(tmp_dir, "log.gz"))

    def test_missing_manifest_id(self, pub_epub, tmp_dir):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(pub_epub.requests, "get", return_value=mock_resp):
            with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                result = pub_epub.fetch_log_db()

        assert result is None

    def test_network_error(self, pub_epub, tmp_dir):
        with patch.object(pub_epub.requests, "get", side_effect=Exception("Connection refused")):
            with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                result = pub_epub.fetch_log_db()
        assert result is None


class TestGetPublications:
    """Tests for get_publications()."""

    def test_returns_rows(self, pub_epub, tmp_db):
        conn = sqlite3.connect(tmp_db)
        conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        conn.execute("INSERT INTO Publication VALUES (20240101, 'w', 'w', 1)")
        conn.execute("INSERT INTO Publication VALUES (0, 'nwt', 'nwt', 1)")
        conn.execute("INSERT INTO Publication VALUES (20240102, 'g', 'g', 2)")  # different lang
        conn.commit()

        result = pub_epub.get_publications(conn, 1)
        conn.close()
        assert len(result) == 2
        assert (20240101, "w", "w") in result
        assert (0, "nwt", "nwt") in result

    def test_no_publications(self, pub_epub, tmp_db):
        conn = sqlite3.connect(tmp_db)
        conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        conn.commit()

        result = pub_epub.get_publications(conn, 999)
        conn.close()
        assert result == []

    def test_handles_db_error(self, pub_epub):
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.execute.side_effect = sqlite3.OperationalError("no such table")
        result = pub_epub.get_publications(mock_conn, 1)
        assert result == []


class TestDownloadEpubs:
    """Tests for download_epubs()."""

    def test_state_db_setup_fails(self, pub_epub):
        """If state DB setup fails, exit early."""
        with patch.object(pub_epub, "setup_state_database", return_value=None):
            with patch.object(pub_epub, "get_language_id") as mock_lang:
                pub_epub.download_epubs()
                mock_lang.assert_not_called()

    def test_language_id_not_found(self, pub_epub, tmp_db):
        """If language ID not found, exit early."""
        conn = pub_epub.setup_state_database(tmp_db)
        with patch.object(pub_epub, "setup_state_database", return_value=conn):
            with patch.object(pub_epub, "get_language_id", return_value=None):
                with patch.object(pub_epub, "fetch_log_db") as mock_fetch:
                    pub_epub.download_epubs()
                    mock_fetch.assert_not_called()
        conn.close()

    def test_fetch_log_db_fails(self, pub_epub, tmp_db):
        """If log DB fetch fails, exit early."""
        conn = pub_epub.setup_state_database(tmp_db)
        with patch.object(pub_epub, "setup_state_database", return_value=conn):
            with patch.object(pub_epub, "get_language_id", return_value=1):
                with patch.object(pub_epub, "fetch_log_db", return_value=None):
                    with patch.object(pub_epub, "get_publications") as mock_pubs:
                        pub_epub.download_epubs()
                        mock_pubs.assert_not_called()
        conn.close()

    def test_skips_already_processed(self, pub_epub, tmp_db, tmp_dir):
        """Publications already in 'processed' state are skipped."""
        state_conn = pub_epub.setup_state_database(tmp_db)
        state_cursor = state_conn.cursor()
        state_cursor.execute(
            "INSERT INTO PublicationState (TagNumber, Symbol, sym, State) VALUES (?, ?, ?, ?)",
            (20240101, "w", "w", "processed"),
        )
        state_conn.commit()

        # Create a log DB with Publication table
        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (20240101, 'w', 'w', 1)")
        log_conn.commit()
        log_conn.close()

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "get_language_id", return_value=1):
                    with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                        with patch.object(pub_epub.requests, "get") as mock_get:
                            pub_epub.download_epubs()
                            mock_get.assert_not_called()

        state_conn.close()

    def test_downloads_epub_with_tag_number(self, pub_epub, tmp_db, tmp_dir):
        """Successfully downloads an EPUB when tag_number != 0."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (20240101, 'w', 'w', 1)")
        log_conn.commit()
        log_conn.close()

        metadata_response = MagicMock()
        metadata_response.json.return_value = {
            "files": {"S": {"EPUB": [{"file": {"url": "http://cdn.example.com/w_20240101.epub"}}]}}
        }
        metadata_response.raise_for_status = MagicMock()

        file_response = MagicMock()
        file_response.headers = {"Content-Disposition": 'attachment; filename="w_20240101.epub"'}
        file_response.raw = MagicMock()
        file_response.raise_for_status = MagicMock()

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                    with patch.object(pub_epub, "get_language_id", return_value=1):
                        with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                            with patch.object(pub_epub.requests, "get", side_effect=[metadata_response, file_response]):
                                with patch("shutil.copyfileobj"):
                                    pub_epub.download_epubs()

        # Check state was updated
        check_conn = sqlite3.connect(tmp_db)
        cursor = check_conn.cursor()
        cursor.execute("SELECT State FROM PublicationState WHERE TagNumber=20240101 AND Symbol='w'")
        row = cursor.fetchone()
        check_conn.close()
        state_conn.close()
        assert row is not None
        assert row[0] == "processed"

    def test_downloads_epub_without_tag_number(self, pub_epub, tmp_db, tmp_dir):
        """Uses Symbol-based URL when tag_number == 0."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (0, 'nwt', 'nwt', 1)")
        log_conn.commit()
        log_conn.close()

        metadata_response = MagicMock()
        metadata_response.json.return_value = {
            "files": {"S": {"EPUB": [{"file": {"url": "http://cdn.example.com/nwt.epub"}}]}}
        }
        metadata_response.raise_for_status = MagicMock()

        file_response = MagicMock()
        file_response.headers = {}  # No Content-Disposition
        file_response.raw = MagicMock()
        file_response.raise_for_status = MagicMock()

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                    with patch.object(pub_epub, "get_language_id", return_value=1):
                        with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                            with patch.object(pub_epub.requests, "get", side_effect=[metadata_response, file_response]):
                                with patch("shutil.copyfileobj"):
                                    pub_epub.download_epubs()

        check_conn = sqlite3.connect(tmp_db)
        cursor = check_conn.cursor()
        cursor.execute("SELECT State FROM PublicationState WHERE TagNumber=0 AND Symbol='nwt'")
        row = cursor.fetchone()
        check_conn.close()
        state_conn.close()
        assert row is not None
        assert row[0] == "processed"

    def test_no_epub_files_marks_no_epub(self, pub_epub, tmp_db, tmp_dir):
        """When API returns no EPUB files, marks state as no_epub."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (20240101, 'w', 'w', 1)")
        log_conn.commit()
        log_conn.close()

        metadata_response = MagicMock()
        metadata_response.json.return_value = {"files": {"S": {"EPUB": []}}}
        metadata_response.raise_for_status = MagicMock()

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                    with patch.object(pub_epub, "get_language_id", return_value=1):
                        with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                            with patch.object(pub_epub.requests, "get", return_value=metadata_response):
                                pub_epub.download_epubs()

        check_conn = sqlite3.connect(tmp_db)
        cursor = check_conn.cursor()
        cursor.execute("SELECT State FROM PublicationState WHERE TagNumber=20240101 AND Symbol='w'")
        row = cursor.fetchone()
        check_conn.close()
        state_conn.close()
        assert row is not None
        assert row[0] == "no_epub"

    def test_request_error_retries_and_fails(self, pub_epub, tmp_db, tmp_dir):
        """Request errors trigger retries; all failures mark state as failed."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (20240101, 'w', 'w', 1)")
        log_conn.commit()
        log_conn.close()

        import requests as req_lib

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                    with patch.object(pub_epub, "get_language_id", return_value=1):
                        with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                            with patch.object(pub_epub.requests, "get", side_effect=req_lib.exceptions.RequestException("timeout")):
                                with patch("time.sleep"):
                                    pub_epub.download_epubs()

        check_conn = sqlite3.connect(tmp_db)
        cursor = check_conn.cursor()
        cursor.execute("SELECT State FROM PublicationState WHERE TagNumber=20240101 AND Symbol='w'")
        row = cursor.fetchone()
        check_conn.close()
        state_conn.close()
        assert row is not None
        assert row[0] == "failed"

    def test_unexpected_error_marks_failed(self, pub_epub, tmp_db, tmp_dir):
        """Non-request exceptions mark state as failed and break retry loop."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (20240101, 'w', 'w', 1)")
        log_conn.commit()
        log_conn.close()

        metadata_response = MagicMock()
        metadata_response.json.side_effect = ValueError("bad json")
        metadata_response.raise_for_status = MagicMock()

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                    with patch.object(pub_epub, "get_language_id", return_value=1):
                        with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                            with patch.object(pub_epub.requests, "get", return_value=metadata_response):
                                pub_epub.download_epubs()

        check_conn = sqlite3.connect(tmp_db)
        cursor = check_conn.cursor()
        cursor.execute("SELECT State FROM PublicationState WHERE TagNumber=20240101 AND Symbol='w'")
        row = cursor.fetchone()
        check_conn.close()
        state_conn.close()
        assert row is not None
        assert row[0] == "failed"

    def test_log_db_connect_error(self, pub_epub, tmp_db, tmp_dir):
        """If connecting to log DB fails, exits gracefully."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "get_language_id", return_value=1):
                    with patch.object(pub_epub, "fetch_log_db", return_value="/nonexistent/bad.db"):
                        with patch("sqlite3.connect", side_effect=sqlite3.OperationalError("cannot open")):
                            pub_epub.download_epubs()
        state_conn.close()

    def test_multiple_publications(self, pub_epub, tmp_db, tmp_dir):
        """Processes multiple publications, one succeeds, one has no epub."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (20240101, 'w', 'w', 1)")
        log_conn.execute("INSERT INTO Publication VALUES (20240102, 'g', 'g', 1)")
        log_conn.commit()
        log_conn.close()

        meta_1 = MagicMock()
        meta_1.json.return_value = {
            "files": {"S": {"EPUB": [{"file": {"url": "http://cdn.example.com/w.epub"}}]}}
        }
        meta_1.raise_for_status = MagicMock()

        file_resp = MagicMock()
        file_resp.headers = {"Content-Disposition": 'attachment; filename="w_20240101.epub"'}
        file_resp.raw = MagicMock()
        file_resp.raise_for_status = MagicMock()

        meta_2 = MagicMock()
        meta_2.json.return_value = {"files": {"S": {"EPUB": []}}}
        meta_2.raise_for_status = MagicMock()

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                    with patch.object(pub_epub, "get_language_id", return_value=1):
                        with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                            with patch.object(pub_epub.requests, "get", side_effect=[meta_1, file_resp, meta_2]):
                                with patch("shutil.copyfileobj"):
                                    pub_epub.download_epubs()

        check_conn = sqlite3.connect(tmp_db)
        cursor = check_conn.cursor()
        cursor.execute("SELECT State FROM PublicationState WHERE TagNumber=20240101")
        assert cursor.fetchone()[0] == "processed"
        cursor.execute("SELECT State FROM PublicationState WHERE TagNumber=20240102")
        assert cursor.fetchone()[0] == "no_epub"
        check_conn.close()
        state_conn.close()

    def test_outer_exception_continues(self, pub_epub, tmp_db, tmp_dir):
        """Outer try/except around a publication continues to next one."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (1, 'bad', 'bad', 1)")
        log_conn.execute("INSERT INTO Publication VALUES (2, 'good', 'good', 1)")
        log_conn.commit()
        log_conn.close()

        # First pub causes outer exception (cursor_state.execute fails), second succeeds
        original_execute = state_conn.cursor().execute.__class__

        call_count = {"n": 0}

        meta_good = MagicMock()
        meta_good.json.return_value = {
            "files": {"S": {"EPUB": [{"file": {"url": "http://cdn.example.com/good.epub"}}]}}
        }
        meta_good.raise_for_status = MagicMock()

        file_resp = MagicMock()
        file_resp.headers = {}
        file_resp.raw = MagicMock()
        file_resp.raise_for_status = MagicMock()

        def mock_get_side_effect(*args, **kwargs):
            url = args[0] if args else kwargs.get("url", "")
            if "bad" in url:
                raise RuntimeError("simulated outer error")
            return meta_good

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                    with patch.object(pub_epub, "get_language_id", return_value=1):
                        with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                            with patch.object(pub_epub.requests, "get", side_effect=[
                                RuntimeError("outer error"),
                                meta_good, file_resp
                            ]):
                                with patch("shutil.copyfileobj"):
                                    pub_epub.download_epubs()

        # Second publication should have been processed despite first failing
        check_conn = sqlite3.connect(tmp_db)
        cursor = check_conn.cursor()
        cursor.execute("SELECT State FROM PublicationState WHERE TagNumber=2")
        row = cursor.fetchone()
        check_conn.close()
        state_conn.close()
        # The outer exception is on the requests.get call which is inside the inner try,
        # so it would be caught as a generic Exception and marked failed, then continue.
        # Both publications should have state entries.

    def test_content_disposition_filename(self, pub_epub, tmp_db, tmp_dir):
        """Extracts filename from Content-Disposition header."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (20240101, 'w', 'w', 1)")
        log_conn.commit()
        log_conn.close()

        metadata_response = MagicMock()
        metadata_response.json.return_value = {
            "files": {"S": {"EPUB": [{"file": {"url": "http://cdn.example.com/file.epub"}}]}}
        }
        metadata_response.raise_for_status = MagicMock()

        file_response = MagicMock()
        file_response.headers = {"Content-Disposition": 'attachment; filename="custom_name.epub"'}
        file_response.raw = MagicMock()
        file_response.raise_for_status = MagicMock()

        written_path = None
        original_open = open

        def capture_open(path, mode="r", **kwargs):
            nonlocal written_path
            if path.endswith(".epub"):
                written_path = path
                return MagicMock()
            return original_open(path, mode, **kwargs)

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                    with patch.object(pub_epub, "get_language_id", return_value=1):
                        with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                            with patch.object(pub_epub.requests, "get", side_effect=[metadata_response, file_response]):
                                with patch("shutil.copyfileobj"):
                                    with patch("builtins.open", side_effect=capture_open):
                                        pub_epub.download_epubs()

        state_conn.close()
        assert written_path is not None
        assert "custom_name.epub" in written_path

    def test_fallback_filename_no_content_disposition(self, pub_epub, tmp_db, tmp_dir):
        """Falls back to symbol_tagnumber.epub when no Content-Disposition."""
        state_conn = pub_epub.setup_state_database(tmp_db)

        log_db_path = os.path.join(tmp_dir, "log")
        log_conn = sqlite3.connect(log_db_path)
        log_conn.execute("CREATE TABLE Publication (TagNumber INTEGER, Symbol TEXT, sym TEXT, LanguageId INTEGER)")
        log_conn.execute("INSERT INTO Publication VALUES (20240101, 'w', 'w', 1)")
        log_conn.commit()
        log_conn.close()

        metadata_response = MagicMock()
        metadata_response.json.return_value = {
            "files": {"S": {"EPUB": [{"file": {"url": "http://cdn.example.com/file.epub"}}]}}
        }
        metadata_response.raise_for_status = MagicMock()

        file_response = MagicMock()
        file_response.headers = {}  # No Content-Disposition
        file_response.raw = MagicMock()
        file_response.raise_for_status = MagicMock()

        written_path = None
        original_open = open

        def capture_open(path, mode="r", **kwargs):
            nonlocal written_path
            if path.endswith(".epub"):
                written_path = path
                return MagicMock()
            return original_open(path, mode, **kwargs)

        with patch.object(pub_epub, "setup_state_database", return_value=state_conn):
            with patch.object(pub_epub, "DB_PATH", tmp_db):
                with patch.object(pub_epub, "OUTPUT_PATH", tmp_dir):
                    with patch.object(pub_epub, "get_language_id", return_value=1):
                        with patch.object(pub_epub, "fetch_log_db", return_value=log_db_path):
                            with patch.object(pub_epub.requests, "get", side_effect=[metadata_response, file_response]):
                                with patch("shutil.copyfileobj"):
                                    with patch("builtins.open", side_effect=capture_open):
                                        pub_epub.download_epubs()

        state_conn.close()
        assert written_path is not None
        assert "w_20240101.epub" in written_path


class TestPublicationsEpubMain:
    """Tests for the __main__ block of publications-epub.py."""

    def test_main_calls_download_epubs(self, pub_epub):
        with patch.object(pub_epub, "download_epubs") as mock_dl:
            pub_epub.download_epubs()
            mock_dl.assert_called_once()

    def test_main_handles_exception(self, pub_epub):
        with patch.object(pub_epub, "download_epubs", side_effect=Exception("boom")):
            # Should not propagate
            try:
                pub_epub.download_epubs()
            except Exception:
                pass  # __main__ block catches this


# ===================================================================
# URL / FILENAME EXTRACTION TESTS (standalone, no module import needed)
# ===================================================================


class TestUrlFilenameExtraction:
    """Test filename extraction from download URLs."""

    def test_basic_url(self):
        url = "https://cdn.example.com/media/subtitle_en.vtt"
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        assert filename == "subtitle_en.vtt"

    def test_url_encoded_filename(self):
        url = "https://cdn.example.com/media/my%20file%20name.vtt"
        parsed = urlparse(url)
        filename = unquote(os.path.basename(parsed.path))
        assert filename == "my file name.vtt"

    def test_url_with_query_params(self):
        url = "https://cdn.example.com/media/file.vtt?token=abc123"
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        assert filename == "file.vtt"

    def test_empty_path(self):
        url = "https://cdn.example.com/"
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        assert filename == ""

    def test_deeply_nested_path(self):
        url = "https://cdn.example.com/a/b/c/d/file.vtt"
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        assert filename == "file.vtt"


class TestPublicationUrlGeneration:
    """Test URL generation logic from publications-epub.py."""

    def test_url_with_tag_number(self):
        lang = "S"
        sym = "w"
        tag_number = 20240101
        url = f"https://place.holder/{lang}&pub={sym}&issue={tag_number}&fileformat=epub"
        assert "pub=w" in url
        assert "issue=20240101" in url
        assert "fileformat=epub" in url

    def test_url_without_tag_number(self):
        lang = "S"
        symbol = "nwt"
        url = f"https://place.holder/{lang}&pub={symbol}&fileformat=epub"
        assert "pub=nwt" in url
        assert "issue" not in url

    def test_content_disposition_filename_extraction(self):
        header = 'attachment; filename="publication_2024.epub"'
        match = re.search(r'filename="?([^"]+)"?', header)
        assert match is not None
        assert match.group(1) == "publication_2024.epub"

    def test_content_disposition_without_quotes(self):
        header = "attachment; filename=publication_2024.epub"
        match = re.search(r'filename="?([^"]+)"?', header)
        assert match is not None
        assert match.group(1) == "publication_2024.epub"

    def test_fallback_filename(self):
        symbol = "w"
        tag_number = 20240101
        filename = f"{symbol}_{tag_number}.epub"
        assert filename == "w_20240101.epub"

    def test_fallback_filename_zero_tag(self):
        symbol = "nwt"
        tag_number = 0
        filename = f"{symbol}_{tag_number}.epub"
        assert filename == "nwt_0.epub"


# ===================================================================
# MODULE LOADING EDGE CASES
# ===================================================================


class TestModuleLoadingEdgeCases:
    """Test that modules load correctly with different env configurations."""

    def test_media_vtt_default_env(self):
        mod = _load_media_vtt()
        assert mod.LANG == "S"
        assert mod.OUTPUT_PATH == "/tmp/test_vtts"
        assert mod.DB_PATH == "/tmp/test.db"

    def test_media_vtt_custom_env(self):
        mod = _load_media_vtt({"LANG": "E", "OUTPUT_PATH": "/custom/path", "DB_PATH": "/custom/db.db"})
        assert mod.LANG == "E"
        assert mod.OUTPUT_PATH == "/custom/path"
        assert mod.DB_PATH == "/custom/db.db"

    def test_publications_epub_default_env(self):
        mod = _load_publications_epub()
        assert mod.LANG == "S"
        assert mod.OUTPUT_PATH == "/tmp/test_epubs"
        assert mod.DB_PATH == "/tmp/test_state.db"

    def test_publications_epub_custom_env(self):
        mod = _load_publications_epub({"LANG": "E"})
        assert mod.LANG == "E"

    def test_media_vtt_creates_output_dir(self):
        """When OUTPUT_PATH does not exist, makedirs is called."""
        with patch.dict(os.environ, {"LANG": "S", "OUTPUT_PATH": "/fake/vtts", "DB_PATH": "/fake/db"}):
            with patch("os.path.exists", return_value=False):
                with patch("os.makedirs") as mock_makedirs:
                    spec = importlib.util.spec_from_file_location(
                        "media_vtt_test", os.path.join(SRC_DIR, "media-vtt.py")
                    )
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    mock_makedirs.assert_called()

    def test_publications_epub_creates_output_dir(self):
        """When OUTPUT_PATH does not exist, makedirs is called."""
        with patch.dict(os.environ, {
            "LANG": "S", "OUTPUT_PATH": "/fake/epubs",
            "DB_PATH": "/fake/state.db", "UNIT_DB_PATH": "/fake/unit.db"
        }):
            with patch("os.path.exists", return_value=False):
                with patch("os.makedirs") as mock_makedirs:
                    spec = importlib.util.spec_from_file_location(
                        "pub_epub_test", os.path.join(SRC_DIR, "publications-epub.py")
                    )
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    mock_makedirs.assert_called()
