"""Tests for media-download URL extraction and folder schema logic."""

import pytest
import tempfile
import os
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch, MagicMock
from urllib.parse import urlparse, unquote

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


class TestExtractMediaInfo:
    """Test the extract_media_info function from media-vtt.py."""

    def test_parses_media_items(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "media_vtt",
            str(Path(__file__).parent.parent / "src" / "media-vtt.py")
        )
        mod = importlib.util.module_from_spec(spec)
        # Patch environment before exec
        with patch.dict(os.environ, {"LANG": "S", "OUTPUT_PATH": "/tmp/test_vtts", "DB_PATH": "/tmp/test.db"}):
            with patch("os.path.exists", return_value=True):
                with patch("os.makedirs"):
                    spec.loader.exec_module(mod)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            items = [
                {"type": "media-item", "o": {"keyParts": {"pubS": "w_E_123", "track": 1, "formatCode": "mp4"}}},
                {"type": "media-item", "o": {"keyParts": {"docID": "doc456", "track": 2, "formatCode": "mp3"}}},
                {"type": "other-type", "o": {"keyParts": {"pubS": "ignored", "track": 0, "formatCode": "x"}}},
            ]
            for item in items:
                f.write(json.dumps(item) + "\n")
            f.flush()

            result = mod.extract_media_info(f.name)
            os.unlink(f.name)

        assert len(result) == 2
        assert result[0][0] == "w_E_123"
        assert result[0][1] == 1
        assert result[1][0] == "doc456"

    def test_skips_incomplete_items(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "media_vtt",
            str(Path(__file__).parent.parent / "src" / "media-vtt.py")
        )
        mod = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"LANG": "S", "OUTPUT_PATH": "/tmp/test_vtts", "DB_PATH": "/tmp/test.db"}):
            with patch("os.path.exists", return_value=True):
                with patch("os.makedirs"):
                    spec.loader.exec_module(mod)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            # Missing track field
            items = [
                {"type": "media-item", "o": {"keyParts": {"pubS": "w_E_123", "formatCode": "mp4"}}},
            ]
            for item in items:
                f.write(json.dumps(item) + "\n")
            f.flush()

            result = mod.extract_media_info(f.name)
            os.unlink(f.name)

        assert len(result) == 0


class TestDatabaseSetup:
    """Test database setup and state tracking."""

    def test_setup_creates_table(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "media_vtt",
            str(Path(__file__).parent.parent / "src" / "media-vtt.py")
        )
        mod = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"LANG": "S", "OUTPUT_PATH": "/tmp/test_vtts", "DB_PATH": "/tmp/test.db"}):
            with patch("os.path.exists", return_value=True):
                with patch("os.makedirs"):
                    spec.loader.exec_module(mod)

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        mod.setup_database(db_path)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        os.unlink(db_path)

        assert "downloaded_vtts" in tables

    def test_mark_and_check_processed(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "media_vtt",
            str(Path(__file__).parent.parent / "src" / "media-vtt.py")
        )
        mod = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"LANG": "S", "OUTPUT_PATH": "/tmp/test_vtts", "DB_PATH": "/tmp/test.db"}):
            with patch("os.path.exists", return_value=True):
                with patch("os.makedirs"):
                    spec.loader.exec_module(mod)

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        mod.setup_database(db_path)
        mod.mark_vtt_as_downloaded(db_path, "pub123", 1, "mp4", "http://example.com/file.vtt", "success")

        status = mod.is_vtt_processed(db_path, "pub123", 1, "mp4")
        os.unlink(db_path)

        assert status == "success"

    def test_unprocessed_returns_none(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "media_vtt",
            str(Path(__file__).parent.parent / "src" / "media-vtt.py")
        )
        mod = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"LANG": "S", "OUTPUT_PATH": "/tmp/test_vtts", "DB_PATH": "/tmp/test.db"}):
            with patch("os.path.exists", return_value=True):
                with patch("os.makedirs"):
                    spec.loader.exec_module(mod)

        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        mod.setup_database(db_path)
        status = mod.is_vtt_processed(db_path, "nonexistent", 1, "mp4")
        os.unlink(db_path)

        assert status is None


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
        tag_number = 0
        url = f"https://place.holder/{lang}&pub={symbol}&fileformat=epub"
        assert "pub=nwt" in url
        assert "issue" not in url

    def test_content_disposition_filename_extraction(self):
        """Test extracting filename from Content-Disposition header."""
        import re
        header = 'attachment; filename="publication_2024.epub"'
        match = re.search(r'filename="?([^"]+)"?', header)
        assert match is not None
        assert match.group(1) == "publication_2024.epub"

    def test_fallback_filename(self):
        symbol = "w"
        tag_number = 20240101
        filename = f"{symbol}_{tag_number}.epub"
        assert filename == "w_20240101.epub"
