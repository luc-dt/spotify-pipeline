"""tests/test_s3_uploader.py
--------------------------
Unit Tests for S3Uploader using unittest.mock.
Tests bucket validation, deterministic key generation, upload mechanics,
ExtraArgs metadata injection, and size verification without calling real AWS APIs.
"""

import os
import sys
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError

# Ensure workspace root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage.s3_uploader import S3Uploader


@pytest.fixture
def mock_env(monkeypatch):
    """Set mock AWS credentials in test environment."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "mock_key_id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "mock_secret_key")
    monkeypatch.setenv("AWS_REGION", "ap-southeast-2")
    monkeypatch.setenv("S3_BUCKET", "spotify-music-intelligence-test")


@pytest.fixture
def uploader(mock_env):
    """Fixture providing an S3Uploader instance with mocked boto3."""
    with patch("boto3.Session") as mock_session:
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        inst = S3Uploader()
        inst.s3_client = mock_client
        return inst


class TestS3Uploader:
    """Test suite for S3Uploader methods."""

    def test_init_missing_credentials(self, monkeypatch):
        """Test constructor raises ValueError if credentials are missing."""
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        with pytest.raises(ValueError, match="Missing AWS"):
            S3Uploader()

    def test_validate_bucket_success(self, uploader):
        """Test bucket validation returns True when head_bucket succeeds."""
        uploader.s3_client.head_bucket.return_value = {}
        assert uploader.validate_bucket() is True
        uploader.s3_client.head_bucket.assert_called_once_with(
            Bucket="spotify-music-intelligence-test"
        )

    def test_validate_bucket_not_found(self, uploader):
        """Test bucket validation raises RuntimeError when bucket does not exist (404)."""
        error_resp = {"Error": {"Code": "404", "Message": "Not Found"}}
        uploader.s3_client.head_bucket.side_effect = ClientError(
            error_resp, "HeadBucket"
        )
        with pytest.raises(RuntimeError, match="does not exist"):
            uploader.validate_bucket()

    def test_validate_bucket_access_denied(self, uploader):
        """Test bucket validation raises RuntimeError on 403 Forbidden."""
        error_resp = {"Error": {"Code": "403", "Message": "Forbidden"}}
        uploader.s3_client.head_bucket.side_effect = ClientError(
            error_resp, "HeadBucket"
        )
        with pytest.raises(RuntimeError, match="Access Denied"):
            uploader.validate_bucket()

    def test_build_s3_key_hive_format(self, uploader):
        """Test deterministic Hive-style S3 object key construction."""
        key = uploader.build_s3_key(
            entity_type="artists",
            filename="artists_2026-08-31.json",
            snapshot_date="2026-08-31",
        )
        assert key == "raw/extracted_at=2026-08-31/artists/artists_2026-08-31.json"

    def test_upload_file_with_extra_args(self, uploader, tmp_path):
        """Test upload_file passes ContentType, AES256, and Metadata in ExtraArgs."""
        test_file = tmp_path / "test_artists.json"
        test_file.write_text('{"artist": "Taylor Swift"}', encoding="utf-8")

        s3_key = "raw/extracted_at=2026-08-31/artists/test_artists.json"
        res = uploader.upload_file(
            local_path=str(test_file),
            s3_key=s3_key,
            snapshot_date="2026-08-31",
            source="spotify-web-api",
        )

        assert res["s3_key"] == s3_key
        uploader.s3_client.upload_file.assert_called_once()
        call_kwargs = uploader.s3_client.upload_file.call_args[1]

        assert call_kwargs["Filename"] == str(test_file)
        assert call_kwargs["Bucket"] == "spotify-music-intelligence-test"
        assert call_kwargs["Key"] == s3_key
        assert call_kwargs["ExtraArgs"]["ContentType"] == "application/json"
        assert call_kwargs["ExtraArgs"]["ServerSideEncryption"] == "AES256"
        assert call_kwargs["ExtraArgs"]["Metadata"]["snapshot-date"] == "2026-08-31"
        assert call_kwargs["ExtraArgs"]["Metadata"]["source"] == "spotify-web-api"

    def test_verify_object_success(self, uploader, tmp_path):
        """Test verify_object succeeds when S3 ContentLength matches local size."""
        test_file = tmp_path / "sample.json"
        content = "test content"
        test_file.write_text(content, encoding="utf-8")
        file_size = len(content.encode("utf-8"))

        uploader.s3_client.head_object.return_value = {
            "ContentLength": file_size,
            "ContentType": "application/json",
            "ServerSideEncryption": "AES256",
            "ETag": '"abc12345"',
            "Metadata": {"snapshot-date": "2026-08-31", "source": "spotify-web-api"},
        }

        ver = uploader.verify_object("test/key.json", str(test_file))
        assert ver["size_matches"] is True
        assert ver["s3_content_length"] == file_size
        assert ver["encryption"] == "AES256"

    def test_verify_object_size_mismatch(self, uploader, tmp_path):
        """Test verify_object raises ValueError when byte sizes differ."""
        test_file = tmp_path / "sample.json"
        test_file.write_text("local content", encoding="utf-8")

        uploader.s3_client.head_object.return_value = {
            "ContentLength": 9999,  # Mismatch
            "ContentType": "application/json",
            "ServerSideEncryption": "AES256",
            "ETag": '"abc12345"',
        }

        with pytest.raises(ValueError, match="Integrity Mismatch"):
            uploader.verify_object("test/key.json", str(test_file))
