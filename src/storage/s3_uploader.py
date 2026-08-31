# Move local raw file into our S3 raw data lake and verify the upload

import os
from pathlib import Path
from datetime import date
from typing import Any, Dict, List, Optional
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# 💡 Load .env variables into os.environ
load_dotenv()


class S3Uploader:

    # constructor __init__ (Boto3 Session)
    def __init__(self, bucket_name: Optional[str] = None, region_name: Optional[str] = None,):
        # 1. Read configuration from .env 
        self.bucket_name = bucket_name or os.getenv(
            "S3_BUCKET", "spotify-music-intelligence-luc"
        )
        self.region_name = (
            region_name
            or os.getenv("AWS_REGION")
            or os.getenv("AWS_DEFAULT_REGION")
            or "ap-southeast-2"
        )

        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if not self.aws_access_key or not self.aws_secret_key:
            raise ValueError("Missing AWS credentitals in .env")

        # 2. Establish Boto3 Session & S3 Client
        self.session = boto3.Session(
            aws_access_key_id = self.aws_access_key,
            aws_secret_access_key = self.aws_secret_key,
            region_name = self.region_name,
        )

        self.s3_client = self.session.client("s3")

    # bucket Validation
    def validate_bucket(self) -> bool:
        """Verifies target bucket exists and credentials have access."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404":
                raise RuntimeError(
                    f"S3 Bucket '{self.bucket_name}' does not exist!"
                ) from e
            if error_code == "403":
                raise RuntimeError(
                    f"Access Denied to bucket '{self.bucket_name}'!"
                )
            raise RuntimeError(
                f"Failed to reach bucket '{self.bucket_name}': {e} "
            ) from e

    # deterministic S3 Key 
    def build_s3_key(self, entity_type: str, filename: str, snapshot_date: str) -> str:
        """Constructs deterministic Hive-style S3 object key."""
        return f"raw/extracted_at={snapshot_date}/{entity_type}/{filename}"

    # upload with extraArgs
    def upload_file(self, local_path: str, s3_key: str, snapshot_date: str, source: str = "spotify-web-api",) -> Dict[str, Any]:
        local_size_bytes = os.path.getsize(local_path)
        # Attach ContentType, Encryption & Metadata
        extra_args = {
            "ContentType": "application/json",
            "ServerSideEncryption": "AES256",
            "Metadata": {
              "snapshot-date": str(snapshot_date),
              "source": str(source),
              "local-size-bytes": str(local_size_bytes),
            },
        }
        self.s3_client.upload_file(
          Filename=local_path,
          Bucket=self.bucket_name,
          Key=s3_key,
          ExtraArgs=extra_args,
        )
        return {"s3_key": s3_key, "size_bytes": local_size_bytes}

    # verify 
    def verify_object(self, s3_key: str, expected_local_path: str) -> Dict[str, Any]:
        local_size = os.path.getsize(expected_local_path)

        # Query S3 object headers without downloading payload
        head_resp = self.s3_client.head_object(
            Bucket=self.bucket_name, Key=s3_key
        )

        s3_content_length = head_resp.get("ContentLength", 0)
        s3_encryption = head_resp.get("ServerSideEncryption")
        s3_etag = head_resp.get("ETag", "").strip('"')
      
        # Basic integrity check: local bytes == S3 ContentLength
        if local_size != s3_content_length:
            raise ValueError(
                f"Integrity Mismatch for {s3_key}! "
                f"Local: {local_size} B != S3: {s3_content_length} B"
        )
        return {
          "s3_key": s3_key,
          "size_matches": True,
          "s3_content_length": s3_content_length,
          "encryption": s3_encryption,
          "etag": s3_etag,
        }

    def upload_snapshot_datasets(
        self,
        local_raw_base: str = "data/raw",
        snapshot_date: str = "2026-08-30",
    ) -> List[Dict[str, Any]]:

      """Uploads and verifies all 3 raw datasets for the snapshot date."""
      # 1. Validate bucket first
      self.validate_bucket()

      entities = ["artists", "albums", "tracks"]
      results = []

      print("=" * 70)
      print("☁️  S3 CLOUD LAKEHOUSE INGESTION ENGINE")
      print(f"📦 Target Bucket : s3://{self.bucket_name}/")
      print(f"📅 Snapshot Date : {snapshot_date}")
      print("=" * 70)

      for entity in entities:
        filename = f"{entity}_{snapshot_date}.json"
        local_path = os.path.join(local_raw_base, entity, filename)

        if not os.path.exists(local_path):
          print(f"⚠️  Skipping '{entity}': {local_path} not found.")
          continue

        s3_key = self.build_s3_key(entity, filename, snapshot_date)
        print(f"\n📤 Uploading '{entity}' dataset...")
        print(f"   • Local Source : {local_path}")
        print(f"   • S3 Target Key: {s3_key}")

        # Upload
        self.upload_file(local_path, s3_key, snapshot_date)

        # Verify
        verification = self.verify_object(s3_key, local_path)
        print(
            f"   ✓ Verified in S3! ContentLength:"
            f" {verification['s3_content_length']} B | Encryption:"
            f" {verification['encryption']} | ETag: {verification['etag'][:8]}..."
        )

        results.append(
            {"entity": entity, "s3_key": s3_key, "verification": verification}
        )

      print("\n" + "=" * 70)
      print(
          f"🏁 S3 Ingestion Complete — {len(results)}/{len(entities)} datasets"
          " uploaded & verified!"
      )
      print("=" * 70)
      return results


if __name__ == "__main__":
  import sys

  uploader = S3Uploader()
  target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-08-30"
  uploader.upload_snapshot_datasets(snapshot_date=target_date)
