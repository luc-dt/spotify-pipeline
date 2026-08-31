# 📓 Spotify Music Intelligence Platform — Engineering Diary & Implementation Plan

A cumulative engineering diary and architectural implementation plan tracking daily goals, technical decisions, code designs, lessons learned, and definitions of done.

---

# 🗓️ Day 2: Python Extraction Engine (COMPLETED)

### 🎯 Day 2 Objective:
Build a production-grade, modular Python extraction package in `src/extract/` that extracts Spotify catalog metadata for our target cohort and stores immutable raw JSON in `data/raw/`.

---

### 🧩 The 5 Modular Components Built:
```text
src/extract/
├── __init__.py           ──▶ [0] Package Interface & Exports
├── spotify_client.py     ──▶ [1] Token Manager (OAuth 2.0) + 429 Rate-Limit Retry Engine + Circuit Breaker
├── artist_extractor.py   ──▶ [2] Artist Search & Canonical ID Resolver
├── album_extractor.py    ──▶ [3] Discography Paginator (limit=10) + Deduplication
├── track_extractor.py    ──▶ [4] Simplified Track Paginator (limit=10) + Foreign Key Lineage
└── main.py               ──▶ [5] Master Orchestrator (Writes partitioned JSON to data/raw/)
```

---

### 🧠 Core Architectural Decisions & Lessons Learned (Day 2):

1. **OAuth 2.0 Client Credentials & Token Caching**:
   * Spotify access tokens expire in 1 hour (3,600s). Re-authenticating on every request triggers rate limits.
   * `SpotifyClient` caches the token in memory with a 60-second safety buffer (`time.time() + expires_in - 60`). Subsequent calls reuse the token with 0ms auth latency.
2. **Rate Limiting (429) vs. Account Quota Exhaustion**:
   * HTTP 429 sends a `Retry-After` header. For short transient limits, we back off exponentially with random jitter ($0.2\text{s}$–$0.8\text{s}$).
   * In 2026 Development Mode, Spotify enforces an account-level quota shared across all client IDs. If `Retry-After > 60s`, the client acts as a **circuit breaker** and raises a descriptive `RuntimeError`.
3. **Full Track vs. Simplified Track Objects**:
   * Calling `GET /v1/albums/{id}/tracks` returns lightweight, simplified track objects directly in paginated lists for each album, avoiding thousands of redundant individual track queries and saving 90% of API quota.
4. **Target Cohort Design**:
   * Selected 8 diverse superstars across genres and eras (Taylor Swift, The Weeknd, Drake, Ed Sheeran, Billie Eilish, Ariana Grande, Coldplay, BTS) to provide analytical variance (singles vs albums, track durations, release cadences).
5. **Partial Snapshot State Consistency**:
   * Encountered an upstream quota exhaustion halfway through the run (after 5 artists). Learned the importance of atomic batches and snapshot watermarking (`snapshot_date = YYYY-MM-DD`).

---

### 🏁 Day 2 Definition of Done (Verified Results):
* ✅ Extracted **8 Artists, 654 Albums, and 2,459 Tracks** into `data/raw/`:
  * `data/raw/artists/artists_2026-08-30.json` (2.7 KB)
  * `data/raw/albums/albums_2026-08-30.json` (356 KB)
  * `data/raw/tracks/tracks_2026-08-30.json` (1.07 MB)
* ✅ Committed and pushed to GitHub: [`https://github.com/luc-dt/spotify-pipeline.git`](https://github.com/luc-dt/spotify-pipeline.git).

---
---

# 🗓️ Day 3: AWS S3 Raw Data Lake

## 🎯 Day 3 Objective

Persist the immutable raw JSON payloads extracted on Day 2 into an **Amazon S3 Raw Data Lake** using `boto3`, with Hive-style partitioning:

```text
raw/extracted_at=YYYY-MM-DD/
```

The main learning objective is:

> **Move locally extracted raw data into reliable cloud object storage while understanding the fundamentals of S3, boto3, partitioning, metadata, encryption, verification, and idempotent uploads.**

---

# 🏗️ Master Architecture

```text
┌───────────────────────────────┐
│       LOCAL RAW STORAGE       │
│                               │
│ data/raw/artists/             │
│ data/raw/albums/              │
│ data/raw/tracks/              │
└───────────────┬───────────────┘
                │
                │ boto3
                ▼
┌──────────────────────────────────────────┐
│          S3 UPLOAD APPLICATION            │
│        src/storage/s3_uploader.py         │
│                                          │
│ • Validate bucket                         │
│ • Build deterministic S3 keys             │
│ • Upload JSON                             │
│ • Attach metadata                         │
│ • Verify object size/checksum              │
│ • Handle upload errors                    │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│              AMAZON S3                    │
│                                          │
│ s3://spotify-music-intelligence-luc/     │
│                                          │
│ raw/                                     │
│ └── extracted_at=2026-08-30/             │
│     ├── artists/artists.json             │
│     ├── albums/albums.json               │
│     └── tracks/tracks.json               │
└──────────────────────────────────────────┘
```

### Infrastructure vs Application

AWS infrastructure is configured separately:

```text
AWS Infrastructure
├── S3 Bucket
├── Block Public Access
├── IAM permissions
└── Encryption configuration
        │
        ▼
Application
└── s3_uploader.py
    └── uploads and verifies data
```

The uploader should **not automatically create or configure infrastructure**.

---

# 🪜 12-Step Incremental Implementation Sequence

The implementation will be deliberately incremental so that each AWS concept is understood before adding the next one.

### Step 1 — Understand `boto3.Session`

Learn how the AWS SDK establishes a session and obtains credentials from the environment:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
```

Understand the difference between:

```python
boto3.Session()
```

and AWS service clients such as:

```python
session.client("s3")
session.client("sts")
```

---

### Step 2 — Verify AWS Credentials

Create:

```text
scripts/verify_aws_credentials.py
```

Use:

```python
sts.get_caller_identity()
```

to verify that the local environment can authenticate to AWS and determine which IAM identity is being used.

This is a **diagnostic/one-time verification tool**, not something that needs to execute on every pipeline run.

---

### Step 3 — Validate the Existing S3 Bucket

The application assumes that the bucket already exists.

Use:

```python
s3.head_bucket()
```

to verify:

* bucket exists
* credentials have access
* configured bucket is reachable

If the bucket does not exist, fail clearly rather than creating infrastructure automatically.

---

### Step 4 — Upload One Test File

Start with the smallest dataset:

```text
artists_2026-08-30.json
```

Upload it to:

```text
raw/extracted_at=2026-08-30/artists/artists_2026-08-30.json
```

The goal is simply to understand:

```text
local file
    ↓
upload_file()
    ↓
S3 object
```

---

### Step 5 — Verify the S3 Object

Use:

```python
s3.head_object()
```

to inspect:

* `ContentLength`
* `ContentType`
* metadata
* `ETag`
* server-side encryption information

This establishes the basic upload → verification workflow.

---

### Step 6 — Upload the Full Dataset

Once the single-file upload works, upload:

```text
artists_2026-08-30.json
albums_2026-08-30.json
tracks_2026-08-30.json
```

to their corresponding S3 locations.

---

### Step 7 — Implement Hive-Style Partitioning

Use deterministic S3 keys:

```text
raw/extracted_at=YYYY-MM-DD/{entity}/{entity}_{date}.json
```

Example:

```text
raw/
└── extracted_at=2026-08-30/
    ├── artists/artists_2026-08-30.json
    ├── albums/albums_2026-08-30.json
    └── tracks/tracks_2026-08-30.json
```

The `key=value` directory structure is Hive-style partitioning, which Athena supports. Partitioning allows Athena to scan only relevant partitions instead of the entire dataset, improving query performance and reducing scan costs.

---

### Step 8 — Attach Object Metadata

Use S3 upload arguments such as:

```python
ExtraArgs={
    "ContentType": "application/json",
    "Metadata": {
        "snapshot-date": "2026-08-30",
        "source": "spotify-web-api"
    }
}
```

This teaches the difference between:

* object content
* HTTP/content metadata
* custom S3 metadata

---

### Step 9 — Understand and Verify SSE-S3

S3 automatically applies **SSE-S3 using AES-256** to new object uploads by default since January 5, 2023.

Therefore, the goal is **verification**, not pretending that the application is responsible for creating encryption from scratch.

Use `head_object()` to confirm the stored object's server-side encryption information.

---

### Step 10 — Local ↔ S3 Integrity Verification

Compare:

```text
Local file
├── byte size (os.path.getsize())
└── checksum
        │
        ▼
S3 object
├── ContentLength
└── stored checksum / integrity information
```

Compare local file size with S3 `ContentLength` as a basic integrity check.

---

### Step 11 — Test Idempotent Uploads

Run the same upload twice using the same deterministic S3 key:

```text
raw/extracted_at=2026-08-30/artists/artists_2026-08-30.json
```

Expected behavior:

```text
First upload
     ↓
one S3 object

Second upload
     ↓
same S3 key
     ↓
no duplicate object
     ↓
same expected content
```

The important concept is:

> **Same input + same deterministic key → same logical S3 object, without duplicate objects being created.**

---

### Step 12 — Unit Tests with Mocks

Create:

```text
tests/
└── test_s3_uploader.py
```

Use `unittest.mock` to test the application logic without repeatedly calling AWS.

Test cases should cover:

* bucket validation
* S3 key construction
* metadata
* upload behavior
* error handling
* deterministic keys
* verification logic

---

# 🔒 Security & Architecture Principles

### 1. Infrastructure ≠ Application Code

AWS infrastructure is configured separately:

```text
Infrastructure
├── S3 bucket
├── IAM
├── Block Public Access
└── encryption
```

Application code handles:

```text
Application
└── reliable data transfer
```

---

### 2. No Hardcoded Credentials

Never put AWS credentials directly into Python code. Use environment-based credentials locally and IAM-based authentication in cloud execution environments when appropriate.

---

### 3. Hive-Style Partitioning

Use:

```text
raw/extracted_at=2026-08-30/
```

Athena recognizes Hive-style `key=value` paths and can use partition information to reduce the amount of S3 data scanned by queries.

---

### 4. Encryption

S3 automatically encrypts new object uploads using SSE-S3/AES-256 by default. The pipeline should **verify** the encryption status rather than incorrectly presenting AES-256 as a custom security mechanism implemented by `s3_uploader.py`.

---

# 🏁 Day 3 Definition of Done

* [ ] S3 bucket `spotify-music-intelligence-luc` exists and is accessible.
* [ ] `scripts/verify_aws_credentials.py` successfully identifies the AWS caller.
* [ ] `python -m src.storage.s3_uploader` successfully uploads the raw datasets.
* [ ] S3 keys follow:

```text
raw/extracted_at=YYYY-MM-DD/
├── artists/artists_YYYY-MM-DD.json
├── albums/albums_YYYY-MM-DD.json
└── tracks/tracks_YYYY-MM-DD.json
```

* [ ] Local file sizes match S3 `ContentLength` (`os.path.getsize(local) == s3_obj['ContentLength']`).
* [ ] S3 object metadata is present:
  * `snapshot-date`
  * `source`
  * `ContentType=application/json`
* [ ] SSE-S3/AES-256 is confirmed through S3 object metadata/API response.
* [ ] Re-running the upload produces deterministic objects without duplicate keys.
* [ ] Integrity is checked using size and, where appropriate, checksum/ETag.
* [ ] Unit tests use mocks and do not require repeated AWS API calls.
* [ ] Code committed and pushed to GitHub with:

```text
feat: Day 3 - AWS S3 raw lakehouse ingestion engine
```
