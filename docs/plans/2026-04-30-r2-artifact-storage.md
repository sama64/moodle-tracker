# Cloudflare R2 Artifact Storage Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Move Moodle tracker downloaded PDFs/media and extracted text artifacts out of the VPS filesystem and into Cloudflare R2, while keeping the API, provenance views, and PDF text extraction working.

**Architecture:** Add a pluggable artifact storage layer with `local` and `s3` backends. Cloudflare R2 is S3-compatible, so the tracker writes raw files, JSON payloads, ICS exports, and extracted text to R2 through `boto3`, stores backend/key metadata in Postgres, and reads extracted text back through the same storage service. Keep local storage as the default and provide a migration script to upload existing artifacts before deleting local copies.

**Tech Stack:** Python 3.12, SQLAlchemy/Alembic, boto3 S3 client, Cloudflare R2 S3-compatible endpoint, FastAPI existing routes, pytest.

---

## Current findings

- Live VPS disk is already tight: `/dev/sda1` is **38G total / 30G used / 6.1G free / 84%**.
- Existing artifact directory is **8.6G**: `data/uni-tracker/artifacts/runtime`.
- DB-reported artifact payloads:
  - `file`: 2,561 rows, ~5.8 GiB
  - `json`: 83,187 rows, ~679 MiB
  - `extracted_text`: 1,920 rows, ~61 MiB
  - `ics`: 1,084 rows, ~2.4 MiB
- Main code paths:
  - Storage writer: `src/uni_tracker/services/storage.py`
  - Moodle file downloader: `src/uni_tracker/collectors/moodle.py:766-926`
  - Artifact read for `/items/{id}/content`: `src/uni_tracker/services/tools.py:631-640`
  - DB model: `src/uni_tracker/models.py:106-120`
  - Artifact persistence helper: `src/uni_tracker/services/persistence.py:72-100`

## Required R2 setup

Create one Cloudflare R2 bucket, for example:

```text
Bucket: moodle-tracker-artifacts
Prefix: production/
Endpoint: https://<ACCOUNT_ID>.r2.cloudflarestorage.com
Access key: R2 token with Object Read + Object Write for this bucket only
Secret key: token secret
Public access: disabled
```

Environment variables to add to `.env` / deployment:

```env
ARTIFACT_STORAGE_BACKEND=s3
S3_ENDPOINT_URL=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
S3_BUCKET=moodle-tracker-artifacts
S3_REGION=auto
S3_ACCESS_KEY_ID=<r2-access-key-id>
S3_SECRET_ACCESS_KEY=<r2-secret-access-key>
S3_KEY_PREFIX=production
S3_PRESIGN_TTL_SECONDS=3600
LOCAL_ARTIFACT_CACHE_PATH=data/uni-tracker/artifacts/cache
```

Important: R2 has no egress fees for normal reads, but S3 list/read/write operations are billed. The tracker should avoid list-heavy migration checks and use DB rows as the index.

---

## Task 1: Add config and dependency

**Objective:** Make S3/R2 settings available without changing behavior by default.

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/uni_tracker/config.py`
- Test: `tests/test_config.py` or existing config test file if one exists

**Step 1: Add dependency**

Add to `pyproject.toml` dependencies:

```toml
  "boto3>=1.35,<2.0",
```

**Step 2: Add settings**

In `src/uni_tracker/config.py`, add fields:

```python
    artifact_storage_backend: str = Field(default="local", alias="ARTIFACT_STORAGE_BACKEND")
    s3_endpoint_url: str | None = Field(default=None, alias="S3_ENDPOINT_URL")
    s3_bucket: str | None = Field(default=None, alias="S3_BUCKET")
    s3_region: str = Field(default="auto", alias="S3_REGION")
    s3_access_key_id: str | None = Field(default=None, alias="S3_ACCESS_KEY_ID")
    s3_secret_access_key: str | None = Field(default=None, alias="S3_SECRET_ACCESS_KEY")
    s3_key_prefix: str = Field(default="", alias="S3_KEY_PREFIX")
    s3_presign_ttl_seconds: int = Field(default=3600, alias="S3_PRESIGN_TTL_SECONDS")
    local_artifact_cache_path: Path = Field(default=Path("data/uni-tracker/artifacts/cache"), alias="LOCAL_ARTIFACT_CACHE_PATH")
```

Normalize backend to lowercase and ensure local directories are only created for local/cache paths.

**Step 3: Test**

Run:

```bash
PYTHONPATH=src pytest tests/test_config.py -q
```

Expected: config tests pass; default backend remains `local`.

---

## Task 2: Add backend-aware artifact store

**Objective:** Replace direct filesystem-only writes with a storage interface that supports local and R2.

**Files:**
- Modify: `src/uni_tracker/services/storage.py`
- Test: `tests/test_storage.py`

**Design:**

Create a protocol-like class with these methods:

```python
class ArtifactStore:
    def write_json(self, relative_dir: str, stem: str, payload: Any) -> StoredArtifact: ...
    def write_text(self, relative_dir: str, stem: str, content: str, *, suffix: str = ".txt") -> StoredArtifact: ...
    def write_bytes(self, relative_dir: str, stem: str, content: bytes, *, suffix: str) -> StoredArtifact: ...
    def read_text(self, storage_path: str) -> str | None: ...
    def exists(self, storage_path: str) -> bool: ...
```

Where:

```python
@dataclass(frozen=True)
class StoredArtifact:
    storage_path: str
    content_hash: str
    size_bytes: int
    storage_backend: str
    storage_bucket: str | None = None
    storage_key: str | None = None
```

Local backend returns `storage_backend="local"`, `storage_path=<relative_path>`.

S3 backend:
- Computes content hash before upload.
- Builds object key as `{S3_KEY_PREFIX}/{relative_dir}/{timestamp}-{stem}{suffix}` with duplicate slashes stripped.
- Uploads with `put_object`.
- Returns `storage_path="s3://<bucket>/<key>"`, `storage_backend="s3"`, `storage_bucket=<bucket>`, `storage_key=<key>`.
- Uses `ContentType` from caller when available in a later task; okay to start with `application/octet-stream`.

**Testing approach:**
- Unit-test local writes/read using `tmp_path`.
- Unit-test S3 key construction and `put_object` call using a fake client; do not require real Cloudflare credentials.

Run:

```bash
PYTHONPATH=src pytest tests/test_storage.py -q
```

---

## Task 3: Extend raw artifact schema for remote storage metadata

**Objective:** Store backend/bucket/key explicitly so future code does not parse `storage_path` forever.

**Files:**
- Modify: `src/uni_tracker/models.py`
- Modify: `src/uni_tracker/services/persistence.py`
- Create: `alembic/versions/0007_artifact_storage_backend.py`
- Test: `tests/test_persistence_notifications.py` or new `tests/test_raw_artifacts.py`

**DB columns:**

```python
storage_backend: Mapped[str] = mapped_column(String(50), default="local", server_default="local")
storage_bucket: Mapped[str | None] = mapped_column(String(255), nullable=True)
storage_key: Mapped[str | None] = mapped_column(String(1000), nullable=True)
```

Alembic upgrade:

```python
op.add_column("raw_artifacts", sa.Column("storage_backend", sa.String(length=50), server_default="local", nullable=False))
op.add_column("raw_artifacts", sa.Column("storage_bucket", sa.String(length=255), nullable=True))
op.add_column("raw_artifacts", sa.Column("storage_key", sa.String(length=1000), nullable=True))
```

Alembic downgrade drops those columns.

Update `create_raw_artifact()` signature with optional:

```python
storage_backend: str = "local"
storage_bucket: str | None = None
storage_key: str | None = None
```

Run:

```bash
PYTHONPATH=src pytest tests/test_persistence_notifications.py -q
```

---

## Task 4: Wire collectors to StoredArtifact return values

**Objective:** Ensure every artifact insert records R2 metadata when R2 is enabled.

**Files:**
- Modify: `src/uni_tracker/collectors/moodle.py`
- Modify: any other collector path using `artifact_store.write_*`
- Test: `tests/test_collectors.py`

**Implementation pattern:**

Replace tuple unpacking:

```python
relative_path, content_hash, size_bytes = self.context.artifact_store.write_json(...)
```

With:

```python
stored = self.context.artifact_store.write_json(...)
```

And pass:

```python
storage_path=stored.storage_path,
content_hash=stored.content_hash,
size_bytes=stored.size_bytes,
storage_backend=stored.storage_backend,
storage_bucket=stored.storage_bucket,
storage_key=stored.storage_key,
```

Apply for:
- JSON collector snapshots
- calendar `.ics`
- downloaded PDFs/media `artifact_type="file"`
- extracted text artifacts `artifact_type="extracted_text"`

Run:

```bash
PYTHONPATH=src pytest tests/test_collectors.py -q
```

---

## Task 5: Read extracted text from local or R2 in API views

**Objective:** Keep `/items/{item_id}/content` and schedule-risk fallback working after text files live in R2.

**Files:**
- Modify: `src/uni_tracker/services/tools.py`
- Test: `tests/test_api_routes.py`

**Implementation:**

Change `_read_artifact_text(storage_path: str)` to accept a `RawArtifact`, or add a helper:

```python
def read_artifact_text(artifact: RawArtifact) -> str | None:
    store = build_artifact_store(get_settings())
    return store.read_text(artifact.storage_path, backend=artifact.storage_backend, bucket=artifact.storage_bucket, key=artifact.storage_key)
```

Then update callers at `tools.py:548-552` to pass the artifact object instead of only `storage_path`.

Tests:
- Existing local artifact content test must still pass.
- Add fake S3 store test proving extracted text is read when `storage_backend="s3"`.

Run:

```bash
PYTHONPATH=src pytest tests/test_api_routes.py -q
```

---

## Task 6: Add migration script for existing local artifacts

**Objective:** Upload existing artifact files to R2 safely and update DB rows without data loss.

**Files:**
- Create: `scripts/migrate_artifacts_to_r2.py`
- Test: `tests/test_migrate_artifacts_to_r2.py`

**Behavior:**

CLI flags:

```bash
python scripts/migrate_artifacts_to_r2.py --dry-run
python scripts/migrate_artifacts_to_r2.py --limit 100
python scripts/migrate_artifacts_to_r2.py --verify-only
python scripts/migrate_artifacts_to_r2.py --delete-local-after-verify
```

Rules:
- Select `raw_artifacts where storage_backend='local'`.
- Resolve local file under `settings.raw_storage_path` and reject path traversal.
- Upload object to R2 key: `{S3_KEY_PREFIX}/legacy/{storage_path}`.
- Compute SHA-256 locally and compare to `raw_artifacts.content_hash` before marking migrated.
- After upload, optionally `head_object` and compare size / metadata hash.
- Update row:
  - `storage_backend='s3'`
  - `storage_bucket=settings.s3_bucket`
  - `storage_key=<key>`
  - `storage_path='s3://<bucket>/<key>'`
  - preserve old path in `metadata_json.old_local_storage_path`
- Commit in small batches, e.g. 100 rows.
- Only delete local file if `--delete-local-after-verify` is passed and verification succeeds.

First live run should be:

```bash
docker compose exec worker python /app/scripts/migrate_artifacts_to_r2.py --dry-run
```

Then:

```bash
docker compose exec worker python /app/scripts/migrate_artifacts_to_r2.py --limit 100
```

Only after API checks:

```bash
docker compose exec worker python /app/scripts/migrate_artifacts_to_r2.py --verify-only
```

---

## Task 7: Add operational health checks

**Objective:** Surface R2 configuration problems before the worker silently fails downloads.

**Files:**
- Modify: `src/uni_tracker/services/health.py`
- Modify: `src/uni_tracker/api/routes.py` if health schema needs changes
- Test: `tests/test_health.py`

Add to `/health/details` under `details`:

```json
"artifact_storage": {
  "backend": "s3",
  "bucket": "moodle-tracker-artifacts",
  "configured": true,
  "writable": true,
  "readable": true
}
```

Implementation should avoid expensive listing. Use one tiny probe object under `{prefix}/healthcheck/` or `head_bucket` + optional `put_object` depending on R2 permissions.

Run:

```bash
PYTHONPATH=src pytest tests/test_health.py -q
```

---

## Task 8: Update deployment docs and compose env

**Objective:** Make R2 deployment reproducible.

**Files:**
- Modify: `README.md`
- Modify: `docs/AGENT_ONBOARDING.md`
- Modify: `.env.example` if present
- Modify: `docker-compose.yml` only if env passthrough/default cache path is needed

Add docs for:
- R2 bucket creation
- Required env vars
- Migration command sequence
- Rollback: set `ARTIFACT_STORAGE_BACKEND=local` before migration, or read migrated rows from R2 after migration
- Cleanup: delete local artifacts only after `--verify-only` passes

Run:

```bash
python3 -m compileall -q src scripts
PYTHONPATH=src pytest tests/test_storage.py tests/test_api_routes.py tests/test_health.py -q
```

---

## Task 9: Live rollout sequence

**Objective:** Deploy safely without losing Moodle artifacts.

1. Stop worker only, keep API/db running:

```bash
docker compose stop worker
```

2. Apply code and migration:

```bash
docker compose build api worker
docker compose run --rm api alembic upgrade head
```

3. Add R2 env vars to `.env`.

4. Start API and verify health:

```bash
docker compose up -d api
curl -sS http://localhost:8000/health/details
```

5. Run migration dry-run:

```bash
docker compose run --rm worker python /app/scripts/migrate_artifacts_to_r2.py --dry-run
```

6. Migrate a small batch:

```bash
docker compose run --rm worker python /app/scripts/migrate_artifacts_to_r2.py --limit 100
```

7. Verify `/items/{id}/content` for a known PDF with extracted text.

8. Complete migration:

```bash
docker compose run --rm worker python /app/scripts/migrate_artifacts_to_r2.py
```

9. Verify and delete local migrated files:

```bash
docker compose run --rm worker python /app/scripts/migrate_artifacts_to_r2.py --verify-only
docker compose run --rm worker python /app/scripts/migrate_artifacts_to_r2.py --delete-local-after-verify
```

10. Restart worker and monitor:

```bash
docker compose up -d worker
curl -sS http://localhost:8000/health/details
```

Expected final state:
- Worker `moodle_files` continues to download one file per run.
- New PDFs/media are stored in R2, not under the runtime artifact directory.
- `/items/{id}/content` still returns extracted text.
- Local runtime artifacts shrink from ~8.6G to mostly cache/temp files.

---

## Rollback plan

If R2 writes fail before migration:
- Set `ARTIFACT_STORAGE_BACKEND=local`.
- Restart worker.
- No DB rollback needed if no R2 rows were created.

If R2 reads fail after migration:
- Keep local files until `--delete-local-after-verify` has succeeded.
- Migration preserves `metadata_json.old_local_storage_path`, so a rollback script can restore:
  - `storage_backend='local'`
  - `storage_path=metadata_json.old_local_storage_path`
  - clear `storage_bucket/storage_key`

Do **not** delete local files until API content reads and migration verification both pass. That is the guardrail. University PDFs are annoying enough without us launching them into the void.
