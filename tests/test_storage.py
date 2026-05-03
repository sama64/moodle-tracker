from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

from uni_tracker.models import RawArtifact
from uni_tracker.services.storage import ArtifactStore, build_artifact_store

_MIGRATION_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "migrate_artifacts_to_r2.py"
_SPEC = importlib.util.spec_from_file_location("migrate_artifacts_to_r2", _MIGRATION_SCRIPT)
assert _SPEC and _SPEC.loader
_migration_module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_migration_module)
_verify_remote = _migration_module._verify_remote


class FakeS3Client:
    def __init__(self) -> None:
        self.put_calls: list[dict] = []
        self.objects: dict[tuple[str, str], bytes] = {}
        self.metadata: dict[tuple[str, str], dict[str, str]] = {}

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        key = (kwargs["Bucket"], kwargs["Key"])
        self.objects[key] = kwargs["Body"]
        self.metadata[key] = kwargs.get("Metadata") or {}
        return {"ETag": "fake"}

    def get_object(self, *, Bucket: str, Key: str):
        return {"Body": SimpleNamespace(read=lambda: self.objects[(Bucket, Key)])}

    def head_object(self, *, Bucket: str, Key: str):
        key = (Bucket, Key)
        body = self.objects[key]
        return {"ContentLength": len(body), "Metadata": self.metadata.get(key, {})}


def test_local_artifact_store_writes_and_reads_text(tmp_path):
    store = ArtifactStore(tmp_path)

    stored = store.write_text("moodle/files/module-1", "cronograma", "Semana 1", suffix=".txt")

    assert stored.storage_backend == "local"
    assert stored.storage_bucket is None
    assert stored.storage_key is None
    assert stored.storage_path.endswith("-cronograma.txt")
    assert store.read_text(stored.storage_path) == "Semana 1"


def test_s3_artifact_store_writes_to_r2_compatible_client_and_reads_text(tmp_path):
    client = FakeS3Client()
    store = ArtifactStore(
        tmp_path,
        backend="s3",
        s3_client=client,
        s3_bucket="moodle-tracker-artifacts",
        s3_key_prefix="production",
    )

    stored = store.write_text("moodle/files/module-1", "cronograma", "Semana 1", suffix=".txt")

    assert stored.storage_backend == "s3"
    assert stored.storage_bucket == "moodle-tracker-artifacts"
    assert stored.storage_key is not None
    assert stored.storage_key.startswith("production/moodle/files/module-1/")
    assert stored.storage_path == f"s3://moodle-tracker-artifacts/{stored.storage_key}"
    assert client.put_calls[0]["Bucket"] == "moodle-tracker-artifacts"
    assert client.put_calls[0]["Key"] == stored.storage_key
    assert client.put_calls[0]["Body"] == b"Semana 1"
    assert client.put_calls[0]["Metadata"] == {"sha256": stored.content_hash}
    assert store.read_text(stored.storage_path, backend="s3", bucket=stored.storage_bucket, key=stored.storage_key) == "Semana 1"


def test_build_artifact_store_uses_s3_settings(tmp_path):
    settings = SimpleNamespace(
        raw_storage_path=tmp_path / "runtime",
        artifact_storage_backend="s3",
        s3_endpoint_url="https://account.r2.cloudflarestorage.com",
        s3_bucket="bucket",
        s3_region="auto",
        s3_access_key_id="key",
        s3_secret_access_key="secret",
        s3_key_prefix="production",
    )

    store = build_artifact_store(settings, s3_client=FakeS3Client())

    stored = store.write_bytes("dir", "file", b"content", suffix=".bin")
    assert stored.storage_backend == "s3"
    assert stored.storage_path.startswith("s3://bucket/production/dir/")


def test_migration_verify_remote_requires_hash_for_rows_with_local_rollback_path(tmp_path):
    client = FakeS3Client()
    store = ArtifactStore(tmp_path, backend="s3", s3_client=client, s3_bucket="bucket", s3_key_prefix="production")
    stored = store.write_bytes("legacy", "artifact", b"same-size", suffix=".bin")
    artifact = RawArtifact(
        storage_backend="s3",
        storage_bucket="bucket",
        storage_key=stored.storage_key,
        storage_path=stored.storage_path,
        content_hash=stored.content_hash,
        size_bytes=stored.size_bytes,
        metadata_json={"old_local_storage_path": "legacy/artifact.bin"},
    )

    assert _verify_remote(store, artifact) is True

    client.metadata[("bucket", stored.storage_key)] = {"sha256": "wrong"}
    assert _verify_remote(store, artifact) is False

    client.metadata[("bucket", stored.storage_key)] = {}
    assert _verify_remote(store, artifact) is False


def test_migration_verify_remote_allows_size_only_for_new_s3_rows_without_local_rollback_path(tmp_path):
    client = FakeS3Client()
    store = ArtifactStore(tmp_path, backend="s3", s3_client=client, s3_bucket="bucket", s3_key_prefix="production")
    stored = store.write_bytes("moodle/files", "artifact", b"content", suffix=".bin")
    client.metadata[("bucket", stored.storage_key)] = {}
    artifact = RawArtifact(
        storage_backend="s3",
        storage_bucket="bucket",
        storage_key=stored.storage_key,
        storage_path=stored.storage_path,
        content_hash=stored.content_hash,
        size_bytes=stored.size_bytes,
        metadata_json={},
    )

    assert _verify_remote(store, artifact) is True
