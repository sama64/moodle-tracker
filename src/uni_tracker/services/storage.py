from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True)
class StoredArtifact:
    storage_path: str
    content_hash: str
    size_bytes: int
    storage_backend: str = "local"
    storage_bucket: str | None = None
    storage_key: str | None = None

    def __iter__(self):
        # Backwards-compatible tuple unpacking for existing collectors.
        yield self.storage_path
        yield self.content_hash
        yield self.size_bytes


class ArtifactStore:
    def __init__(
        self,
        root: Path,
        *,
        backend: str = "local",
        s3_client: Any | None = None,
        s3_bucket: str | None = None,
        s3_key_prefix: str = "",
    ) -> None:
        self.root = root
        self.backend = (backend or "local").lower()
        self.s3_client = s3_client
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = _normalize_key_part(s3_key_prefix)
        self.root.mkdir(parents=True, exist_ok=True)
        if self.backend == "s3" and not self.s3_bucket:
            raise ValueError("s3_bucket is required for s3 artifact storage")

    def write_json(self, relative_dir: str, stem: str, payload: Any) -> StoredArtifact:
        content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        return self.write_text(relative_dir, stem, content, suffix=".json", content_type="application/json")

    def write_text(
        self,
        relative_dir: str,
        stem: str,
        content: str,
        *,
        suffix: str = ".txt",
        content_type: str = "text/plain; charset=utf-8",
    ) -> StoredArtifact:
        return self._write_content(
            relative_dir,
            stem,
            content.encode("utf-8"),
            suffix=suffix,
            content_type=content_type,
        )

    def write_bytes(
        self,
        relative_dir: str,
        stem: str,
        content: bytes,
        *,
        suffix: str,
        content_type: str = "application/octet-stream",
    ) -> StoredArtifact:
        return self._write_content(relative_dir, stem, content, suffix=suffix, content_type=content_type)

    def read_text(
        self,
        storage_path: str,
        *,
        backend: str | None = None,
        bucket: str | None = None,
        key: str | None = None,
    ) -> str | None:
        effective_backend = (backend or _backend_from_storage_path(storage_path) or self.backend).lower()
        if effective_backend == "s3":
            bucket, key = _resolve_s3_location(storage_path, bucket=bucket, key=key)
            if not bucket or not key or self.s3_client is None:
                return None
            try:
                body = self.s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
            except Exception:
                return None
            return body.decode("utf-8", errors="replace")
        artifact_path = self._local_path(storage_path)
        if artifact_path is None or not artifact_path.is_file():
            return None
        return artifact_path.read_text(encoding="utf-8", errors="replace")

    def exists(
        self,
        storage_path: str,
        *,
        backend: str | None = None,
        bucket: str | None = None,
        key: str | None = None,
    ) -> bool:
        effective_backend = (backend or _backend_from_storage_path(storage_path) or self.backend).lower()
        if effective_backend == "s3":
            bucket, key = _resolve_s3_location(storage_path, bucket=bucket, key=key)
            if not bucket or not key or self.s3_client is None:
                return False
            try:
                self.s3_client.head_object(Bucket=bucket, Key=key)
            except Exception:
                return False
            return True
        artifact_path = self._local_path(storage_path)
        return bool(artifact_path and artifact_path.is_file())

    def _write_content(self, relative_dir: str, stem: str, content: bytes, *, suffix: str, content_type: str) -> StoredArtifact:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        relative_path = Path(relative_dir) / f"{timestamp}-{stem}{suffix}"
        content_hash = hashlib.sha256(content).hexdigest()
        if self.backend == "s3":
            key = _join_s3_key(self.s3_key_prefix, relative_path.as_posix())
            if self.s3_client is None:
                raise ValueError("s3_client is required for s3 artifact storage")
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
                Metadata={"sha256": content_hash},
            )
            return StoredArtifact(
                storage_path=f"s3://{self.s3_bucket}/{key}",
                content_hash=content_hash,
                size_bytes=len(content),
                storage_backend="s3",
                storage_bucket=self.s3_bucket,
                storage_key=key,
            )
        output_path = self.root / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(content)
        return StoredArtifact(str(relative_path), content_hash, len(content), "local")

    def _local_path(self, storage_path: str) -> Path | None:
        artifact_path = (self.root / storage_path).resolve()
        try:
            artifact_path.relative_to(self.root.resolve())
        except ValueError:
            return None
        return artifact_path


def build_artifact_store(settings: Any, *, s3_client: Any | None = None) -> ArtifactStore:
    backend = (getattr(settings, "artifact_storage_backend", "local") or "local").lower()
    if backend == "s3" and s3_client is None:
        import boto3

        s3_client = boto3.client(
            "s3",
            endpoint_url=getattr(settings, "s3_endpoint_url", None),
            region_name=getattr(settings, "s3_region", "auto"),
            aws_access_key_id=getattr(settings, "s3_access_key_id", None),
            aws_secret_access_key=getattr(settings, "s3_secret_access_key", None),
        )
    return ArtifactStore(
        getattr(settings, "raw_storage_path"),
        backend=backend,
        s3_client=s3_client,
        s3_bucket=getattr(settings, "s3_bucket", None),
        s3_key_prefix=getattr(settings, "s3_key_prefix", ""),
    )


def parse_s3_storage_path(storage_path: str) -> tuple[str | None, str | None]:
    parsed = urlparse(storage_path)
    if parsed.scheme != "s3":
        return None, None
    return parsed.netloc or None, parsed.path.lstrip("/") or None


def _backend_from_storage_path(storage_path: str) -> str | None:
    return "s3" if storage_path.startswith("s3://") else None


def _resolve_s3_location(storage_path: str, *, bucket: str | None, key: str | None) -> tuple[str | None, str | None]:
    if bucket and key:
        return bucket, key
    parsed_bucket, parsed_key = parse_s3_storage_path(storage_path)
    return bucket or parsed_bucket, key or parsed_key


def _join_s3_key(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part and part.strip("/"))


def _normalize_key_part(value: str) -> str:
    return value.strip("/")
