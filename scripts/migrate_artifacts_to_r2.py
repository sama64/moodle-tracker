from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Iterable

from sqlalchemy import select

from uni_tracker.config import get_settings
from uni_tracker.db import SessionLocal
from uni_tracker.models import RawArtifact
from uni_tracker.services.storage import _join_s3_key, build_artifact_store


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate local raw_artifacts files to Cloudflare R2/S3.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would migrate without uploading or changing DB rows.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of artifacts to process.")
    parser.add_argument("--verify-only", action="store_true", help="Only verify already-migrated S3 artifacts.")
    parser.add_argument(
        "--delete-local-after-verify",
        action="store_true",
        help="Delete old local files only after their migrated S3 object verifies.",
    )
    parser.add_argument("--batch-size", type=int, default=100, help="Rows to commit per batch.")
    args = parser.parse_args()

    settings = get_settings()
    if (settings.artifact_storage_backend or "local").lower() != "s3":
        if args.verify_only:
            # Verification is explicitly about S3 rows; build an S3-capable
            # store even if the live app is currently configured for local
            # artifacts.
            settings.artifact_storage_backend = "s3"
        else:
            raise SystemExit("Set ARTIFACT_STORAGE_BACKEND=s3 and R2/S3 credentials before migrating.")
    if not settings.s3_bucket:
        raise SystemExit("S3_BUCKET is required.")

    store = build_artifact_store(settings)
    counters = {"seen": 0, "uploaded": 0, "verified": 0, "deleted": 0, "missing_local": 0, "failed": 0}
    with SessionLocal() as session:
        last_id = 0
        while True:
            query = select(RawArtifact).where(RawArtifact.id > last_id).order_by(RawArtifact.id.asc()).limit(args.batch_size)
            if args.verify_only or args.delete_local_after_verify:
                query = query.where(RawArtifact.storage_backend == "s3")
            else:
                query = query.where(RawArtifact.storage_backend == "local")
            if args.limit:
                remaining = args.limit - counters["seen"]
                if remaining <= 0:
                    break
                query = query.limit(min(args.batch_size, remaining))
            artifacts = session.scalars(query).all()
            if not artifacts:
                break
            for artifact in artifacts:
                last_id = artifact.id
                counters["seen"] += 1
                try:
                    if args.verify_only or args.delete_local_after_verify:
                        if _verify_remote(store, artifact):
                            counters["verified"] += 1
                            if args.delete_local_after_verify and _delete_old_local(settings.raw_storage_path, artifact):
                                counters["deleted"] += 1
                        else:
                            counters["failed"] += 1
                        continue

                    local_path = _safe_local_path(settings.raw_storage_path, artifact.storage_path)
                    if not local_path or not local_path.is_file():
                        counters["missing_local"] += 1
                        continue
                    digest = _sha256_file(local_path)
                    if digest != artifact.content_hash:
                        counters["failed"] += 1
                        print(f"hash-mismatch artifact_id={artifact.id} path={artifact.storage_path}")
                        continue
                    key = _join_s3_key(settings.s3_key_prefix, "legacy", artifact.storage_path)
                    if args.dry_run:
                        print(f"would-upload artifact_id={artifact.id} {local_path} -> s3://{settings.s3_bucket}/{key}")
                        continue
                    with local_path.open("rb") as body:
                        store.s3_client.put_object(
                            Bucket=settings.s3_bucket,
                            Key=key,
                            Body=body,
                            ContentType=artifact.mime_type or "application/octet-stream",
                            Metadata={"sha256": digest, "raw_artifact_id": str(artifact.id)},
                        )
                    artifact.metadata_json = {**(artifact.metadata_json or {}), "old_local_storage_path": artifact.storage_path}
                    artifact.storage_backend = "s3"
                    artifact.storage_bucket = settings.s3_bucket
                    artifact.storage_key = key
                    artifact.storage_path = f"s3://{settings.s3_bucket}/{key}"
                    counters["uploaded"] += 1
                except Exception as exc:  # noqa: BLE001 - migration should keep going and report failures.
                    counters["failed"] += 1
                    print(f"failed artifact_id={artifact.id}: {exc}")
            if not args.dry_run:
                session.commit()
            session.expunge_all()
    print(" ".join(f"{key}={value}" for key, value in counters.items()))


def _verify_remote(store, artifact: RawArtifact) -> bool:
    if artifact.storage_backend != "s3" or not artifact.storage_bucket or not artifact.storage_key:
        return False
    try:
        head = store.s3_client.head_object(Bucket=artifact.storage_bucket, Key=artifact.storage_key)
    except Exception:
        return False
    if int(head.get("ContentLength") or -1) != artifact.size_bytes:
        return False
    remote_hash = (head.get("Metadata") or {}).get("sha256")
    if remote_hash and artifact.content_hash:
        return remote_hash == artifact.content_hash
    # Rows migrated by this script have an old local rollback path and must
    # carry the sha256 metadata written at upload time before we delete local
    # files. Newer S3 artifacts may not have rollback paths, so size-only is
    # acceptable for existence checks on those rows.
    if (artifact.metadata_json or {}).get("old_local_storage_path"):
        return False
    return True


def _delete_old_local(root: Path, artifact: RawArtifact) -> bool:
    old_path = (artifact.metadata_json or {}).get("old_local_storage_path")
    if not old_path:
        return False
    local_path = _safe_local_path(root, old_path)
    if not local_path or not local_path.exists():
        return False
    local_path.unlink()
    return True


def _safe_local_path(root: Path, storage_path: str) -> Path | None:
    if storage_path.startswith("s3://"):
        return None
    candidate = (root / storage_path).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        return None
    return candidate


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    main()
