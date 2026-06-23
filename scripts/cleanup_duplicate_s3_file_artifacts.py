from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass

from sqlalchemy import text

from uni_tracker.config import get_settings
from uni_tracker.db import SessionLocal
from uni_tracker.models import RawArtifact
from uni_tracker.services.storage import build_artifact_store, parse_s3_storage_path


@dataclass(frozen=True)
class DuplicatePlan:
    canonical_id: int
    duplicate_ids: list[int]
    duplicate_bytes: int


def build_plans(session, *, limit_groups: int | None = None) -> list[DuplicatePlan]:
    rows = (
        session.query(RawArtifact)
        .filter(
            RawArtifact.storage_backend == "s3",
            RawArtifact.artifact_type == "file",
            RawArtifact.source_url.isnot(None),
        )
        .order_by(RawArtifact.source_url, RawArtifact.content_hash, RawArtifact.id)
        .all()
    )
    groups: dict[tuple[str, str], list[RawArtifact]] = defaultdict(list)
    for row in rows:
        groups[(row.source_url or "", row.content_hash)].append(row)

    plans: list[DuplicatePlan] = []
    for group_rows in groups.values():
        if len(group_rows) <= 1:
            continue
        canonical = group_rows[0]
        duplicates = group_rows[1:]
        plans.append(
            DuplicatePlan(
                canonical_id=canonical.id,
                duplicate_ids=[row.id for row in duplicates],
                duplicate_bytes=sum(row.size_bytes for row in duplicates),
            )
        )
        if limit_groups is not None and len(plans) >= limit_groups:
            break
    return plans


def main() -> int:
    parser = argparse.ArgumentParser(description="Deduplicate repeated S3 file artifacts by source_url+content_hash.")
    parser.add_argument("--execute", action="store_true", help="Actually repoint DB refs, delete duplicate S3 objects, and delete duplicate raw_artifacts rows.")
    parser.add_argument("--limit-groups", type=int, default=None, help="Process at most this many duplicate groups.")
    parser.add_argument("--skip-s3-delete", action="store_true", help="For emergency DB-only repair; normally leave false.")
    args = parser.parse_args()

    settings = get_settings()
    store = build_artifact_store(settings)
    s3 = store.s3_client
    if args.execute and not args.skip_s3_delete and s3 is None:
        raise SystemExit("S3 client is required for live cleanup")

    with SessionLocal() as session:
        plans = build_plans(session, limit_groups=args.limit_groups)
        duplicate_ids = [artifact_id for plan in plans for artifact_id in plan.duplicate_ids]
        duplicate_bytes = sum(plan.duplicate_bytes for plan in plans)
        print(f"duplicate_groups={len(plans)} duplicate_rows={len(duplicate_ids)} duplicate_bytes={duplicate_bytes}")
        if not args.execute:
            print("dry_run=true")
            return 0

        deleted_objects = 0
        for plan in plans:
            duplicate_rows = session.query(RawArtifact).filter(RawArtifact.id.in_(plan.duplicate_ids)).all()
            if not duplicate_rows:
                continue
            # Repoint every FK to the canonical artifact before deleting duplicate DB rows.
            for table, column in [
                ("item_versions", "source_artifact_id"),
                ("item_facts", "source_artifact_id"),
                ("item_briefs", "source_artifact_id"),
                ("llm_jobs", "raw_artifact_id"),
            ]:
                session.execute(
                    text(f"update {table} set {column} = :canonical where {column} = any(:dups)"),
                    {"canonical": plan.canonical_id, "dups": plan.duplicate_ids},
                )

            for row in duplicate_rows:
                if not args.skip_s3_delete:
                    bucket, key = parse_s3_storage_path(row.storage_path)
                    bucket = row.storage_bucket or bucket
                    key = row.storage_key or key
                    if not bucket or not key:
                        raise RuntimeError(f"artifact {row.id} has no S3 bucket/key")
                    s3.delete_object(Bucket=bucket, Key=key)
                    deleted_objects += 1
                session.delete(row)
            session.commit()
        print(f"deleted_duplicate_rows={len(duplicate_ids)} deleted_s3_objects={deleted_objects} freed_bytes={duplicate_bytes}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
