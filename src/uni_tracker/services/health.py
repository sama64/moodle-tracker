from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.config import get_settings
from uni_tracker.models import CollectorRun, SourceAccount


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def get_health_snapshot(session: Session) -> dict:
    settings = get_settings()
    from uni_tracker.services.sync import COLLECTOR_REGISTRY

    source = session.scalar(select(SourceAccount).where(SourceAccount.label == "default"))
    runs = session.scalars(
        select(CollectorRun).order_by(CollectorRun.started_at.desc()).limit(10)
    ).all()
    stale_cutoff = datetime.now(UTC) - timedelta(hours=settings.stale_sync_threshold_hours)
    latest_by_collector = {}
    for run in runs:
        latest_by_collector.setdefault(run.collector_name, run)
    seen_collectors = set(latest_by_collector)
    stale_collectors = sorted(
        {
            run.collector_name
            for run in latest_by_collector.values()
            if run.status != "completed" or (run.finished_at and _as_utc(run.finished_at) < stale_cutoff)
        }
        | (set(COLLECTOR_REGISTRY) - seen_collectors)
    )
    return {
        "source_auth_health": source.auth_health if source else "missing",
        "recent_runs": [
            {
                "collector_name": run.collector_name,
                "status": run.status,
                "started_at": run.started_at.isoformat(),
                "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            }
            for run in runs
        ],
        "stale_collectors": stale_collectors,
        "artifact_storage": _artifact_storage_snapshot(settings),
    }


def _artifact_storage_snapshot(settings) -> dict:
    backend = (getattr(settings, "artifact_storage_backend", "local") or "local").lower()
    if backend == "s3":
        missing = [
            name
            for name in ("s3_endpoint_url", "s3_bucket", "s3_access_key_id", "s3_secret_access_key")
            if not getattr(settings, name, None)
        ]
        return {
            "backend": "s3",
            "bucket": getattr(settings, "s3_bucket", None),
            "prefix": getattr(settings, "s3_key_prefix", ""),
            "configured": not missing,
            "missing": missing,
        }
    return {"backend": "local", "configured": True, "path": str(settings.raw_storage_path)}
