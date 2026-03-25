from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.config import get_settings
from uni_tracker.models import CollectorRun, SourceAccount


def get_health_snapshot(session: Session) -> dict:
    settings = get_settings()
    from uni_tracker.services.sync import COLLECTOR_REGISTRY

    source = session.scalar(select(SourceAccount).where(SourceAccount.label == "default"))
    runs = session.scalars(
        select(CollectorRun).order_by(CollectorRun.started_at.desc()).limit(10)
    ).all()
    stale_cutoff = datetime.now(UTC) - timedelta(hours=settings.stale_sync_threshold_hours)
    seen_collectors = {run.collector_name for run in runs}
    stale_collectors = sorted(
        {
            run.collector_name
            for run in runs
            if run.status != "completed" or (run.finished_at and run.finished_at < stale_cutoff)
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
    }
