from __future__ import annotations

from sqlalchemy import select

from uni_tracker.collectors.base import CollectorContext
from uni_tracker.collectors.moodle import MoodleCourseCatalogCollector, MoodleCourseContentsCollector
from uni_tracker.config import get_settings
from uni_tracker.db import SessionLocal
from uni_tracker.models import SourceAccount
from uni_tracker.services.storage import ArtifactStore


COLLECTOR_REGISTRY = {
    MoodleCourseCatalogCollector.name: MoodleCourseCatalogCollector,
    MoodleCourseContentsCollector.name: MoodleCourseContentsCollector,
}


def ensure_source_account(session) -> SourceAccount:
    settings = get_settings()
    account = session.scalar(
        select(SourceAccount).where(
            SourceAccount.source_type == "moodle",
            SourceAccount.label == "default",
        )
    )
    if account is None:
        account = SourceAccount(
            source_type="moodle",
            label="default",
            base_url=settings.moodle_base_url,
            auth_mode="token",
            is_active=True,
            auth_health="unknown",
        )
        session.add(account)
        session.commit()
        session.refresh(account)
    return account


def run_collector(collector_name: str) -> dict:
    settings = get_settings()
    collector_cls = COLLECTOR_REGISTRY[collector_name]
    with SessionLocal() as session:
        account = ensure_source_account(session)
        context = CollectorContext(
            session=session,
            settings=settings,
            artifact_store=ArtifactStore(settings.raw_storage_path),
            source_account=account,
        )
        collector = collector_cls(context)
        return collector.run()


def run_all_collectors() -> list[tuple[str, dict]]:
    results = []
    for collector_name in COLLECTOR_REGISTRY:
        results.append((collector_name, run_collector(collector_name)))
    return results
