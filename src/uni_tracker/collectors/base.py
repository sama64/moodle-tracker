from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from uni_tracker.config import Settings
from uni_tracker.models import CollectorRun, SourceAccount
from uni_tracker.services.storage import ArtifactStore


@dataclass
class CollectorContext:
    session: Session
    settings: Settings
    artifact_store: ArtifactStore
    source_account: SourceAccount


class BaseCollector(ABC):
    name: str

    def __init__(self, context: CollectorContext) -> None:
        self.context = context

    def run(self) -> dict[str, Any]:
        run = CollectorRun(
            collector_name=self.name,
            source_account_id=self.context.source_account.id,
            status="running",
        )
        self.context.session.add(run)
        self.context.session.flush()

        try:
            stats = self.collect(run)
            run.status = "completed"
            run.stats = stats
            run.finished_at = datetime.now(UTC)
            self.context.session.commit()
            return {"status": run.status, "stats": stats}
        except Exception as exc:
            self.context.session.rollback()
            failure_run = self.context.session.get(CollectorRun, run.id)
            if failure_run is not None:
                failure_run.status = "failed"
                failure_run.error_text = str(exc)
                failure_run.finished_at = datetime.now(UTC)
                self.context.session.commit()
            raise

    @abstractmethod
    def collect(self, run: CollectorRun) -> dict[str, Any]:
        raise NotImplementedError
