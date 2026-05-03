from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from uni_tracker.db import Base
from uni_tracker.main import app
from uni_tracker.models import CollectorRun, SourceAccount
from uni_tracker.services.health import get_health_snapshot


def test_health() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_health_snapshot_uses_latest_run_per_collector_for_staleness() -> None:
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    session = Session()
    account = SourceAccount(
        source_type="moodle",
        label="default",
        base_url="https://example.invalid",
        auth_mode="token",
        is_active=True,
        auth_health="healthy",
        metadata_json={},
    )
    session.add(account)
    session.flush()
    now = datetime.now(UTC)
    session.add_all(
        [
            CollectorRun(
                collector_name="moodle_files",
                source_account_id=account.id,
                status="completed",
                started_at=now,
                finished_at=now,
            ),
            CollectorRun(
                collector_name="moodle_files",
                source_account_id=account.id,
                status="failed",
                started_at=now - timedelta(minutes=5),
                finished_at=now - timedelta(minutes=4),
                error_text="interrupted",
            ),
        ]
    )
    session.commit()

    snapshot = get_health_snapshot(session)

    assert "moodle_files" not in snapshot["stale_collectors"]
