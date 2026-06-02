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


def test_health_snapshot_queries_latest_run_even_when_collector_not_in_recent_10() -> None:
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

    # This collector is healthy but outside the 10 most recent rows. The health
    # snapshot must still find it, otherwise the daily horizon reports bogus
    # stale collector warnings after busy sync windows.
    session.add(
        CollectorRun(
            collector_name="moodle_contents",
            source_account_id=account.id,
            status="completed",
            started_at=now - timedelta(minutes=30),
            finished_at=now - timedelta(minutes=29),
        )
    )
    for idx in range(10):
        session.add(
            CollectorRun(
                collector_name="moodle_files",
                source_account_id=account.id,
                status="completed",
                started_at=now - timedelta(minutes=idx),
                finished_at=now - timedelta(minutes=idx),
            )
        )
    session.commit()

    snapshot = get_health_snapshot(session)

    assert "moodle_contents" not in snapshot["stale_collectors"]


def test_health_snapshot_does_not_mark_recent_running_collector_stale() -> None:
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
        auth_health="running",
        metadata_json={},
    )
    session.add(account)
    session.flush()
    now = datetime.now(UTC)
    session.add(
        CollectorRun(
            collector_name="moodle_forums",
            source_account_id=account.id,
            status="running",
            started_at=now - timedelta(minutes=5),
            finished_at=None,
        )
    )
    session.commit()

    snapshot = get_health_snapshot(session)

    assert "moodle_forums" not in snapshot["stale_collectors"]
