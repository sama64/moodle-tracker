from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from uni_tracker.collectors.base import CollectorContext
from uni_tracker.collectors.moodle import (
    COMPLETION_STATE_COMPLETED,
    COMPLETION_STATE_INCOMPLETE,
    COMPLETION_STATE_UNKNOWN,
    MoodleFilesCollector,
    extract_quiz_completion_state,
)
from uni_tracker.db import Base
from uni_tracker.models import Course, SourceAccount, SourceObject
from uni_tracker.services.storage import ArtifactStore


def test_moodle_files_cursor_persists_between_runs(monkeypatch, tmp_path) -> None:
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
    course = Course(
        source_account_id=account.id,
        external_id="101",
        shortname="CALC1",
        fullname="Calculo I",
        display_name="Calculo I",
        course_url="https://example.invalid/course/101",
        visible=True,
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(course)
    session.flush()
    for idx in range(2):
        session.add(
            SourceObject(
                source_account_id=account.id,
                course_id=course.id,
                external_id=f"module-{idx + 1}",
                object_type="resource",
                parent_external_id=course.external_id,
                source_url=f"https://example.invalid/mod/resource/view.php?id={idx + 1}",
                current_hash=f"hash-{idx + 1}",
                raw_payload={
                    "contents": [
                        {
                            "filename": f"archivo-{idx + 1}.pdf",
                            "filepath": "/",
                            "fileurl": f"https://example.invalid/pluginfile.php/{idx + 1}/archivo-{idx + 1}.pdf",
                            "mimetype": "application/pdf",
                            "filesize": 16,
                        }
                    ]
                },
                first_seen_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
            )
        )
    session.commit()
    session.close()

    downloaded_urls: list[str] = []

    class FakeMoodleServiceClient:
        def __init__(self, settings, session=None, source_account=None) -> None:
            self.settings = settings

        def download_file(self, url: str) -> bytes:
            downloaded_urls.append(url)
            return b"%PDF-1.4 fake pdf"

        def close(self) -> None:
            return None

    monkeypatch.setattr("uni_tracker.collectors.moodle.MoodleServiceClient", FakeMoodleServiceClient)
    settings = SimpleNamespace(file_download_limit_per_run=1)

    for _ in range(2):
        run_session = Session()
        account = run_session.get(SourceAccount, 1)
        collector = MoodleFilesCollector(
            CollectorContext(
                session=run_session,
                settings=settings,
                artifact_store=ArtifactStore(tmp_path / "runtime"),
                source_account=account,
            )
        )
        collector.run()
        run_session.close()

    verify_session = Session()
    stored_account = verify_session.get(SourceAccount, 1)
    assert stored_account.metadata_json == {"moodle_files_cursor": 0}
    assert downloaded_urls == [
        "https://example.invalid/pluginfile.php/1/archivo-1.pdf",
        "https://example.invalid/pluginfile.php/2/archivo-2.pdf",
    ]


def test_extract_quiz_completion_state_prefers_finished_attempts() -> None:
    module = {
        "completiondata": {
            "state": 0,
            "hascompletion": True,
            "isoverallcomplete": False,
        }
    }
    attempts_payload = {
        "attempts": [
            {"preview": 0, "state": "finished"},
        ]
    }

    assert extract_quiz_completion_state(module, attempts_payload) == COMPLETION_STATE_COMPLETED


def test_extract_quiz_completion_state_marks_started_quiz_incomplete() -> None:
    module = {
        "completiondata": {
            "state": 0,
            "hascompletion": True,
            "isoverallcomplete": False,
        }
    }
    attempts_payload = {
        "attempts": [
            {"preview": 0, "state": "inprogress"},
        ]
    }

    assert extract_quiz_completion_state(module, attempts_payload) == COMPLETION_STATE_INCOMPLETE


def test_extract_quiz_completion_state_falls_back_to_module_completion() -> None:
    module = {
        "completiondata": {
            "state": 1,
            "hascompletion": True,
            "isoverallcomplete": True,
        }
    }

    assert extract_quiz_completion_state(module, None) == COMPLETION_STATE_COMPLETED
    assert extract_quiz_completion_state({}, None) == COMPLETION_STATE_UNKNOWN
