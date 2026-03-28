from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from contextlib import nullcontext

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from uni_tracker.db import Base
from uni_tracker.main import app
from uni_tracker.models import Course, SourceAccount, SourceObject
from uni_tracker.services.persistence import create_raw_artifact, upsert_normalized_item


def test_collectors_endpoint() -> None:
    client = TestClient(app)
    response = client.get("/sync/collectors")
    assert response.status_code == 200
    payload = response.json()
    assert "moodle_courses" in payload
    assert "moodle_files" in payload


def test_item_content_exposes_declared_file_metadata(monkeypatch) -> None:
    session = _make_session()
    _, course, source_object = _seed_resource(session)
    item, _ = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material",
        title="Cronograma de clases",
        body_text=None,
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload=source_object.raw_payload,
    )
    session.commit()

    _install_test_session(monkeypatch, session)
    client = TestClient(app)
    response = client.get(f"/items/{item.id}/content")

    assert response.status_code == 200
    payload = response.json()
    assert payload["item"]["id"] == item.id
    assert payload["artifacts"] == [
        {
            "source_object_id": source_object.id,
            "parent_source_object_id": None,
            "filename": "Cronograma CALCULO I 1er. Cuat. 2026.pdf",
            "filepath": "/",
            "item_id": item.id,
            "item_type": "material",
            "mime_type": "application/pdf",
            "source_url": "https://example.invalid/pluginfile.php/1/cronograma.pdf",
            "file_artifact_id": None,
            "extracted_text_artifact_id": None,
            "storage_path": None,
            "size_bytes": 74784,
            "downloaded": False,
            "extracted_text": None,
            "extraction_status": "not_downloaded",
        }
    ]


def test_item_content_exposes_downloaded_artifact_text(monkeypatch, tmp_path: Path) -> None:
    session = _make_session()
    _, course, source_object = _seed_resource(session)
    resource_item, _ = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material",
        title="Cronograma de clases",
        body_text=None,
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload=source_object.raw_payload,
    )
    session.flush()

    child_source_object = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="module-1:/:Cronograma CALCULO I 1er. Cuat. 2026.pdf",
        object_type="module_file",
        parent_external_id=source_object.external_id,
        source_url="https://example.invalid/pluginfile.php/1/cronograma.pdf",
        current_hash="child-hash",
        raw_payload={
            "filename": "Cronograma CALCULO I 1er. Cuat. 2026.pdf",
            "content": {
                "filename": "Cronograma CALCULO I 1er. Cuat. 2026.pdf",
                "filepath": "/",
                "fileurl": "https://example.invalid/pluginfile.php/1/cronograma.pdf",
                "mimetype": "application/pdf",
                "filesize": 74784,
            },
        },
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(child_source_object)
    session.flush()
    child_item, _ = upsert_normalized_item(
        session,
        source_object_id=child_source_object.id,
        course_id=course.id,
        item_type="material_file",
        title="Cronograma CALCULO I 1er. Cuat. 2026.pdf",
        body_text="Cronograma semana 1",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload=child_source_object.raw_payload,
    )

    artifact_dir = tmp_path / "runtime" / "moodle" / "files" / source_object.external_id
    artifact_dir.mkdir(parents=True)
    pdf_path = artifact_dir / "cronograma.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")
    text_path = artifact_dir / "cronograma-text.txt"
    text_path.write_text("Semana 1\nSemana 2", encoding="utf-8")

    create_raw_artifact(
        session,
        collector_run_id=_seed_collector_run(session, source_object.source_account_id),
        source_object_id=child_source_object.id,
        artifact_type="file",
        mime_type="application/pdf",
        storage_path=str(pdf_path.relative_to(tmp_path / "runtime")),
        content_hash="file-hash",
        size_bytes=8,
        source_url=child_source_object.source_url,
        metadata_json={"filename": child_item.title},
    )
    create_raw_artifact(
        session,
        collector_run_id=_seed_collector_run(session, source_object.source_account_id),
        source_object_id=child_source_object.id,
        artifact_type="extracted_text",
        mime_type="text/plain",
        storage_path=str(text_path.relative_to(tmp_path / "runtime")),
        content_hash="text-hash",
        size_bytes=len(text_path.read_bytes()),
        source_url=child_source_object.source_url,
        metadata_json={"filename": child_item.title},
        extraction_status="completed",
    )
    session.commit()

    _install_test_session(monkeypatch, session)
    monkeypatch.setattr("uni_tracker.services.tools.get_settings", lambda: SimpleNamespace(raw_storage_path=tmp_path / "runtime"))
    client = TestClient(app)
    response = client.get(f"/items/{resource_item.id}/content")

    assert response.status_code == 200
    payload = response.json()
    assert payload["artifacts"][0]["filename"] == child_item.title
    assert payload["artifacts"][0]["item_id"] == child_item.id
    assert payload["artifacts"][0]["item_type"] == "material_file"
    assert payload["artifacts"][0]["downloaded"] is True
    assert payload["artifacts"][0]["extracted_text"] == "Semana 1\nSemana 2"
    assert payload["artifacts"][0]["extraction_status"] == "completed"


def _make_session():
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)()


def _install_test_session(monkeypatch, session) -> None:
    monkeypatch.setattr("uni_tracker.api.routes.SessionLocal", lambda: nullcontext(session))


def _seed_resource(session):
    account = SourceAccount(
        source_type="moodle",
        label="default",
        base_url="https://example.invalid",
        auth_mode="token",
        is_active=True,
        auth_health="healthy",
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
    source_object = SourceObject(
        source_account_id=account.id,
        course_id=course.id,
        external_id="module-1",
        object_type="resource",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/mod/resource/view.php?id=1",
        current_hash="resource-hash",
        raw_payload={
            "contents": [
                {
                    "filename": "Cronograma CALCULO I 1er. Cuat. 2026.pdf",
                    "filepath": "/",
                    "fileurl": "https://example.invalid/pluginfile.php/1/cronograma.pdf",
                    "mimetype": "application/pdf",
                    "filesize": 74784,
                }
            ]
        },
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(source_object)
    session.commit()
    return account, course, source_object


def _seed_collector_run(session, source_account_id: int) -> int:
    from uni_tracker.models import CollectorRun

    run = CollectorRun(
        collector_name="test",
        source_account_id=source_account_id,
        status="success",
    )
    session.add(run)
    session.flush()
    return run.id
