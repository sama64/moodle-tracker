from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from uni_tracker.db import Base
from uni_tracker.models import Course, ItemBrief, LLMJob, Notification, SourceAccount, SourceObject
from uni_tracker.services.notifications import (
    dispatch_due_notifications,
    build_digest_message,
    schedule_daily_digest,
    schedule_notifications_for_item,
)
from uni_tracker.services.briefs import get_item_brief, upsert_item_brief
from uni_tracker.services.llm import backfill_item_briefs, enrich_recent_items
from uni_tracker.services.persistence import ItemChange, upsert_normalized_item
from uni_tracker.services.telegram_bot import poll_telegram_commands
from uni_tracker.services.tools import get_item_course_name, get_risk_items


def make_session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)()


def seed_source_graph(session: Session) -> tuple[SourceAccount, Course, SourceObject]:
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
        shortname="TEST101",
        fullname="Curso de prueba",
        display_name="Curso de prueba",
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
        source_url="https://example.invalid/module/1",
        current_hash="hash",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(source_object)
    session.commit()
    return account, course, source_object


def test_upsert_normalized_item_classifies_deadline_change() -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    first_due = datetime.now(UTC) + timedelta(days=4)
    second_due = first_due + timedelta(days=2)

    item, change = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 1",
        body_text="Entrega del trabajo practico",
        published_at=None,
        starts_at=None,
        due_at=first_due,
        primary_url=source_object.source_url,
        raw_payload={},
        facts_payload=[{"fact_type": "due_at", "value": {"value": first_due.isoformat()}, "extractor_type": "test"}],
    )
    session.commit()
    assert change.change_type == "created"

    item, change = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 1",
        body_text="Entrega del trabajo practico",
        published_at=None,
        starts_at=None,
        due_at=second_due,
        primary_url=source_object.source_url,
        raw_payload={},
        facts_payload=[{"fact_type": "due_at", "value": {"value": second_due.isoformat()}, "extractor_type": "test"}],
    )
    session.commit()

    assert item.due_at.replace(tzinfo=UTC) == second_due
    assert change.state == "updated"
    assert change.change_type == "deadline_changed"


def test_upsert_normalized_item_classifies_schedule_change_from_facts() -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    start_a = datetime.now(UTC) + timedelta(days=7)
    start_b = start_a + timedelta(hours=1)

    upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material_file",
        title="cronograma.pdf",
        body_text="Cronograma de clases",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload={},
        review_status="watch",
        review_reason="high_risk_schedule_document",
        facts_payload=[{"fact_type": "class_session_at", "value": {"value": start_a.isoformat()}, "extractor_type": "test"}],
    )
    session.commit()

    _, change = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material_file",
        title="cronograma.pdf",
        body_text="Cronograma de clases",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload={},
        review_status="watch",
        review_reason="high_risk_schedule_document",
        facts_payload=[{"fact_type": "class_session_at", "value": {"value": start_b.isoformat()}, "extractor_type": "test"}],
    )
    session.commit()

    assert change.change_type == "schedule_changed"


def test_schedule_notifications_for_deadline_change_creates_urgent() -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    due_at = datetime.now(UTC) + timedelta(days=5)
    item, _ = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 2",
        body_text="",
        published_at=None,
        starts_at=None,
        due_at=due_at,
        primary_url=source_object.source_url,
        raw_payload={},
    )
    session.flush()

    schedule_notifications_for_item(
        session,
        item,
        ItemChange(
            state="updated",
            change_type="deadline_changed",
            changed_fields=["due_at"],
            previous_values={"due_at": (due_at - timedelta(days=1)).isoformat(), "starts_at": None},
            new_values={"due_at": due_at.isoformat(), "starts_at": None},
        ),
    )
    session.commit()

    notification = session.scalar(select(Notification))
    assert notification is not None
    assert notification.kind == "urgent"
    assert notification.payload["reason"] == "deadline_changed"


def test_schedule_daily_digest_schedules_immediately_when_past_hour(monkeypatch) -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material",
        title="Clase 1",
        body_text="Apuntes",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload={},
    )
    session.commit()

    monkeypatch.setattr(
        "uni_tracker.services.notifications.get_settings",
        lambda: SimpleNamespace(daily_digest_hour=0),
    )
    before = datetime.now(UTC)
    schedule_daily_digest(session)
    session.commit()

    notification = session.scalar(select(Notification))
    assert notification is not None
    assert notification.kind == "digest"
    assert notification.scheduled_for.replace(tzinfo=UTC) >= before


def test_dispatch_due_notifications_creates_reminder(monkeypatch) -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    item, _ = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 3",
        body_text="Entrega",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=1),
        primary_url=source_object.source_url,
        raw_payload={},
    )
    session.flush()
    session.add(
        Notification(
            normalized_item_id=item.id,
            channel="telegram",
            severity="high",
            kind="urgent",
            dedup_key="urgent:test",
            payload={"reason": "deadline_changed", "reminder_number": 0, "base_dedup": "urgent:test"},
            ack_required=True,
            scheduled_for=datetime.now(UTC) - timedelta(minutes=1),
        )
    )
    session.commit()

    class FakeResponse:
        is_success = True
        text = "ok"

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def post(self, *args, **kwargs) -> FakeResponse:
            return FakeResponse()

    monkeypatch.setattr(
        "uni_tracker.services.notifications.get_settings",
        lambda: SimpleNamespace(telegram_bot_token="token", telegram_chat_id="chat", daily_digest_hour=7),
    )
    monkeypatch.setattr("uni_tracker.services.notifications.httpx.Client", FakeClient)

    result = dispatch_due_notifications(session)
    session.commit()

    notifications = session.scalars(select(Notification).order_by(Notification.id)).all()
    assert result["sent"] == 1
    assert len(notifications) == 2
    assert notifications[1].dedup_key == "urgent:test:reminder:1"


def test_build_digest_message_groups_by_decision_area() -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)

    first_due = datetime.now(UTC) + timedelta(days=4)
    second_due = first_due + timedelta(days=2)
    upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 4",
        body_text="Entrega del trabajo practico",
        published_at=None,
        starts_at=None,
        due_at=first_due,
        primary_url=source_object.source_url,
        raw_payload={},
        facts_payload=[{"fact_type": "due_at", "value": {"value": first_due.isoformat()}, "extractor_type": "test"}],
    )
    session.commit()
    upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 4",
        body_text="Entrega del trabajo practico",
        published_at=None,
        starts_at=None,
        due_at=second_due,
        primary_url=source_object.source_url,
        raw_payload={},
        facts_payload=[{"fact_type": "due_at", "value": {"value": second_due.isoformat()}, "extractor_type": "test"}],
    )

    announcement_object = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="announcement-1",
        object_type="forum_discussion",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/announcement/1",
        current_hash="hash2",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(announcement_object)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=announcement_object.id,
        course_id=course.id,
        item_type="announcement",
        title="Cambio de aula",
        body_text="La clase de mañana pasa al aula 204.",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=announcement_object.source_url,
        raw_payload={},
    )

    upcoming_source = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="assignment-2",
        object_type="assign",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/assignment/2",
        current_hash="hash3",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(upcoming_source)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=upcoming_source.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 5",
        body_text="Entrega en dos dias",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=2),
        primary_url=upcoming_source.source_url,
        raw_payload={},
    )

    earlier_source = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="assignment-3",
        object_type="assign",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/assignment/3",
        current_hash="hash5",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(earlier_source)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=earlier_source.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 3",
        body_text="Entrega en un dia",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=1),
        primary_url=earlier_source.source_url,
        raw_payload={},
    )

    material_source = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="resource-1",
        object_type="resource",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/resource/1",
        current_hash="hash4",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(material_source)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=material_source.id,
        course_id=course.id,
        item_type="material_file",
        title="Clase 2- Estructuras cristalinas",
        body_text="PDF de clase",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=material_source.source_url,
        raw_payload={},
    )
    session.commit()

    digest = build_digest_message(session)
    assert "Urgent changes" in digest
    assert "deadline changed from" in digest
    assert "Action needed soon" in digest
    assert "New materials" not in digest
    assert "new material posted" not in digest
    assert "Curso de prueba" in digest
    assert "2026-" not in digest
    assert digest.index("tp 3") < digest.index("tp 5")


def test_get_risk_items_filters_low_signal_pages() -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)

    schedule_source = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="schedule-pdf",
        object_type="resource",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/schedule.pdf",
        current_hash="hash4",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(schedule_source)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=schedule_source.id,
        course_id=course.id,
        item_type="material_file",
        title="Programa analítico Cálculo I.pdf",
        body_text="Cronograma",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=schedule_source.source_url,
        raw_payload={},
        review_status="watch",
        review_reason="high_risk_schedule_document",
    )

    page_source = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="index-page",
        object_type="page",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/index.html",
        current_hash="hash5",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(page_source)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=page_source.id,
        course_id=course.id,
        item_type="material_file",
        title="index.html",
        body_text="",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=page_source.source_url,
        raw_payload={},
        review_status="needs_review",
        review_reason="low_text_density",
    )
    session.commit()

    risks = get_risk_items(session)
    titles = [item.title for item in risks]
    assert "Programa analítico Cálculo I.pdf" in titles
    assert "index.html" not in titles


def test_get_risk_items_orders_closest_due_first() -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)

    first_source = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="due-1",
        object_type="assign",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/due/1",
        current_hash="hash6",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(first_source)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=first_source.id,
        course_id=course.id,
        item_type="assignment",
        title="TP A",
        body_text="",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=2),
        primary_url=first_source.source_url,
        raw_payload={},
    )

    second_source = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="due-2",
        object_type="assign",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/due/2",
        current_hash="hash7",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(second_source)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=second_source.id,
        course_id=course.id,
        item_type="assignment",
        title="TP B",
        body_text="",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=5),
        primary_url=second_source.source_url,
        raw_payload={},
    )
    session.commit()

    risks = get_risk_items(session)
    risk_titles = [item.title for item in risks if item.title in {"TP A", "TP B"}]
    assert risk_titles == ["TP A", "TP B"]


def test_get_risk_items_places_schedule_docs_after_due_items() -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)

    schedule_source = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="schedule-doc",
        object_type="resource",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/schedule.pdf",
        current_hash="hash8",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(schedule_source)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=schedule_source.id,
        course_id=course.id,
        item_type="material_file",
        title="Programa analítico Cálculo I.pdf",
        body_text="Cronograma",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=schedule_source.source_url,
        raw_payload={},
        review_status="watch",
        review_reason="high_risk_schedule_document",
    )

    due_source = SourceObject(
        source_account_id=source_object.source_account_id,
        course_id=course.id,
        external_id="due-doc",
        object_type="assign",
        parent_external_id=course.external_id,
        source_url="https://example.invalid/due.pdf",
        current_hash="hash9",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(due_source)
    session.flush()
    upsert_normalized_item(
        session,
        source_object_id=due_source.id,
        course_id=course.id,
        item_type="assignment",
        title="TP C",
        body_text="",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=1),
        primary_url=due_source.source_url,
        raw_payload={},
    )
    session.commit()

    risks = get_risk_items(session)
    titles = [item.title for item in risks if item.title in {"TP C", "Programa analítico Cálculo I.pdf"}]
    assert titles == ["TP C", "Programa analítico Cálculo I.pdf"]


def test_poll_telegram_commands_sends_digest_on_demand(monkeypatch) -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 6",
        body_text="Entrega",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=2),
        primary_url=source_object.source_url,
        raw_payload={},
    )
    session.commit()

    class FakeResponse:
        def __init__(self, payload: dict | None = None) -> None:
            self._payload = payload or {}
            self.is_success = True
            self.text = "ok"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._payload

    class FakeClient:
        sent_texts: list[str] = []

        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, *args, **kwargs) -> FakeResponse:
            return FakeResponse(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 100,
                            "message": {
                                "chat": {"id": "chat"},
                                "text": "/digest",
                            },
                        }
                    ],
                }
            )

        def post(self, *args, **kwargs) -> FakeResponse:
            FakeClient.sent_texts.append(kwargs["json"]["text"])
            return FakeResponse()

    monkeypatch.setattr(
        "uni_tracker.services.telegram_bot.get_settings",
        lambda: SimpleNamespace(telegram_bot_token="token", telegram_chat_id="chat", telegram_polling_enabled=True),
    )
    monkeypatch.setattr("uni_tracker.services.telegram_bot.httpx.Client", FakeClient)

    result = poll_telegram_commands(session)
    session.commit()

    state = session.scalar(select(Notification).where(Notification.kind == "digest"))
    assert result.handled == 1
    assert result.sent == 1
    assert FakeClient.sent_texts
    assert "Daily Moodle digest" in FakeClient.sent_texts[0]
    assert state is None


def test_get_item_course_name_resolves_from_categories() -> None:
    session = make_session()
    account, course, source_object = seed_source_graph(session)

    category_object = SourceObject(
        source_account_id=account.id,
        course_id=None,
        external_id="calendar-event-1",
        object_type="calendar_event",
        parent_external_id=None,
        source_url="https://example.invalid/calendar/1",
        current_hash="hash8",
        raw_payload={"categories": [f"UNLZ-{course.display_name.replace(' ', '_')}-1"]},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add(category_object)
    session.flush()
    item, _ = upsert_normalized_item(
        session,
        source_object_id=category_object.id,
        course_id=None,
        item_type="calendar_event",
        title="Quiz 1",
        body_text="",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=2),
        primary_url=category_object.source_url,
        raw_payload={},
    )
    session.commit()

    resolved = get_item_course_name(session, item)
    assert resolved == course.display_name


def test_enrich_recent_items_promotes_item_brief(monkeypatch) -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material_file",
        title="Programa analítico Cálculo I.pdf",
        body_text="Unidad 1: Taylor. Entrega final el 12 de abril.",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload={},
        review_status="watch",
        review_reason="high_risk_schedule_document",
    )
    session.commit()

    class FakeResponse:
        is_success = True
        text = "ok"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "choices": [
                    {
                        "message": {
                            "content": """```json
{
  \"summary_short\": \"Syllabus de Cálculo I con fecha clave de entrega en abril.\",
  \"summary_bullets\": [\"Incluye la unidad 1\", \"Entrega el 12 de abril\"],
  \"key_dates\": [{\"type\": \"due_at\", \"iso_datetime\": \"2026-04-12T02:00:00+00:00\", \"matched_text\": \"12 de abril\"}],
  \"key_requirements\": [\"Revisar la unidad 1\"],
  \"risk_flags\": [\"high_risk_schedule_document\"],
  \"course_context\": {\"course_name\": \"Cálculo I\"},
  \"confidence\": 0.87,
  \"source_refs\": [{\"type\": \"item\", \"item_id\": 380}]
}
```"""
                        }
                    }
                ]
            }

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def post(self, *args, **kwargs) -> FakeResponse:
            return FakeResponse()

    monkeypatch.setattr(
        "uni_tracker.services.llm.get_settings",
        lambda: SimpleNamespace(
            enable_llm=True,
            nvidia_api_key="test-key",
            nvidia_api_url="https://example.invalid/v1/chat/completions",
            nvidia_model="moonshotai/kimi-k2.5",
            llm_body_char_limit=12000,
        ),
    )
    monkeypatch.setattr("uni_tracker.services.llm.httpx.Client", FakeClient)

    result = enrich_recent_items(session)
    session.commit()

    brief = session.scalar(select(ItemBrief))
    job = session.scalar(select(LLMJob).order_by(LLMJob.id.desc()))
    assert result["processed"] == 1
    assert brief is not None
    assert brief.summary_short.startswith("Syllabus de Cálculo I")
    assert brief.origin == "stored"
    assert brief.model == "moonshotai/kimi-k2.5"
    assert job is not None
    assert job.status == "completed"


def test_enrich_recent_items_rejects_title_echo_brief(monkeypatch) -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material_file",
        title="Programa analítico Cálculo I.pdf",
        body_text="Unidad 1: Taylor. Entrega final el 12 de abril.",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload={},
        review_status="watch",
        review_reason="high_risk_schedule_document",
    )
    session.commit()

    class WeakResponse:
        is_success = True
        text = "ok"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "choices": [
                    {
                        "message": {
                            "content": """```json
{
  \"summary_short\": \"Programa analítico Cálculo I.pdf\",
  \"summary_bullets\": [\"Programa analítico Cálculo I.pdf\"],
  \"key_dates\": [],
  \"key_requirements\": [],
  \"risk_flags\": [],
  \"course_context\": {\"course_name\": \"Cálculo I\"},
  \"confidence\": 0.91,
  \"source_refs\": [{\"type\": \"item\", \"item_id\": 380}]
}
```"""
                        }
                    }
                ]
            }

    class WeakClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "WeakClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def post(self, *args, **kwargs) -> WeakResponse:
            return WeakResponse()

    monkeypatch.setattr(
        "uni_tracker.services.llm.get_settings",
        lambda: SimpleNamespace(
            enable_llm=True,
            nvidia_api_key="test-key",
            nvidia_api_url="https://example.invalid/v1/chat/completions",
            nvidia_model="moonshotai/kimi-k2.5",
            llm_body_char_limit=12000,
        ),
    )
    monkeypatch.setattr("uni_tracker.services.llm.httpx.Client", WeakClient)

    result = enrich_recent_items(session)
    session.commit()

    brief = session.scalar(select(ItemBrief))
    job = session.scalar(select(LLMJob).order_by(LLMJob.id.desc()))
    assert result["processed"] == 0
    assert brief is None
    assert job is not None
    assert job.status == "rejected"
    assert "summary echoes title" in (job.error_text or "")


def test_backfill_item_briefs_replaces_weak_brief(monkeypatch) -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    item, _ = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material_file",
        title="Programa analítico Cálculo I.pdf",
        body_text="Unidad 1: Taylor. Entrega final el 12 de abril.",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload={},
        review_status="watch",
        review_reason="high_risk_schedule_document",
    )
    upsert_item_brief(
        session,
        item=item,
        payload={
            "summary_short": item.title,
            "summary_bullets": [item.title],
            "key_dates": [],
            "key_requirements": [],
            "risk_flags": [],
            "course_context": {"course_name": course.display_name},
            "confidence": 0.2,
            "source_refs": [{"type": "item", "item_id": item.id}],
        },
        model="moonshotai/kimi-k2.5",
        llm_job_id=None,
        origin="stored",
    )
    session.commit()

    class WeakResponse:
        is_success = True
        text = "ok"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "choices": [
                    {
                        "message": {
                            "content": """```json
{
  \"summary_short\": \"Programa analítico Cálculo I.pdf\",
  \"summary_bullets\": [\"Programa analítico Cálculo I.pdf\"],
  \"key_dates\": [],
  \"key_requirements\": [],
  \"risk_flags\": [],
  \"course_context\": {\"course_name\": \"Cálculo I\"},
  \"confidence\": 0.91,
  \"source_refs\": [{\"type\": \"item\", \"item_id\": 380}]
}
```"""
                        }
                    }
                ]
            }

    class WeakClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "WeakClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def post(self, *args, **kwargs) -> WeakResponse:
            return WeakResponse()

    monkeypatch.setattr(
        "uni_tracker.services.llm.get_settings",
        lambda: SimpleNamespace(
            enable_llm=True,
            nvidia_api_key="test-key",
            nvidia_api_url="https://example.invalid/v1/chat/completions",
            nvidia_model="moonshotai/kimi-k2.5",
            llm_body_char_limit=12000,
        ),
    )
    monkeypatch.setattr("uni_tracker.services.llm.httpx.Client", WeakClient)

    result = backfill_item_briefs(session, [item], force=True)
    session.commit()

    brief = session.scalar(select(ItemBrief))
    assert result["processed"] == 1
    assert brief is not None
    assert brief.origin == "backfill"
    assert brief.summary_short.startswith(course.display_name)
    assert len(brief.summary_bullets) >= 2


def test_enrich_recent_items_retries_retryable_failures(monkeypatch) -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material_file",
        title="Programa analitico Calculo I.pdf",
        body_text="Unidad 1: Taylor. Entrega final el 12 de abril.",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload={},
        review_status="watch",
        review_reason="high_risk_schedule_document",
    )
    session.commit()

    class RetryResponse:
        def __init__(self, status_code: int, payload: dict | None = None) -> None:
            self.status_code = status_code
            self._payload = payload or {}
            self.text = "retry"
            self.headers: dict[str, str] = {}
            self.request = httpx.Request("POST", "https://example.invalid")

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("error", request=self.request, response=self)

        def json(self) -> dict:
            return self._payload

    class RetryClient:
        def __init__(self, *args, **kwargs) -> None:
            self.calls = 0

        def __enter__(self) -> "RetryClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def post(self, *args, **kwargs):
            self.calls += 1
            if self.calls < 3:
                return RetryResponse(503)
            return RetryResponse(
                200,
                {
                    "choices": [
                        {
                            "message": {
                                "content": """```json
{
  "summary_short": "Programa del curso con fecha de entrega en abril.",
  "summary_bullets": ["Incluye contenidos de la primera unidad", "Entrega el 12 de abril"],
  "key_dates": [{"type": "due_at", "iso_datetime": "2026-04-12T02:00:00+00:00", "matched_text": "12 de abril"}],
  "key_requirements": ["Revisar la unidad 1"],
  "risk_flags": ["high_risk_schedule_document"],
  "course_context": {"course_name": "Cálculo I"},
  "confidence": 0.87,
  "source_refs": [{"type": "item", "item_id": 1}]
}
```"""
                            }
                        }
                    ]
                },
            )

    monkeypatch.setattr(
        "uni_tracker.services.llm.get_settings",
        lambda: SimpleNamespace(
            enable_llm=True,
            nvidia_api_key="test-key",
            nvidia_api_url="https://example.invalid/v1/chat/completions",
            nvidia_model="moonshotai/kimi-k2.5",
            llm_body_char_limit=12000,
            llm_request_max_attempts=3,
            llm_retry_base_delay_seconds=0.0,
            llm_retry_max_delay_seconds=0.0,
            llm_retry_cooldown_minutes=180,
        ),
    )
    monkeypatch.setattr("uni_tracker.services.llm.httpx.Client", RetryClient)
    monkeypatch.setattr("uni_tracker.services.llm.time.sleep", lambda *_args, **_kwargs: None)

    result = enrich_recent_items(session)
    session.commit()

    job = session.scalar(select(LLMJob).order_by(LLMJob.id.desc()))
    assert result["processed"] == 1
    assert job is not None
    assert job.status == "completed"
    assert job.request_payload["attempts"] == 3


def test_enrich_recent_items_skips_recent_failed_jobs(monkeypatch) -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    item, _ = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="material_file",
        title="Programa analitico Calculo I.pdf",
        body_text="Unidad 1: Taylor. Entrega final el 12 de abril.",
        published_at=None,
        starts_at=None,
        due_at=None,
        primary_url=source_object.source_url,
        raw_payload={},
        review_status="watch",
        review_reason="high_risk_schedule_document",
    )
    session.add(
        LLMJob(
            normalized_item_id=item.id,
            raw_artifact_id=None,
            job_type="summary",
            provider="nvidia",
            model="moonshotai/kimi-k2.5",
            status="failed",
            request_payload={"attempts": 3},
            error_text="503",
            finished_at=datetime.now(UTC) - timedelta(minutes=15),
        )
    )
    session.commit()

    class ExplodingClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "ExplodingClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def post(self, *args, **kwargs):
            raise AssertionError("client.post should not be called during cooldown")

    monkeypatch.setattr(
        "uni_tracker.services.llm.get_settings",
        lambda: SimpleNamespace(
            enable_llm=True,
            nvidia_api_key="test-key",
            nvidia_api_url="https://example.invalid/v1/chat/completions",
            nvidia_model="moonshotai/kimi-k2.5",
            llm_body_char_limit=12000,
            llm_request_max_attempts=3,
            llm_retry_base_delay_seconds=0.0,
            llm_retry_max_delay_seconds=0.0,
            llm_retry_cooldown_minutes=180,
        ),
    )
    monkeypatch.setattr("uni_tracker.services.llm.httpx.Client", ExplodingClient)

    result = enrich_recent_items(session)
    session.commit()

    assert result["processed"] == 0
    jobs = session.scalars(select(LLMJob).order_by(LLMJob.id)).all()
    assert len(jobs) == 1


def test_get_item_brief_falls_back_without_llm_data() -> None:
    session = make_session()
    _, course, source_object = seed_source_graph(session)
    item, _ = upsert_normalized_item(
        session,
        source_object_id=source_object.id,
        course_id=course.id,
        item_type="assignment",
        title="TP 7",
        body_text="",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=3),
        primary_url=source_object.source_url,
        raw_payload={},
    )
    session.commit()

    brief = get_item_brief(session, item.id)
    assert brief is not None
    assert brief["origin"] == "fallback"
    assert brief["item"].id == item.id
    assert brief["summary_short"] == "TP 7"
    assert brief["key_dates"]


def test_build_digest_message_orders_course_blocks_by_soonest_due() -> None:
    session = make_session()
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

    late_course = Course(
        source_account_id=account.id,
        external_id="201",
        shortname="LATE",
        fullname="Alpha Course",
        display_name="Alpha Course",
        course_url="https://example.invalid/course/201",
        visible=True,
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    early_course = Course(
        source_account_id=account.id,
        external_id="202",
        shortname="EARLY",
        fullname="Zeta Course",
        display_name="Zeta Course",
        course_url="https://example.invalid/course/202",
        visible=True,
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add_all([late_course, early_course])
    session.flush()

    late_object = SourceObject(
        source_account_id=account.id,
        course_id=late_course.id,
        external_id="late-assign",
        object_type="assign",
        parent_external_id=late_course.external_id,
        source_url="https://example.invalid/late",
        current_hash="hash10",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    early_object = SourceObject(
        source_account_id=account.id,
        course_id=early_course.id,
        external_id="early-assign",
        object_type="assign",
        parent_external_id=early_course.external_id,
        source_url="https://example.invalid/early",
        current_hash="hash11",
        raw_payload={},
        first_seen_at=datetime.now(UTC),
        last_seen_at=datetime.now(UTC),
    )
    session.add_all([late_object, early_object])
    session.flush()

    upsert_normalized_item(
        session,
        source_object_id=late_object.id,
        course_id=late_course.id,
        item_type="assignment",
        title="TP Late",
        body_text="",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=5),
        primary_url=late_object.source_url,
        raw_payload={},
    )
    upsert_normalized_item(
        session,
        source_object_id=early_object.id,
        course_id=early_course.id,
        item_type="assignment",
        title="TP Early",
        body_text="",
        published_at=None,
        starts_at=None,
        due_at=datetime.now(UTC) + timedelta(days=1),
        primary_url=early_object.source_url,
        raw_payload={},
    )
    session.commit()

    digest = build_digest_message(session)
    assert digest.index("Zeta Course") < digest.index("Alpha Course")
