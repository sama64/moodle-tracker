from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.config import get_settings
from uni_tracker.models import SystemState
from uni_tracker.services.notifications import build_digest_message
from uni_tracker.services.tools import get_item_course_name, get_recent_changes, get_risk_items, get_upcoming_deadlines
from uni_tracker.services.timeutils import format_local_date_time


TELEGRAM_UPDATE_STATE_KEY = "telegram_updates_offset"


@dataclass
class TelegramCommandResult:
    handled: int = 0
    sent: int = 0
    skipped: int = 0


def poll_telegram_commands(session: Session) -> TelegramCommandResult:
    settings = get_settings()
    if not settings.telegram_polling_enabled:
        return TelegramCommandResult()
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        return TelegramCommandResult()

    offset = _get_update_offset(session)
    updates = _get_updates(settings.telegram_bot_token, offset=offset)
    if not updates:
        return TelegramCommandResult()

    result = TelegramCommandResult()
    max_update_id = offset or 0
    for update in updates:
        update_id = int(update.get("update_id") or 0)
        max_update_id = max(max_update_id, update_id + 1)
        if _handle_update(session, update, settings.telegram_bot_token, settings.telegram_chat_id):
            result.handled += 1
            result.sent += 1
        else:
            result.skipped += 1

    if offset is None or max_update_id > offset:
        _set_update_offset(session, max_update_id)
    return result


def _handle_update(session: Session, update: dict[str, Any], token: str, chat_id: str) -> bool:
    message = update.get("message") or update.get("edited_message")
    if not isinstance(message, dict):
        return False
    chat = message.get("chat") or {}
    if str(chat.get("id")) != str(chat_id):
        return False
    text = (message.get("text") or "").strip()
    if not text.startswith("/"):
        return False

    command, *args = text.split(maxsplit=1)
    command = command.split("@", 1)[0].lower()
    argument = args[0].strip() if args else ""

    if command in {"/start", "/help"}:
        reply = _help_text()
    elif command == "/digest":
        window_hours = _parse_positive_int(argument, default=24)
        reply = build_digest_message(session, window_hours=window_hours) or (
            f"No notable Moodle changes in the last {window_hours} hours."
        )
    elif command == "/risks":
        reply = _items_text(session, "Risk items", get_risk_items(session))
    elif command == "/deadlines":
        reply = _items_text(session, "Upcoming deadlines", get_upcoming_deadlines(session))
    elif command == "/changes":
        reply = _items_text(session, "Recent changes", get_recent_changes(session))
    else:
        reply = "Unknown command. Send /help for available commands."

    sent = _send_message(token, chat_id, reply)
    return sent.is_success


def _help_text() -> str:
    return "\n".join(
        [
            "Commands:",
            "/digest [hours] - current digest for the last 24h or custom window",
            "/risks - items that need attention",
            "/deadlines - upcoming deadlines",
            "/changes - recent changes",
            "/help - show this message",
        ]
    )


def _items_text(session: Session, title: str, items: list[Any]) -> str:
    if not items:
        return f"{title}: none"
    lines = [title]
    for item in items[:5]:
        parts = ["-"]
        if item.due_at:
            parts.append(format_local_date_time(item.due_at))
        elif item.starts_at:
            parts.append(format_local_date_time(item.starts_at))
        elif item.updated_at:
            parts.append(format_local_date_time(item.updated_at))
        else:
            parts.append("No date")
        course_name = get_item_course_name(session, item)
        parts.append(f"[{course_name}]")
        parts.append(item.title)
        if getattr(item, "review_status", None) in {"watch", "needs_review"} and getattr(item, "review_reason", None):
            parts.append(f"{item.review_reason.replace('_', ' ')}")
        if item.primary_url:
            parts.append(item.primary_url)
        lines.append(" ".join(parts))
    return "\n".join(lines)


def _send_message(token: str, chat_id: str, text: str) -> httpx.Response:
    with httpx.Client(timeout=20.0) as client:
        response = client.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
        )
    return response


def _parse_positive_int(value: str, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _get_updates(token: str, offset: int | None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"timeout": 0, "limit": 50}
    if offset is not None:
        params["offset"] = offset
    with httpx.Client(timeout=20.0) as client:
        response = client.get(f"https://api.telegram.org/bot{token}/getUpdates", params=params)
        response.raise_for_status()
        payload = response.json()
    if not payload.get("ok"):
        return []
    updates = payload.get("result") or []
    return [update for update in updates if isinstance(update, dict)]


def _get_update_offset(session: Session) -> int | None:
    state = session.scalar(select(SystemState).where(SystemState.key == TELEGRAM_UPDATE_STATE_KEY))
    if state is None:
        return None
    value = state.value_json.get("offset")
    return int(value) if value is not None else None


def _set_update_offset(session: Session, offset: int) -> None:
    state = session.scalar(select(SystemState).where(SystemState.key == TELEGRAM_UPDATE_STATE_KEY))
    payload = {"offset": offset, "updated_at": datetime.now(UTC).isoformat()}
    if state is None:
        session.add(SystemState(key=TELEGRAM_UPDATE_STATE_KEY, value_json=payload))
        return
    state.value_json = payload
