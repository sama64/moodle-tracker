#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

API_BASE = os.environ.get('MOODLE_TRACKER_API', 'http://localhost:8000')
STATE_DIR = Path.home() / '.hermes' / 'state'
CURSOR_PATH = STATE_DIR / 'moodle_tracker_cursor.txt'
DEADLINE_REMINDER_PATH = STATE_DIR / 'moodle_tracker_deadline_reminders.json'
SCHEDULE_REMINDER_PATH = STATE_DIR / 'moodle_tracker_schedule_reminders.json'
TZ = ZoneInfo('America/Argentina/Buenos_Aires')
URGENT_DEADLINE_WINDOW_HOURS = 24
# Exams/schedule dates need earlier heads-up than ordinary deadlines, but not a
# full week of noise. Keep this tight: 3 days, day-before, and same-day.
EXAM_REMINDER_BUCKETS_HOURS = (
    ('three_days_before', 72),
    ('day_before', 24),
)
# Schedule documents often only give a date, not a time. Those must be handled
# as all-day academic risks; otherwise a date-only exam becomes "past" at
# 00:00 and we never send the same-day heads-up Santiago actually needs.
EXAM_REMINDER_BUCKETS_DAYS = (
    ('three_days_before', 3),
    ('day_before', 1),
    ('same_day', 0),
)
URGENT_SCHEDULE_WINDOW_HOURS = max(hours for _, hours in EXAM_REMINDER_BUCKETS_HOURS)
ACTIONABLE_DEADLINE_WINDOW_DAYS = 14
EXAM_WORDS = ('parcial', 'recuperatorio', 'examen')
SCHEDULE_TITLE_WORDS = ('cronograma', 'calendario', 'horario', 'fechas')
DATE_RE = re.compile(r'\b(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?(?:\s*(?:de\s*)?(\d{1,2})[:.](\d{2}))?', re.I)
IMPORTANT_TITLE_KEYWORDS = (
    'parcial', 'recuperatorio', 'examen', 'entrega', 'vence', 'vencimiento',
    'cuestionario', 'quiz', 'tp', 'trabajo práctico', 'trabajo practico',
    'suspendida', 'suspendido', 'cambio de aula', 'aula', 'horario',
    'cronograma', 'calendario', 'fecha', 'fechas',
)
# For passive resources/files, be much stricter: a new TP PDF/resource is usually
# just class material, not an interruption-worthy obligation. Assignments/quizzes
# still alert through item_type/deadline logic below.
IMPORTANT_RESOURCE_KEYWORDS = (
    'parcial', 'recuperatorio', 'examen', 'entrega', 'vence', 'vencimiento',
    'suspendida', 'suspendido', 'cambio de aula', 'aula', 'horario',
    'cronograma', 'calendario', 'fecha', 'fechas',
)
PASSIVE_RESOURCE_TYPES = {
    'material', 'material_file', 'resource', 'folder', 'url', 'page', 'book', 'label',
}
LOW_SIGNAL_TITLE_KEYWORDS = (
    'clase', 'tablas', 'tabla', 'teorica', 'teórica', 'practica', 'práctica',
    'parte', 'capitulo', 'capítulo', 'apunte', 'diapositiva', 'presentación',
    'presentacion', 'pdf',
)
NON_USER_BLOCKING_STALE_COLLECTORS = {'moodle_files', 'moodle_grades'}
NON_COURSE_ANNOUNCEMENT_COURSE_KEYWORDS = (
    'red de estudiantes', 'centro de estudiantes', 'bienestar', 'becas',
)
NON_COURSE_ANNOUNCEMENT_KEYWORDS = (
    'elecciones', 'premio', 'pre ingeniería', 'pre ingenieria',
    'beca', 'becas', 'convocatoria', 'invest your talent',
    'aulas examenes finales', 'aulas exámenes finales',
)


def fetch(path: str, method: str = 'GET', data: dict | None = None):
    url = f"{API_BASE}{path}"
    payload = None
    headers = {}
    if data is not None:
        payload = json.dumps(data).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    request = urllib.request.Request(url, data=payload, method=method, headers=headers)
    with urllib.request.urlopen(request, timeout=20) as r:
        return json.load(r)


def try_repair_collectors(collectors: list[str]) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for collector in collectors:
        try:
            response = fetch(f'/sync/run/{urllib.parse.quote(collector, safe="")}', method='POST')
            status = str(response.get('status') or 'unknown')
            results.append({'collector': collector, 'status': status})
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode('utf-8', errors='replace').strip()
            results.append({'collector': collector, 'status': f'http_error:{exc.code}', 'detail': detail})
        except Exception as exc:
            results.append({'collector': collector, 'status': 'error', 'detail': str(exc)})
    return results


def repair_stale_collectors(initial_health: dict, max_rounds: int = 3) -> tuple[dict, list[dict[str, str]], list[str]]:
    health = initial_health
    all_attempts: list[dict[str, str]] = []
    repaired_collectors: set[str] = set()
    attempted_collectors: set[str] = set()

    for _ in range(max_rounds):
        stale_collectors = list(health.get('details', {}).get('stale_collectors', []))
        pending = [collector for collector in stale_collectors if collector not in attempted_collectors]
        if not pending:
            break
        attempts = try_repair_collectors(pending)
        all_attempts.extend(attempts)
        attempted_collectors.update(pending)
        health = fetch('/health/details')
        remaining = set(health.get('details', {}).get('stale_collectors', []))
        repaired_collectors.update(collector for collector in pending if collector not in remaining)

    return health, all_attempts, sorted(repaired_collectors)


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def fmt_local(value: str | None) -> str | None:
    dt = parse_dt(value)
    if not dt:
        return None
    return dt.astimezone(TZ).strftime('%a %d/%m %H:%M')


def fmt_schedule_time(event: dict) -> str | None:
    dt = parse_dt(event.get('at'))
    if not dt:
        return None
    local = dt.astimezone(TZ)
    if event.get('all_day'):
        return local.strftime('%a %d/%m') + ' (time not specified)'
    return local.strftime('%a %d/%m %H:%M')


def bump_iso_cursor(value: str) -> str:
    dt = parse_dt(value)
    if dt is None:
        return value
    return (dt + timedelta(microseconds=1)).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def load_reminder_state() -> dict[str, str]:
    return load_json_state(DEADLINE_REMINDER_PATH)


def load_json_state(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items()}


def save_json_state(path: Path, state: dict[str, str]) -> None:
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True))


def save_reminder_state(state: dict[str, str]) -> None:
    save_json_state(DEADLINE_REMINDER_PATH, state)


def reminder_key(item: dict) -> str:
    return f"{item.get('id')}|{item.get('due_at')}|{item.get('completion_state')}"


def schedule_reminder_key(event: dict, bucket: str) -> str:
    return f"{bucket}|{event.get('course_id')}|{event.get('at')}|{event.get('label')}"


def exam_reminder_bucket(event: dict, now_utc: datetime) -> str | None:
    at = parse_dt(event.get('at'))
    if at is None:
        return None
    if event.get('all_day'):
        event_date = at.astimezone(TZ).date()
        today = now_utc.astimezone(TZ).date()
        days_until = (event_date - today).days
        if days_until < 0:
            return None
        # Return the tightest date-based window so reminders escalate from
        # 3 days -> day before -> same day, even when Moodle gave no time.
        for bucket, window_days in sorted(EXAM_REMINDER_BUCKETS_DAYS, key=lambda pair: pair[1]):
            if days_until <= window_days:
                return bucket
        return None

    hours_until = (at - now_utc).total_seconds() / 3600
    if hours_until < 0:
        return None
    # Return the tightest matching window so reminders escalate from 3 days -> 1 day.
    for bucket, window_hours in sorted(EXAM_REMINDER_BUCKETS_HOURS, key=lambda pair: pair[1]):
        if hours_until <= window_hours:
            return bucket
    return None


def bucket_label(bucket: str) -> str:
    return {
        'three_days_before': 'exam/schedule within 3 days',
        'day_before': 'exam/schedule tomorrow/soon',
        'same_day': 'exam/schedule today',
    }.get(bucket, 'exam/schedule soon')


def schedule_key_event_at(key: str) -> datetime | None:
    parts = key.split('|', 3)
    if len(parts) < 3:
        return None
    return parse_dt(parts[2])


def prune_schedule_state(state: dict[str, str], active_keys: set[str], now_utc: datetime) -> dict[str, str]:
    # Risk/schedule extraction can be temporarily empty after collector churn or
    # API restarts. Do not erase still-future reminder keys just because the
    # event was absent in one run; that causes the next run to re-alert it.
    retained: dict[str, str] = {}
    for key, sent_at in state.items():
        event_at = schedule_key_event_at(key)
        if key in active_keys or event_at is None or now_utc <= event_at + timedelta(days=1):
            retained[key] = sent_at
    return retained


def looks_like_schedule_doc(item: dict) -> bool:
    title = (item.get('title') or '').lower()
    return item.get('review_reason') == 'high_risk_schedule_document' or any(word in title for word in SCHEDULE_TITLE_WORDS)


def schedule_text(item: dict) -> str:
    text = item.get('body_text') or ''
    if text.strip():
        return text
    item_id = item.get('id')
    if item_id is None:
        return ''
    try:
        content = fetch(f'/items/{item_id}/content')
    except Exception:
        return ''
    best = ''
    for artifact in content.get('artifacts') or []:
        candidate = artifact.get('extracted_text') or ''
        if artifact.get('extraction_status') == 'completed' and len(candidate) > len(best):
            best = candidate
    return best


def extract_schedule_events(risks: list[dict], now_utc: datetime) -> list[dict]:
    horizon_end = now_utc + timedelta(hours=URGENT_SCHEDULE_WINDOW_HOURS)
    current_year = now_utc.astimezone(TZ).year
    events = []
    for item in risks:
        if not looks_like_schedule_doc(item):
            continue
        text = schedule_text(item)
        if not any(word in text.lower() for word in EXAM_WORDS):
            continue
        for match in DATE_RE.finditer(text):
            start = max(match.start() - 45, 0)
            end = min(match.end() + 55, len(text))
            context = text[start:end]
            lowered_context = context.lower()
            if not any(word in lowered_context for word in EXAM_WORDS):
                continue
            before = text[max(match.start() - 35, 0):match.start()].lower()
            after = text[match.end():min(match.end() + 45, len(text))].lower()
            if not any(word in after for word in EXAM_WORDS):
                continue
            after_lower = after.lower()
            exam_positions = [after_lower.find(word) for word in EXAM_WORDS if word in after_lower]
            if exam_positions and DATE_RE.search(after[:min(exam_positions)]):
                continue
            day = int(match.group(1))
            month = int(match.group(2))
            year = int(match.group(3)) if match.group(3) else current_year
            if year < 100:
                year += 2000
            hour = int(match.group(4) or 0)
            minute = int(match.group(5) or 0)
            all_day = match.group(4) is None
            try:
                local_dt = datetime(year, month, day, hour, minute, tzinfo=TZ)
            except ValueError:
                continue
            dt = local_dt.astimezone(timezone.utc)
            if all_day:
                today = now_utc.astimezone(TZ).date()
                event_date = local_dt.date()
                horizon_date = horizon_end.astimezone(TZ).date()
                if not (today <= event_date <= horizon_date):
                    continue
            elif not (now_utc <= dt <= horizon_end):
                continue
            label = ' '.join(context.split())[:220]
            events.append({
                'at': dt.isoformat().replace('+00:00', 'Z'),
                'course_id': item.get('course_id'),
                'title': item.get('title'),
                'label': label,
                'url': item.get('primary_url'),
                'all_day': all_day,
            })
    deduped = {}
    for event in events:
        key = (event['course_id'], event['at'], event['label'][:80])
        deduped.setdefault(key, event)
    return sorted(deduped.values(), key=lambda event: (event['at'], event.get('course_id') or 0))


def _text_blob(item: dict) -> str:
    parts = [item.get('title'), item.get('body_text'), item.get('review_reason')]
    return ' '.join(str(part or '').lower() for part in parts)


def _hours_until(value: str | None, now_utc: datetime) -> float | None:
    dt = parse_dt(value)
    if dt is None:
        return None
    return (dt - now_utc).total_seconds() / 3600


def announcement_context(item: dict) -> str | None:
    body = ' '.join(str(item.get('body_text') or '').split())
    if not body:
        return None
    sentences = re.split(r'(?<=[.!?])\s+', body)
    for sentence in sentences:
        low = sentence.lower()
        if any(word in low for word in EXAM_WORDS) or DATE_RE.search(sentence):
            return sentence[:220]
    return body[:160]


def is_non_course_announcement(item: dict, course_map: dict[int, str]) -> bool:
    course = (course_map.get(item.get('course_id')) or '').lower()
    title = (item.get('title') or '').lower()
    blob = _text_blob(item)
    if not any(keyword in course for keyword in NON_COURSE_ANNOUNCEMENT_COURSE_KEYWORDS):
        return False
    return any(keyword in title or keyword in blob for keyword in NON_COURSE_ANNOUNCEMENT_KEYWORDS)


def is_actionable_change(item: dict, now_utc: datetime, course_map: dict[int, str]) -> bool:
    """Return True only for changes worth interrupting Santiago about.

    Moodle emits lots of low-signal resource churn (class PDFs, tables, generic
    forum containers). Keep those out of Telegram unless they affect deadlines,
    exams, schedules, or near-term obligations.
    """
    item_type = item.get('item_type') or ''
    change_kind = item.get('change_kind') or ''
    title = (item.get('title') or '').lower()
    blob = _text_blob(item)
    due_hours = _hours_until(item.get('due_at'), now_utc)
    starts_hours = _hours_until(item.get('starts_at'), now_utc)
    has_important_keyword = any(keyword in blob for keyword in IMPORTANT_TITLE_KEYWORDS)

    if is_non_course_announcement(item, course_map):
        return False

    if change_kind in {'deadline_changed', 'deadline_added', 'deadline_removed'}:
        return True

    if item_type in {'assignment', 'quiz'}:
        if due_hours is None:
            return True
        return -24 <= due_hours <= ACTIONABLE_DEADLINE_WINDOW_DAYS * 24

    if item_type == 'calendar_event':
        return has_important_keyword and starts_hours is not None and -24 <= starts_hours <= ACTIONABLE_DEADLINE_WINDOW_DAYS * 24

    if item_type in {'forum_discussion', 'forum_post', 'announcement'}:
        return has_important_keyword

    if item_type == 'forum' and title in {'avisos', 'novedades', 'consultas'}:
        return False

    if item.get('review_status') == 'watch' and item.get('review_reason') == 'high_risk_schedule_document':
        return True

    if item_type in PASSIVE_RESOURCE_TYPES:
        return any(keyword in blob for keyword in IMPORTANT_RESOURCE_KEYWORDS)

    if has_important_keyword:
        return True

    if any(keyword in title for keyword in LOW_SIGNAL_TITLE_KEYWORDS):
        return False

    return False


def main() -> int:
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    courses = fetch('/courses')
    course_map = {c['id']: c['display_name'] for c in courses}
    health = fetch('/health/details')
    health, repair_attempts, repaired_collectors = repair_stale_collectors(health)
    stale_collectors_before = sorted({attempt['collector'] for attempt in repair_attempts})
    deadlines = fetch('/deadlines/upcoming')
    try:
        risks = fetch('/risks')
    except Exception:
        risks = []

    if CURSOR_PATH.exists():
        since = CURSOR_PATH.read_text().strip()
        initialized = False
    else:
        since = now_utc.isoformat().replace('+00:00', 'Z')
        CURSOR_PATH.write_text(since)
        initialized = True

    query = urllib.parse.quote(since, safe='')
    changes = fetch(f'/changes/since?since={query}')

    meaningful = []
    low_priority_count = 0
    for item in changes:
        if not item.get('meaningful_change'):
            continue
        if item.get('change_kind') == 'refresh_only':
            continue
        title = (item.get('title') or '').lower()
        item_type = item.get('item_type')
        # Skip mirrored calendar rows that usually duplicate a real assignment/quiz.
        if item_type == 'calendar_event' and ('está en fecha de entrega' in title or title.endswith(' cierra')):
            continue
        if not is_actionable_change(item, now_utc, course_map):
            low_priority_count += 1
            continue
        meaningful.append(item)

    # Deduplicate by course + type + title + URL, keeping newest.
    deduped = {}
    for item in meaningful:
        key = (
            item.get('course_id'),
            item.get('item_type'),
            item.get('title'),
            item.get('primary_url'),
        )
        prev = deduped.get(key)
        if prev is None or (item.get('updated_at') or '') > (prev.get('updated_at') or ''):
            deduped[key] = item
    meaningful = sorted(deduped.values(), key=lambda x: (x.get('updated_at') or '', x.get('id') or 0))

    latest_seen = since
    for item in changes:
        updated_at = item.get('updated_at')
        if updated_at and updated_at >= latest_seen:
            latest_seen = bump_iso_cursor(updated_at)
    CURSOR_PATH.write_text(latest_seen)

    stale_collectors = list(health.get('details', {}).get('stale_collectors', []))
    user_blocking_stale_collectors = [
        collector for collector in stale_collectors
        if collector not in NON_USER_BLOCKING_STALE_COLLECTORS
    ]
    source_auth = health.get('details', {}).get('source_auth_health')
    failed_repairs = [
        attempt for attempt in repair_attempts
        if attempt.get('collector') in user_blocking_stale_collectors or (
            attempt.get('collector') not in NON_USER_BLOCKING_STALE_COLLECTORS
            and attempt.get('status') != 'completed'
        )
    ]

    reminder_state = load_reminder_state()
    urgent_deadlines = []
    active_reminder_keys = set()
    for item in deadlines:
        if item.get('completion_state') == 'completed':
            continue
        due_dt = parse_dt(item.get('due_at'))
        if due_dt is None:
            continue
        hours_until_due = (due_dt - now_utc).total_seconds() / 3600
        if not (0 <= hours_until_due <= URGENT_DEADLINE_WINDOW_HOURS):
            continue
        key = reminder_key(item)
        active_reminder_keys.add(key)
        if key in reminder_state:
            continue
        urgent_deadlines.append(item)
        reminder_state[key] = now_utc.isoformat().replace('+00:00', 'Z')

    reminder_state = {k: v for k, v in reminder_state.items() if k in active_reminder_keys}
    save_reminder_state(reminder_state)

    schedule_state = load_json_state(SCHEDULE_REMINDER_PATH)
    urgent_schedule_events = []
    active_schedule_keys = set()
    for event in extract_schedule_events(risks, now_utc):
        bucket = exam_reminder_bucket(event, now_utc)
        if bucket is None:
            continue
        key = schedule_reminder_key(event, bucket)
        active_schedule_keys.add(key)
        if key in schedule_state:
            continue
        event['reminder_bucket'] = bucket
        urgent_schedule_events.append(event)
        schedule_state[key] = now_utc.isoformat().replace('+00:00', 'Z')
    schedule_state = prune_schedule_state(schedule_state, active_schedule_keys, now_utc)
    save_json_state(SCHEDULE_REMINDER_PATH, schedule_state)

    lines = []
    if urgent_deadlines:
        for item in sorted(urgent_deadlines, key=lambda x: (x.get('due_at') or '', x.get('id') or 0)):
            course = course_map.get(item.get('course_id'), f"Course {item.get('course_id')}")
            due = fmt_local(item.get('due_at'))
            bits = [f"• {course}", f"{item.get('title')}", '[due soon]']
            if due:
                bits.append(due)
            url = item.get('primary_url')
            if url:
                bits.append(url)
            lines.append(' — '.join(bits))

    if urgent_schedule_events:
        for event in urgent_schedule_events:
            course = course_map.get(event.get('course_id'), f"Course {event.get('course_id')}")
            at = fmt_schedule_time(event)
            label = bucket_label(str(event.get('reminder_bucket') or ''))
            bits = [f"• {course}", event.get('label') or event.get('title'), f'[{label}]']
            if at:
                bits.append(at)
            url = event.get('url')
            if url:
                bits.append(url)
            lines.append(' — '.join(bits))

    if meaningful:
        for item in meaningful:
            course = course_map.get(item.get('course_id'), f"Course {item.get('course_id')}")
            kind = item.get('change_kind') or 'changed'
            due = fmt_local(item.get('due_at'))
            starts = fmt_local(item.get('starts_at'))
            when = due or starts
            bits = [f"• {course}", f"{item.get('title')}", f"[{kind}]"]
            if when:
                bits.append(f"{when}")
            context = announcement_context(item)
            if context and item.get('item_type') in {'announcement', 'forum_discussion', 'forum_post'}:
                bits.append(context)
            url = item.get('primary_url')
            if url:
                bits.append(url)
            lines.append(' — '.join(bits))

    if source_auth != 'healthy':
        lines.append(f"• Tracker issue — source_auth_health={source_auth}")
    if user_blocking_stale_collectors:
        stale_text = ', '.join(user_blocking_stale_collectors)
        if failed_repairs:
            lines.append(f"• Tracker issue — couldn't refresh automatically: {stale_text}")
        else:
            lines.append(f"• Tracker issue — still stale after retry: {stale_text}")

    payload = {
        'initialized': initialized,
        'since': since,
        'cursor_after': latest_seen,
        'meaningful_count': len(meaningful),
        'urgent_schedule_count': len(urgent_schedule_events),
        'low_priority_count': low_priority_count,
        'health_warning': source_auth != 'healthy' or bool(user_blocking_stale_collectors),
        'send_update': bool(meaningful) or bool(urgent_deadlines) or bool(urgent_schedule_events) or source_auth != 'healthy' or bool(user_blocking_stale_collectors),
        'repaired_collectors': repaired_collectors,
        'collector_repair_attempts': repair_attempts,
        'non_user_blocking_stale_collectors': [
            collector for collector in stale_collectors
            if collector in NON_USER_BLOCKING_STALE_COLLECTORS
        ],
        'lines': lines,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
