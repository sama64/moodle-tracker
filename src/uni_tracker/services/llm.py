from __future__ import annotations

import json
import re
import time
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.config import get_settings
from uni_tracker.models import ItemFact, LLMJob, NormalizedItem
from uni_tracker.services.briefs import build_deterministic_backfill_payload, upsert_item_brief


class LLMUnavailable(RuntimeError):
    pass


SUMMARY_FINAL_STATUSES = {"completed", "rejected"}


def build_nvidia_client(*, timeout_seconds: float = 60.0) -> httpx.Client:
    settings = get_settings()
    if not settings.nvidia_api_key:
        raise LLMUnavailable("NVIDIA_API_KEY is not configured.")
    return httpx.Client(
        timeout=timeout_seconds,
        headers={
            "Authorization": f"Bearer {settings.nvidia_api_key}",
            "Accept": "application/json",
        },
    )


def enrich_recent_items(session: Session, limit: int = 10) -> dict[str, int]:
    settings = get_settings()
    if not settings.enable_llm or not settings.nvidia_api_key:
        return {"processed": 0, "skipped": 0}

    items = session.scalars(
        select(NormalizedItem)
        .where(
            NormalizedItem.body_text.is_not(None),
            NormalizedItem.review_status.in_(["needs_review", "watch"]),
        )
        .order_by(NormalizedItem.updated_at.desc())
        .limit(limit)
    ).all()
    if not items:
        return {"processed": 0, "skipped": 0}

    processed = 0
    with build_nvidia_client() as client:
        for item in items:
            outcome = _process_item_brief(
                session=session,
                client=client,
                item=item,
                settings=settings,
                force=False,
                origin="stored",
            )
            if outcome == "processed":
                processed += 1
    return {"processed": processed, "skipped": max(len(items) - processed, 0)}


def backfill_item_briefs(session: Session, items: list[NormalizedItem], *, force: bool = True) -> dict[str, int]:
    settings = get_settings()
    if not settings.enable_llm or not settings.nvidia_api_key:
        return {"processed": 0, "skipped": len(items)}
    if not items:
        return {"processed": 0, "skipped": 0}

    processed = 0
    with build_nvidia_client(timeout_seconds=180.0) as client:
        for item in items:
            outcome = _process_item_brief(
                session=session,
                client=client,
                item=item,
                settings=settings,
                force=force,
                origin="backfill",
            )
            if outcome == "processed":
                processed += 1
            elif outcome in {"rejected", "failed"}:
                payload = build_deterministic_backfill_payload(session, item)
                job = LLMJob(
                    normalized_item_id=item.id,
                    raw_artifact_id=None,
                    job_type="summary",
                    provider="deterministic",
                    model="deterministic_backfill",
                    status="completed",
                    request_payload={"source": "deterministic_backfill"},
                    response_payload={"source": "deterministic_backfill"},
                    output_text=payload["summary_short"],
                    finished_at=datetime.now(UTC),
                )
                session.add(job)
                session.flush()
                upsert_item_brief(
                    session,
                    item=item,
                    payload=payload,
                    model="deterministic_backfill",
                    llm_job_id=job.id,
                    source_artifact_id=None,
                    origin="backfill",
                )
                processed += 1
    return {"processed": processed, "skipped": max(len(items) - processed, 0)}


JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def _build_brief_prompt(item: NormalizedItem, truncated_body: str) -> str:
    document_kind = _infer_document_kind(item)
    if document_kind == "syllabus":
        extra_guidance = (
            "This is a syllabus / programmatic document. Focus on units, evaluation, deadlines, attendance rules, and any date-sensitive obligations.\n"
            "Do not echo the title; summarize the content.\n"
            "Prefer 3 to 5 bullets with concrete facts.\n"
        )
    elif document_kind == "announcement":
        extra_guidance = (
            "This is an announcement or forum post. Focus on what changed, who is affected, and what action is required.\n"
            "Do not echo the title; summarize the change.\n"
        )
    elif document_kind == "assignment":
        extra_guidance = (
            "This is an assignment or quiz item. Focus on due dates, requirements, deliverables, grading notes, and deadlines.\n"
            "Do not echo the title; summarize the obligations.\n"
        )
    else:
        extra_guidance = (
            "Summarize the academic content compactly. If the text contains dates, deadlines, obligations, or exam-related information, extract them explicitly.\n"
            "Do not echo the title; summarize the substance.\n"
        )

    return (
        "You are compressing Moodle content into a compact agent brief.\n"
        "Return JSON only with keys summary_short, summary_bullets, key_dates, key_requirements, risk_flags, course_context, confidence, source_refs.\n"
        "summary_short must be a short Spanish summary with real content, not just the filename or title.\n"
        "summary_bullets must be a short array of Spanish bullet fragments with at least 2 non-empty bullets when the document is substantive.\n"
        "key_dates must be an array of objects with keys type, iso_datetime, matched_text.\n"
        "key_requirements must be an array of short strings.\n"
        "risk_flags must be an array of short strings.\n"
        "course_context must be a small object describing the course/item context.\n"
        "source_refs must be an array of compact references to the source.\n"
        "Use null iso_datetime if ambiguous.\n"
        f"document_kind: {document_kind}\n\n"
        f"{extra_guidance}"
        f"Title: {item.title}\n"
        f"Body:\n{truncated_body}\n"
    )


def _process_item_brief(
    *,
    session: Session,
    client: httpx.Client,
    item: NormalizedItem,
    settings,
    force: bool,
    origin: str,
) -> str:
    now = datetime.now(UTC)
    if not force and (item.brief is not None or _has_final_summary_job(item) or _failed_recently(item, settings, now)):
        return "skipped"

    truncated_body = _truncate_body(item.body_text or "", settings.llm_body_char_limit)
    prompt = _build_brief_prompt(item, truncated_body)
    job = LLMJob(
        normalized_item_id=item.id,
        raw_artifact_id=None,
        job_type="summary",
        provider="nvidia",
        model=settings.nvidia_model,
        status="running",
        request_payload={"prompt": prompt},
    )
    session.add(job)
    session.flush()
    try:
        response, attempts = _post_with_retries(
            client=client,
            settings=settings,
            url=settings.nvidia_api_url,
            request_json={
                "model": settings.nvidia_model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1200,
                "temperature": 0.2,
                "top_p": 1.0,
                "stream": False,
                "chat_template_kwargs": {"thinking": True},
            },
        )
        response.raise_for_status()
        payload = response.json()
        message = _normalize_message_content(payload["choices"][0]["message"].get("content"))
        parsed = _parse_llm_payload(message)
        brief_payload = _build_brief_payload(item, parsed, message)
        quality_error = _validate_brief_payload(item, brief_payload)
        job.response_payload = payload
        job.output_text = message
        job.finished_at = datetime.now(UTC)
        job.request_payload = {
            "prompt": prompt,
            "attempts": attempts,
        }
        if quality_error is not None:
            job.status = "rejected"
            job.error_text = quality_error
            session.flush()
            return "rejected"
        job.status = "completed"
        session.add(
            ItemFact(
                normalized_item_id=item.id,
                source_artifact_id=None,
                fact_type="llm_summary",
                value_json={"summary": brief_payload["summary_short"]},
                confidence=0.5,
                extractor_type="llm_kimi_k2_5",
                source_span=item.title,
            )
        )
        for date_fact in parsed.get("key_dates") or parsed.get("dates") or []:
            iso_datetime = date_fact.get("iso_datetime")
            if not iso_datetime:
                continue
            session.add(
                ItemFact(
                    normalized_item_id=item.id,
                    source_artifact_id=None,
                    fact_type=date_fact.get("type") or "date_mention",
                    value_json={
                        "value": iso_datetime,
                        "matched_text": date_fact.get("matched_text"),
                    },
                    confidence=0.45,
                    extractor_type="llm_kimi_k2_5",
                    source_span=date_fact.get("matched_text"),
                )
            )
        risk_flags = parsed.get("risk_flags") or parsed.get("urgent_signals") or []
        if risk_flags:
            session.add(
                ItemFact(
                    normalized_item_id=item.id,
                    source_artifact_id=None,
                    fact_type="llm_urgent_signals",
                    value_json={"signals": risk_flags},
                    confidence=0.45,
                    extractor_type="llm_kimi_k2_5",
                    source_span=item.title,
                )
            )
        upsert_item_brief(
            session,
            item=item,
            payload=brief_payload,
            model=settings.nvidia_model,
            llm_job_id=job.id,
            source_artifact_id=None,
            origin=origin,
        )
        return "processed"
    except Exception as exc:
        job.status = "failed"
        job.error_text = str(exc)
        job.finished_at = datetime.now(UTC)
        attempts = _coerce_attempt_count(job.request_payload)
        job.request_payload = {
            "prompt": prompt,
            "attempts": attempts,
        }
        return "failed"


def _post_with_retries(
    *,
    client: httpx.Client,
    settings,
    url: str,
    request_json: dict[str, Any],
) -> tuple[httpx.Response, int]:
    max_attempts = max(int(getattr(settings, "llm_request_max_attempts", 3) or 3), 1)
    base_delay = float(getattr(settings, "llm_retry_base_delay_seconds", 2.0) or 2.0)
    max_delay = float(getattr(settings, "llm_retry_max_delay_seconds", 30.0) or 30.0)
    attempt = 0
    delay_seconds = max(base_delay, 0.0)
    last_error: Exception | None = None

    while attempt < max_attempts:
        attempt += 1
        try:
            response = client.post(url, json=request_json)
            if _is_retryable_response(response) and attempt < max_attempts:
                last_error = httpx.HTTPStatusError(
                    f"Retryable NVIDIA status: {response.status_code}",
                    request=response.request,
                    response=response,
                )
                _sleep_for_retry(response=response, delay_seconds=delay_seconds)
                delay_seconds = min(delay_seconds * 2 if delay_seconds else base_delay, max_delay)
                continue
            return response, attempt
        except httpx.TransportError as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            time.sleep(delay_seconds)
            delay_seconds = min(delay_seconds * 2 if delay_seconds else base_delay, max_delay)

    if last_error is not None:
        raise last_error
    raise RuntimeError("LLM request failed without response")


def _validate_brief_payload(item: NormalizedItem, payload: dict[str, Any]) -> str | None:
    summary_short = _coerce_text(payload.get("summary_short"))
    title = _coerce_text(item.title)
    bullets = [bullet for bullet in _coerce_string_list(payload.get("summary_bullets")) if bullet]
    key_dates = payload.get("key_dates") or []
    key_requirements = _coerce_string_list(payload.get("key_requirements"))
    risk_flags = _coerce_string_list(payload.get("risk_flags"))
    document_kind = _infer_document_kind(item)

    if not summary_short:
        return "brief_quality_rejected: missing summary_short"
    if summary_short.casefold() == title.casefold():
        return "brief_quality_rejected: summary echoes title"
    if document_kind in {"syllabus", "assignment"} and len(bullets) < 2:
        return "brief_quality_rejected: insufficient bullet detail"
    if document_kind == "syllabus" and not (key_dates or key_requirements or risk_flags):
        return "brief_quality_rejected: syllabus lacked extracted dates or requirements"
    return None


def _infer_document_kind(item: NormalizedItem) -> str:
    title = (item.title or "").casefold()
    review_reason = (item.review_reason or "").casefold()
    if "programa anal" in title or "syllabus" in title or "program" in title:
        return "syllabus"
    if "high_risk_schedule_document" in review_reason:
        return "syllabus"
    if item.item_type in {"assignment", "quiz"}:
        return "assignment"
    if item.item_type in {"forum", "forum_discussion", "announcement"}:
        return "announcement"
    return "material"


def _has_final_summary_job(item: NormalizedItem) -> bool:
    return any(job.job_type == "summary" and job.status in SUMMARY_FINAL_STATUSES for job in item.llm_jobs)


def _failed_recently(item: NormalizedItem, settings, now: datetime) -> bool:
    cooldown_minutes = int(getattr(settings, "llm_retry_cooldown_minutes", 180) or 180)
    if cooldown_minutes <= 0:
        return False
    latest_failed = max(
        (
            job
            for job in item.llm_jobs
            if job.job_type == "summary" and job.status == "failed"
        ),
        key=lambda job: job.finished_at or job.created_at,
        default=None,
    )
    if latest_failed is None:
        return False
    last_attempt_at = latest_failed.finished_at or latest_failed.created_at
    if last_attempt_at is None:
        return False
    return (now - last_attempt_at.astimezone(UTC)).total_seconds() < cooldown_minutes * 60


def _parse_llm_payload(message: str) -> dict[str, Any]:
    match = JSON_BLOCK_RE.search(message)
    if not match:
        return {"summary": message, "dates": [], "urgent_signals": []}
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {"summary": message, "dates": [], "urgent_signals": []}
    if not isinstance(parsed, dict):
        return {"summary": message, "dates": [], "urgent_signals": []}
    parsed.setdefault("summary", message)
    parsed.setdefault("dates", [])
    parsed.setdefault("urgent_signals", [])
    return parsed


def _is_retryable_response(response: httpx.Response) -> bool:
    return int(getattr(response, "status_code", 200) or 200) in {408, 409, 425, 429, 500, 502, 503, 504}


def _sleep_for_retry(*, response: httpx.Response, delay_seconds: float) -> None:
    headers = getattr(response, "headers", {}) or {}
    retry_after = headers.get("Retry-After")
    if retry_after:
        try:
            time.sleep(max(float(retry_after), 0.0))
            return
        except ValueError:
            pass
    time.sleep(max(delay_seconds, 0.0))


def _coerce_attempt_count(request_payload: dict[str, Any] | None) -> int:
    if not request_payload:
        return 1
    attempts = request_payload.get("attempts")
    try:
        return max(int(attempts), 1)
    except (TypeError, ValueError):
        return 1


def _build_brief_payload(item: NormalizedItem, parsed: dict[str, Any], message: str) -> dict[str, Any]:
    summary_short = parsed.get("summary_short") or parsed.get("summary") or message or item.title
    summary_bullets = parsed.get("summary_bullets") or []
    if isinstance(summary_bullets, str):
        summary_bullets = [summary_bullets]
    key_dates = parsed.get("key_dates") or parsed.get("dates") or []
    key_requirements = parsed.get("key_requirements") or []
    risk_flags = parsed.get("risk_flags") or parsed.get("urgent_signals") or []
    course_context = parsed.get("course_context") or {}
    if not isinstance(course_context, dict):
        course_context = {}
    source_refs = parsed.get("source_refs") or []
    if not isinstance(source_refs, list):
        source_refs = []
    if not source_refs:
        source_refs = [
            {
                "type": "item",
                "item_id": item.id,
                "title": item.title,
                "url": item.primary_url,
            }
        ]
    confidence = parsed.get("confidence")
    try:
        confidence_value = float(confidence) if confidence is not None else 0.45
    except (TypeError, ValueError):
        confidence_value = 0.45
    return {
        "summary_short": summary_short,
        "summary_bullets": summary_bullets,
        "key_dates": key_dates,
        "key_requirements": key_requirements,
        "risk_flags": risk_flags,
        "course_context": course_context,
        "confidence": confidence_value,
        "source_refs": source_refs,
    }


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _coerce_string_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            if isinstance(item, str):
                text = item.strip()
            else:
                text = str(item).strip()
            if text:
                result.append(text)
        return result
    text = str(value).strip()
    return [text] if text else []


def _truncate_body(body: str, limit: int) -> str:
    if len(body) <= limit:
        return body
    truncated = body[:limit].rsplit(" ", 1)[0]
    return truncated + "\n\n[truncated_for_llm]"


def _normalize_message_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if content is None:
        return ""
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text_value = item.get("text") or item.get("content")
                if isinstance(text_value, str):
                    parts.append(text_value)
        return "\n".join(part for part in parts if part)
    if isinstance(content, dict):
        text_value = content.get("text") or content.get("content")
        if isinstance(text_value, str):
            return text_value
    return str(content)
