from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from uni_tracker.config import get_settings
from uni_tracker.models import ItemFact, LLMJob, NormalizedItem
from uni_tracker.services.briefs import upsert_item_brief


class LLMUnavailable(RuntimeError):
    pass


def build_nvidia_client() -> httpx.Client:
    settings = get_settings()
    if not settings.nvidia_api_key:
        raise LLMUnavailable("NVIDIA_API_KEY is not configured.")
    return httpx.Client(
        timeout=60.0,
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
            if item.brief is not None:
                continue
            truncated_body = _truncate_body(item.body_text or "", settings.llm_body_char_limit)
            prompt = (
                "You are compressing Moodle content into a compact agent brief.\n"
                "Return JSON only with keys summary_short, summary_bullets, key_dates, key_requirements, risk_flags, course_context, confidence, source_refs.\n"
                "summary_short must be a short Spanish summary.\n"
                "summary_bullets must be a short array of Spanish bullet fragments.\n"
                "key_dates must be an array of objects with keys type, iso_datetime, matched_text.\n"
                "key_requirements must be an array of short strings.\n"
                "risk_flags must be an array of short strings.\n"
                "course_context must be a small object describing the course/item context.\n"
                "source_refs must be an array of compact references to the source.\n"
                "Use null iso_datetime if ambiguous.\n\n"
                f"Title: {item.title}\n"
                f"Body:\n{truncated_body}\n"
            )
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
                response = client.post(
                    settings.nvidia_api_url,
                    json={
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
                job.status = "completed"
                job.response_payload = payload
                job.output_text = message
                job.finished_at = datetime.now(UTC)
                brief_payload = _build_brief_payload(item, parsed, message)
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
                )
                processed += 1
            except Exception as exc:
                job.status = "failed"
                job.error_text = str(exc)
                job.finished_at = datetime.now(UTC)
    return {"processed": processed, "skipped": max(len(items) - processed, 0)}


JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


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
