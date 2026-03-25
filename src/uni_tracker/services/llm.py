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
            if any(job.job_type == "summary" and job.status == "completed" for job in item.llm_jobs):
                continue
            truncated_body = _truncate_body(item.body_text or "", settings.llm_body_char_limit)
            prompt = (
                "You are extracting academic risk information from Moodle material.\n"
                "Return JSON only with keys summary, dates, urgent_signals.\n"
                "summary must be a short Spanish summary.\n"
                "dates must be an array of objects with keys type, iso_datetime, matched_text.\n"
                "Use null iso_datetime if ambiguous.\n"
                "urgent_signals must be an array of short strings.\n\n"
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
                summary = parsed.get("summary") or message
                session.add(
                    ItemFact(
                        normalized_item_id=item.id,
                        source_artifact_id=None,
                        fact_type="llm_summary",
                        value_json={"summary": summary},
                        confidence=0.5,
                        extractor_type="llm_kimi_k2_5",
                        source_span=item.title,
                    )
                )
                for date_fact in parsed.get("dates", []):
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
                if parsed.get("urgent_signals"):
                    session.add(
                        ItemFact(
                            normalized_item_id=item.id,
                            source_artifact_id=None,
                            fact_type="llm_urgent_signals",
                            value_json={"signals": parsed["urgent_signals"]},
                            confidence=0.45,
                            extractor_type="llm_kimi_k2_5",
                            source_span=item.title,
                        )
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
