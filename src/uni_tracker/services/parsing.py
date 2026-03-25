from __future__ import annotations

import html
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from pypdf import PdfReader


HTML_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
NUMERIC_DATE_RE = re.compile(
    r"(?P<day>\d{1,2})[/-](?P<month>\d{1,2})(?:[/-](?P<year>\d{2,4}))?(?:\s+(?P<hour>\d{1,2})[:.](?P<minute>\d{2}))?",
    re.IGNORECASE,
)
MONTH_NAMES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}
TEXT_DATE_RE = re.compile(
    r"(?P<day>\d{1,2})\s+de\s+(?P<month>"
    + "|".join(MONTH_NAMES)
    + r")(?:\s+de\s+(?P<year>\d{4}))?(?:\s+a\s+las\s+(?P<hour>\d{1,2})[:.](?P<minute>\d{2}))?",
    re.IGNORECASE,
)


@dataclass
class ExtractedFact:
    fact_type: str
    value: dict[str, Any]
    confidence: float
    extractor_type: str
    source_span: str


def strip_html(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = HTML_RE.sub(" ", value)
    return normalize_text(html.unescape(cleaned)) or None


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return WS_RE.sub(" ", value.replace("\x00", " ")).strip()


def _coerce_year(year: str | None, reference_year: int) -> int:
    if year is None:
        return reference_year
    parsed = int(year)
    if parsed < 100:
        return 2000 + parsed
    return parsed


def extract_text_from_pdf(content: bytes) -> str:
    reader = PdfReader(BytesIO(content))
    chunks = []
    for page in reader.pages:
        chunks.append(page.extract_text() or "")
    return normalize_text("\n".join(chunks))


def extract_text_from_docx(content: bytes) -> str:
    with zipfile.ZipFile(BytesIO(content)) as archive:
        xml_bytes = archive.read("word/document.xml")
    root = ElementTree.fromstring(xml_bytes)
    texts = [node.text or "" for node in root.iter() if node.text]
    return normalize_text(" ".join(texts))


def extract_text_from_html(content: bytes) -> str:
    return strip_html(content.decode("utf-8", errors="replace")) or ""


def extract_text_for_file(filename: str, mime_type: str, content: bytes) -> tuple[str | None, str]:
    lower = filename.lower()
    try:
        if mime_type == "application/pdf" or lower.endswith(".pdf"):
            return extract_text_from_pdf(content), "pdf"
        if (
            mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            or lower.endswith(".docx")
        ):
            return extract_text_from_docx(content), "docx"
        if mime_type in {"text/html", "text/plain"} or lower.endswith((".html", ".htm", ".txt")):
            return extract_text_from_html(content), "html_or_text"
    except Exception:
        return None, "failed"
    return None, "unsupported"


def _fact_type_for_context(context: str) -> str:
    lowered = context.lower()
    if any(word in lowered for word in ["entrega", "vence", "cierra", "deadline", "due"]):
        return "due_at"
    if any(word in lowered for word in ["parcial", "recuperatorio", "examen", "quiz"]):
        return "exam_at"
    if any(word in lowered for word in ["clase", "cronograma", "horario", "encuentro"]):
        return "class_session_at"
    return "date_mention"


def extract_date_facts_from_text(text: str, *, reference_year: int | None = None) -> list[ExtractedFact]:
    if not text:
        return []
    if reference_year is None:
        reference_year = datetime.now(UTC).year

    facts: list[ExtractedFact] = []
    for pattern in (NUMERIC_DATE_RE, TEXT_DATE_RE):
        for match in pattern.finditer(text):
            month_text = match.groupdict().get("month")
            if month_text and not month_text.isdigit():
                month = MONTH_NAMES[month_text.lower()]
            else:
                month = int(match.group("month"))
            day = int(match.group("day"))
            year = _coerce_year(match.groupdict().get("year"), reference_year)
            hour = int(match.groupdict().get("hour") or 0)
            minute = int(match.groupdict().get("minute") or 0)
            try:
                parsed = datetime(year, month, day, hour, minute, tzinfo=UTC)
            except ValueError:
                continue

            start = max(match.start() - 40, 0)
            end = min(match.end() + 40, len(text))
            span_text = text[start:end]
            facts.append(
                ExtractedFact(
                    fact_type=_fact_type_for_context(span_text),
                    value={"value": parsed.isoformat(), "matched_text": match.group(0)},
                    confidence=0.7,
                    extractor_type="deterministic_text_dates",
                    source_span=match.group(0),
                )
            )
    return facts


def derive_review_status(filename: str, extracted_text: str | None) -> tuple[str, str | None]:
    lower = filename.lower()
    if extracted_text is None:
        return "needs_review", "text_extraction_failed"
    if len(extracted_text.strip()) < 40:
        return "needs_review", "low_text_density"
    if any(keyword in lower for keyword in ["cronograma", "horario", "calendario", "programa", "fechas"]):
        return "watch", "high_risk_schedule_document"
    return "none", None


def safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_") or "artifact"
