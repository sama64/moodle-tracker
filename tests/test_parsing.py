from __future__ import annotations

import subprocess
import zipfile
from io import BytesIO

from uni_tracker.services import parsing
from uni_tracker.services.parsing import extract_date_facts_from_text, normalize_text


def test_normalize_text_removes_null_bytes() -> None:
    assert normalize_text("hola\x00 mundo") == "hola mundo"


def test_pdf_extraction_timeout_returns_failed_timeout(monkeypatch) -> None:
    def timeout_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs.get("timeout"))

    monkeypatch.setattr(parsing.subprocess, "run", timeout_run)

    text, mode = parsing.extract_text_for_file(
        "slow.pdf",
        "application/pdf",
        b"%PDF-1.4 fake",
        pdf_timeout_seconds=0.05,
    )

    assert text is None
    assert mode == "failed_timeout"


def test_extract_date_facts_from_text_finds_due_date() -> None:
    facts = extract_date_facts_from_text("La entrega vence el 12/04/2026 23:59 hs")
    assert any(fact.fact_type == "due_at" for fact in facts)


def test_extract_date_facts_from_text_classifies_schedule_exam_lines() -> None:
    facts = extract_date_facts_from_text(
        "07/04 PARCIAL 1 08/05 Primer Parcial 17/06 Segundo Parcial",
        reference_year=2026,
    )

    assert [fact.fact_type for fact in facts] == ["exam_at", "exam_at", "exam_at"]


def test_pdf_extraction_success_still_returns_pdf_mode(monkeypatch) -> None:
    def successful_run(*args, **kwargs):
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout=b"hello\nworld", stderr=b"")

    monkeypatch.setattr(parsing.subprocess, "run", successful_run)

    text, mode = parsing.extract_text_for_file(
        "ok.pdf",
        "application/pdf",
        b"%PDF-1.4 fake",
        pdf_timeout_seconds=1,
    )

    assert text == "hello world"
    assert mode == "pdf"


def test_pdf_extraction_subprocess_failure_returns_failed(monkeypatch) -> None:
    def failed_run(*args, **kwargs):
        return subprocess.CompletedProcess(args=args[0], returncode=1, stdout=b"", stderr=b"boom")

    monkeypatch.setattr(parsing.subprocess, "run", failed_run)

    text, mode = parsing.extract_text_for_file("bad.pdf", "application/pdf", b"not really pdf")

    assert text is None
    assert mode == "failed"


def test_docx_extraction_success_returns_docx_mode() -> None:
    document_xml = """
    <w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
      <w:body>
        <w:p><w:r><w:t>Cronograma</w:t></w:r></w:p>
        <w:p><w:r><w:t>Primer parcial 15/05</w:t></w:r></w:p>
      </w:body>
    </w:document>
    """.encode("utf-8")
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("word/document.xml", document_xml)

    text, mode = parsing.extract_text_for_file(
        "cronograma.docx",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        buffer.getvalue(),
    )

    assert mode == "docx"
    assert text == "Cronograma Primer parcial 15/05"
