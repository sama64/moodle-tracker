from uni_tracker.services.parsing import extract_date_facts_from_text, normalize_text


def test_normalize_text_removes_null_bytes() -> None:
    assert normalize_text("hola\x00 mundo") == "hola mundo"


def test_extract_date_facts_from_text_finds_due_date() -> None:
    facts = extract_date_facts_from_text("La entrega vence el 12/04/2026 23:59 hs")
    assert any(fact.fact_type == "due_at" for fact in facts)


def test_extract_date_facts_from_text_classifies_schedule_exam_lines() -> None:
    facts = extract_date_facts_from_text(
        "07/04 PARCIAL 1 08/05 Primer Parcial 17/06 Segundo Parcial",
        reference_year=2026,
    )

    assert [fact.fact_type for fact in facts] == ["exam_at", "exam_at", "exam_at"]
