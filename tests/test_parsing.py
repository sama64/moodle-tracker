from uni_tracker.services.parsing import extract_date_facts_from_text, normalize_text


def test_normalize_text_removes_null_bytes() -> None:
    assert normalize_text("hola\x00 mundo") == "hola mundo"


def test_extract_date_facts_from_text_finds_due_date() -> None:
    facts = extract_date_facts_from_text("La entrega vence el 12/04/2026 23:59 hs")
    assert any(fact.fact_type == "due_at" for fact in facts)
