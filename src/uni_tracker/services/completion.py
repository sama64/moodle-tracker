from __future__ import annotations

from sqlalchemy.orm import Session

from uni_tracker.models import NormalizedItem

COMPLETION_STATE_UNKNOWN = "unknown"
COMPLETION_STATE_INCOMPLETE = "incomplete"
COMPLETION_STATE_COMPLETED = "completed"
VALID_COMPLETION_STATES = {
    COMPLETION_STATE_UNKNOWN,
    COMPLETION_STATE_INCOMPLETE,
    COMPLETION_STATE_COMPLETED,
}


def effective_completion_state(item: NormalizedItem) -> str:
    override_state = item.completion_override_state
    if override_state in VALID_COMPLETION_STATES:
        return override_state
    if item.completion_state in VALID_COMPLETION_STATES:
        return item.completion_state
    return COMPLETION_STATE_UNKNOWN


def is_completed(item: NormalizedItem) -> bool:
    return effective_completion_state(item) == COMPLETION_STATE_COMPLETED


def set_completion_override(
    session: Session,
    item_id: int,
    *,
    override_state: str | None,
) -> NormalizedItem | None:
    item = session.get(NormalizedItem, item_id)
    if item is None:
        return None
    if override_state is not None and override_state not in VALID_COMPLETION_STATES:
        raise ValueError(f"Unsupported completion override state: {override_state}")
    item.completion_override_state = override_state
    session.flush()
    return item
