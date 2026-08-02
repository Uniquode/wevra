"""Provider-private Redis ordering helpers for paged schedules."""

from __future__ import annotations


def encode_due_order(value: float) -> str:
    """Encode a finite due time into fixed-width lexicographic numeric order.

    This must remain semantically identical to ``SCHEDULE_DUE_ORDER_FUNCTION``.
    ``due_member`` separates this fixed-width value from the opaque identity with
    its first colon, so schedule identities may contain further colons.
    """
    value = 0.0 if value == 0 else value
    if value == 0:
        return "1000" + "0" * 18
    mantissa, exponent = format(abs(value), ".17e").split("e")
    digits = mantissa.replace(".", "")
    exponent_code = int(exponent) + 500
    if value > 0:
        return f"1{exponent_code:03d}{digits}"
    inverted_digits = "".join(str(9 - int(digit)) for digit in digits)
    return f"0{999 - exponent_code:03d}{inverted_digits}"


def due_member(next_due_at: float, identity: str) -> str:
    """Return the lexicographic due-index member for one schedule identity."""
    return f"{encode_due_order(next_due_at)}:{identity}"


def due_boundary_member(before: float) -> str:
    """Return an inclusive lexicographic boundary for a due-time query."""
    return f"{encode_due_order(before)};"


__all__ = ("due_boundary_member", "due_member", "encode_due_order")
