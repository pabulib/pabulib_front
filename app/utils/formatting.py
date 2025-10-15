from __future__ import annotations

from typing import Any


def format_short_number(n: float) -> str:
    """Format large numbers into a short human-readable string (e.g., 1.2K, 3.4M).
    Uses base 1000 units and rounds to one decimal when needed.
    """
    try:
        num = float(n)
    except Exception:
        return "—"
    neg = num < 0
    num = abs(num)
    units = ["", "K", "M", "B", "T", "Q"]
    i = 0
    while num >= 1000 and i < len(units) - 1:
        num /= 1000.0
        i += 1
    if num >= 100 or abs(num - round(num)) < 1e-6:
        s = f"{int(round(num))}{units[i]}"
    else:
        s = f"{num:.1f}{units[i]}"
    return f"-{s}" if neg else s


def format_int(num: int) -> str:
    return f"{num:,}".replace(",", " ")


def format_budget(currency: str, amount: float | int) -> str:
    """Format budget amount with proper number formatting.

    Args:
        currency: Currency string (e.g., 'PLN', 'USD')
        amount: Budget amount as float or int

    Returns:
        Formatted budget string with currency (uses spaces as thousand separators)
    """
    # If it's a float with decimal places, show them; otherwise format as int
    if isinstance(amount, float) and amount % 1 != 0:
        # Has decimal places - format with 2 decimal places and space as thousand separator
        formatted = f"{amount:,.2f}".replace(",", " ")
    else:
        # No decimal places or is int - format as integer
        formatted = format_int(int(amount))

    return f"{formatted} {currency}" if currency else formatted


def format_vote_length(val: Any) -> str:
    if val is None:
        return "—"
    try:
        return f"{float(val):.3f}"
    except Exception:
        return "—"
