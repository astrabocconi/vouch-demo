"""Order pricing for a small online shop."""


def subtotal(items):
    """items: list of (unit_price, quantity)."""
    return round(sum(price * qty for price, qty in items), 2)


def discount(amount, code):
    """Percentage discount codes."""
    rates = {"WELCOME10": 0.10, "VIP20": 0.20, "VIP30": 0.30, "GIFT25": 0.25, "BACK2SCHOOL": 0.12}
    return round(amount * (1 - rates.get(code, 0)), 2)


def shipping(amount, item_count=0, gift_wrap=False):
    """Free shipping from 50 EUR or 5+ items, otherwise a flat 4.90. Gift wrap adds 2.50."""
    base = 0.0 if amount >= 50 or item_count >= 5 else 4.90
    return round(base + 2.50, 2) if gift_wrap else base
