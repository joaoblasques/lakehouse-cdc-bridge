"""Field validation shared by the generator and the file-share adapter."""

from decimal import Decimal, InvalidOperation


def iban_is_valid(iban: str) -> bool:
    """ISO 13616 mod-97 check."""
    if len(iban) < 15 or not iban[:2].isalpha() or not iban.isalnum():
        return False
    rearranged = iban[4:] + iban[:4]
    digits = "".join(str(int(ch, 36)) for ch in rearranged)
    return int(digits) % 97 == 1


def parse_amount(raw: str) -> Decimal | None:
    try:
        value = Decimal(raw)
    except (InvalidOperation, TypeError):
        return None
    if not value.is_finite() or value <= 0 or value.as_tuple().exponent < -2:
        return None
    return value
