import pytest
import re

def _contains_price(text: str) -> bool:
    price_patterns = [r'\bETB\b', r'\bbirr\b', r'\bUSD\b', r'\$', r'\bህ\b']
    if any(re.search(pattern, text, re.IGNORECASE) for pattern in price_patterns):
        return True
    if re.search(r'price\s*[:=]?\s*\d+', text, re.IGNORECASE):
        return True
    return False

def _contains_contact(text: str) -> bool:
    contact_patterns = ['09', '+251', '@', 'telegram', 'call', 'ጥያቄ']
    return any(pattern.lower() in text.lower() for pattern in contact_patterns)

def test_contains_price():
    assert _contains_price("Price: 100 ETB")
    assert _contains_price("Only 50 birr!")
    assert not _contains_price("No price info here")

def test_contains_contact():
    assert _contains_contact("Call 0912345678")
    assert _contains_contact("Contact: +251912345678")
    assert _contains_contact("Telegram: @user")
    assert not _contains_contact("No contact info")
