import pytest
from src.scraper import TelegramScraper

scraper = TelegramScraper()

def test_contains_price():
    assert scraper._contains_price("Price: 100 ETB")
    assert scraper._contains_price("Only 50 birr!")
    assert not scraper._contains_price("No price info here")

def test_contains_contact():
    assert scraper._contains_contact("Call 0912345678")
    assert scraper._contains_contact("Contact: +251912345678")
    assert scraper._contains_contact("Telegram: @user")
    assert not scraper._contains_contact("No contact info")
