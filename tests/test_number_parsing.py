"""Tests for number parsing (io_readers.parse_number)."""
import pytest
from src.io_readers import parse_number


class TestParseNumber:
    """Various locale and format tests for parse_number."""

    # German format
    def test_german_simple(self):
        assert parse_number("1.234,56") == 1234.56

    def test_german_no_thousands(self):
        assert parse_number("1234,56") == 1234.56

    def test_german_large(self):
        assert parse_number("1.234.567,89") == 1234567.89

    # English format
    def test_english_simple(self):
        assert parse_number("1,234.56", "en") == 1234.56

    def test_english_no_thousands(self):
        assert parse_number("1234.56", "en") == 1234.56

    # Negative numbers
    def test_negative_dash(self):
        assert parse_number("-1.234,56") == -1234.56

    def test_negative_parentheses(self):
        assert parse_number("(1.234,56)") == -1234.56

    # Currency symbols
    def test_euro(self):
        assert parse_number("€ 1.234,56") == 1234.56

    def test_dollar(self):
        assert parse_number("$1,234.56", "en") == 1234.56

    # Edge cases
    def test_none(self):
        assert parse_number(None) is None

    def test_empty(self):
        assert parse_number("") is None

    def test_whitespace(self):
        assert parse_number("  ") is None

    def test_integer(self):
        assert parse_number("42") == 42.0

    def test_float_passthrough(self):
        assert parse_number(3.14) == 3.14

    # Apostrophe thousand separator
    def test_apostrophe(self):
        assert parse_number("1'234.56", "en") == 1234.56

    # Zero
    def test_zero(self):
        assert parse_number("0") == 0.0

    def test_zero_comma(self):
        assert parse_number("0,00") == 0.0
