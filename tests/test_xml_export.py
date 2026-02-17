"""Tests for XML export module."""
import pytest
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

from src.xml_export import encode_namedata, decode_namedata, generate_xml


class TestEncodeNamedata:

    def test_simple_text(self):
        result = encode_namedata("Aktiva")
        assert result == "417:20:%253B3:Aktiva;"

    def test_umlauts(self):
        result = encode_namedata("Rückstellungen")
        assert "252" in result  # ü encoded
        assert result.endswith(";")

    def test_ampersand(self):
        result = encode_namedata("Werte & Lizenzen")
        assert "&amp;" in result

    def test_roundtrip(self):
        """encode → decode should return original text."""
        original = "Löhne und Gehälter"
        encoded = encode_namedata(original)
        decoded = decode_namedata(encoded)
        assert decoded == original


class TestGenerateXml:

    def test_generates_valid_xml(self, tmp_path):
        """Generate XML from mock data and verify it's well-formed."""
        df = pd.DataFrame([
            {"konto_nr": "1000", "konto_name": "Kasse", "row_type": "ACCOUNT",
             "target_overpos_id": "t1", "target_overpos_name": "Kassenbestand",
             "target_class": "AKTIVA", "amount_normalized": 5000},
            {"konto_nr": "8000", "konto_name": "Umsatzerlöse", "row_type": "ACCOUNT",
             "target_overpos_id": "t2", "target_overpos_name": "Umsatzerlöse",
             "target_class": "ERTRAG", "amount_normalized": 100000},
            {"konto_nr": "9999", "konto_name": "Summe", "row_type": "TOTAL",
             "target_overpos_id": "", "target_overpos_name": "",
             "target_class": "", "amount_normalized": 0},
        ])

        out = tmp_path / "test.xml"
        result = generate_xml(df, output_path=out)

        assert result.exists()
        # Parse to verify well-formedness
        tree = ET.parse(str(result))
        root = tree.getroot()
        assert root.tag == "AccountFramework"

    def test_template_attrs(self, tmp_path):
        """If template provided, root attrs should be preserved."""
        # Use one of the example templates if available
        template = Path(__file__).parent.parent / "Examples" / "Lucanet Einlesen Automation" / "Kontenrahmen_Beispiele" / "DATEV SKR 03 BilMoG.xml"

        df = pd.DataFrame([
            {"konto_nr": "1000", "konto_name": "Kasse", "row_type": "ACCOUNT",
             "target_overpos_id": "t1", "target_overpos_name": "Kassenbestand",
             "target_class": "AKTIVA", "amount_normalized": 5000},
        ])

        out = tmp_path / "test2.xml"
        if template.exists():
            result = generate_xml(df, template_xml_path=template, output_path=out)
            tree = ET.parse(str(result))
            root = tree.getroot()
            assert root.get("versionID") == "1.0"
            assert root.get("financialPositionModelOID") == "1700"
