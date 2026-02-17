"""
Phase 7 — XML Export: Generate LucaNet AccountFramework XML.

Standalone module with no LLM dependency.
"""
from __future__ import annotations

import re
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

import pandas as pd

# ---------------------------------------------------------------------------
# Name encoding (matches LucaNet template pattern)
# ---------------------------------------------------------------------------

def encode_namedata(text: str) -> str:
    """Encode text for the nameData attribute, matching LucaNet XML conventions.

    Pattern: "417:20:%253B3:<encoded_text>;"
    The text itself gets HTML entity encoding for special chars.
    """
    # HTML entity encode special characters
    encoded = text
    encoded = encoded.replace("&", "&amp;")
    encoded = encoded.replace("<", "&lt;")
    encoded = encoded.replace(">", "&gt;")
    encoded = encoded.replace('"', "&quot;")
    # Encode umlauts and special chars as HTML numeric entities
    char_map = {
        "ä": "&#228;", "ö": "&#246;", "ü": "&#252;",
        "Ä": "&#196;", "Ö": "&#214;", "Ü": "&#220;",
        "ß": "&#223;",
    }
    for char, entity in char_map.items():
        # Double-encode the ampersand for the entity (matching template)
        encoded = encoded.replace(char, f"&amp;{entity}")

    return f"417:20:%253B3:{encoded};"


def decode_namedata(namedata: str) -> str:
    """Extract the human-readable text from a nameData attribute."""
    # Pattern: "417:20:...3:TEXT;"
    match = re.search(r"3:(.+);$", namedata)
    if match:
        text = match.group(1)
        # Decode double-encoded umlauts: &amp;&#NNN; → character
        # encode_namedata produces e.g. &amp;&#228; for ä
        text = text.replace("&amp;&#228;", "ä").replace("&amp;&#246;", "ö")
        text = text.replace("&amp;&#252;", "ü").replace("&amp;&#196;", "Ä")
        text = text.replace("&amp;&#214;", "Ö").replace("&amp;&#220;", "Ü")
        text = text.replace("&amp;&#223;", "ß")
        # Also handle single-encoded form (from XML files)
        text = text.replace("&amp;#228;", "ä").replace("&amp;#246;", "ö")
        text = text.replace("&amp;#252;", "ü").replace("&amp;#196;", "Ä")
        text = text.replace("&amp;#214;", "Ö").replace("&amp;#220;", "Ü")
        text = text.replace("&amp;#223;", "ß")
        text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
        return text
    return namedata


# ---------------------------------------------------------------------------
# XML Generation
# ---------------------------------------------------------------------------

def generate_xml(
    mapping_df: pd.DataFrame,
    template_xml_path: Optional[str | Path] = None,
    output_path: str | Path = "kontenrahmen.xml",
    framework_name: str = "Custom Kontenrahmen",
) -> Path:
    """Generate LucaNet AccountFramework XML from mapping data.

    Args:
        mapping_df: DataFrame with columns: konto_nr, konto_name,
                    target_overpos_id, target_overpos_name, target_class, row_type
        template_xml_path: Optional path to template XML for root attributes
        output_path: Where to write the XML
        framework_name: Name for the AccountFramework

    Returns:
        Path to the written XML file.
    """
    output_path = Path(output_path)

    # Get accounts only
    accounts = mapping_df[mapping_df["row_type"] == "ACCOUNT"].copy()
    accounts = accounts.dropna(subset=["konto_nr"])
    accounts["konto_nr_clean"] = accounts["konto_nr"].astype(str).str.strip()

    # Parse template for root attributes
    root_attrs = _get_template_attrs(template_xml_path)
    root_attrs["nameData"] = f"3:{framework_name}"

    # Determine range
    numeric_nrs = []
    for nr in accounts["konto_nr_clean"]:
        try:
            numeric_nrs.append(int(nr))
        except ValueError:
            pass

    if numeric_nrs:
        root_attrs["firstRangeIndex"] = str(min(numeric_nrs))
        root_attrs["lastRangeIndex"] = str(max(numeric_nrs))

    # Build XML tree
    root = ET.Element("AccountFramework", **root_attrs)

    # Description
    desc = ET.SubElement(root, "Description", text=f"Generated Kontenrahmen")

    # Build AssignedRootNumberRange
    assigned_root = ET.SubElement(root, "AssignedRootNumberRange",
        nameData=encode_namedata("Zugeordnete Nummernkreise"))

    # Group accounts by target_class → target_overpos_name
    # Structure: Class > Target Position > Account Ranges
    class_order = ["AKTIVA", "PASSIVA", "AUFWAND", "ERTRAG"]
    class_labels = {
        "AKTIVA": "Aktiva", "PASSIVA": "Passiva",
        "AUFWAND": "Aufwendungen", "ERTRAG": "Erträge",
    }

    for tc in class_order:
        class_accounts = accounts[accounts["target_class"] == tc]
        if class_accounts.empty:
            continue

        class_node = ET.SubElement(assigned_root, "NumberRange",
            nameData=encode_namedata(class_labels.get(tc, tc)))

        # Group by target position
        for target_name, group in class_accounts.groupby("target_overpos_name", sort=False):
            if not target_name or target_name == "UNMAPPED":
                continue

            pos_node = ET.SubElement(class_node, "NumberRange",
                nameData=encode_namedata(str(target_name)),
                positionName=str(target_name),
                positionType="0")

            # Create number ranges for each account
            for _, row in group.iterrows():
                nr = str(row["konto_nr_clean"])
                name = str(row.get("konto_name", nr))

                range_attrs = {
                    "nameData": encode_namedata(name),
                    "positionName": str(target_name),
                    "positionType": "0",
                }
                try:
                    range_attrs["firstRangeIndex"] = str(int(nr))
                except ValueError:
                    range_attrs["firstRangeIndex"] = "0"

                ET.SubElement(pos_node, "NumberRange", **range_attrs)

    # Handle unmapped accounts in NonAssignedRootNumberRange
    unmapped = accounts[
        (accounts.get("target_overpos_id", pd.Series()) == "UNMAPPED") |
        (accounts["target_overpos_name"].isna()) |
        (accounts["target_overpos_name"] == "")
    ]
    if not unmapped.empty:
        non_assigned = ET.SubElement(root, "NonAssignedRootNumberRange",
            nameData=encode_namedata("Nicht zugeordnete Nummernkreise"))
        for _, row in unmapped.iterrows():
            nr = str(row["konto_nr_clean"])
            name = str(row.get("konto_name", nr))
            attrs = {"nameData": encode_namedata(name)}
            try:
                attrs["firstRangeIndex"] = str(int(nr))
            except ValueError:
                attrs["firstRangeIndex"] = "0"
            ET.SubElement(non_assigned, "NumberRange", **attrs)

    # Write XML
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(str(output_path), encoding="UTF-8", xml_declaration=True)

    return output_path


def _get_template_attrs(template_path: Optional[str | Path]) -> Dict[str, str]:
    """Extract root attributes from template XML, or return defaults."""
    defaults = {
        "versionID": "1.0",
        "financialPositionModelOID": "1700",
        "firstRangeIndex": "0",
        "lastRangeIndex": "9999",
    }
    if template_path is None:
        return defaults

    try:
        tree = ET.parse(str(template_path))
        root = tree.getroot()
        attrs = dict(root.attrib)
        # Remove nameData from template (we'll set our own)
        attrs.pop("nameData", None)
        return attrs
    except Exception:
        return defaults
