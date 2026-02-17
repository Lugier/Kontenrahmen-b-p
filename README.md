# 🏦 SuSa to LucaNet XML Pipeline
> **Automatisierte Kontenmmapping-Intelligence für die Wirtschaftsprüfung & Unternehmensberatung**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![OpenAI GPT-5-mini](https://img.shields.io/badge/AI-GPT--5--mini-orange.svg)](https://openai.com/)
[![License: Internal](https://img.shields.io/badge/License-Internal-red.svg)](#)

Diese Pipeline löst eines der zeitaufwendigsten Probleme in der Finanzberatung: Die Transformation von heterogenen **Summen- und Saldenlisten (SuSa)** in strukturierte, LucaNet-kompatible **XML-Importdateien**. Durch den Einsatz von Generativer KI entfällt das manuelle Mapping von tausenden Konten.

---

## 🌟 Key Highlights

*   **🧠 Semantisches Verständnis**: Statt regulärer Ausdrücke nutzt die Pipeline LLMs, um die *Bedeutung* eines Kontos zu verstehen.
*   **📐 Dynamische Grid-Erkennung**: Erkennt automatisch, wo eine Tabelle in einem Excel-Sheet beginnt und endet, selbst bei komplexen Headern oder Leerzeilen.
*   **⚖️ Bilanz-Integrität**: Jedes Mapping wird gegen die ursprüngliche Bilanzsumme validiert. Sollte die Bilanz nicht aufgehen, startet die KI einen automatischen Reparaturprozess.
*   **🔄 Vorzeichen-Logik**: Erkennt automatisch, ob Daten nach der Soll/Haben-Logik oder der Vorzeichen-Logik (LucaNet Standard) strukturiert sind und konvertiert diese fehlerfrei.

---

## 🛠 Architektur & Module

Das System ist modular aufgebaut, um maximale Wartbarkeit zu gewährleisten:

| Modul | Beschreibung |
| :--- | :--- |
| `src/table_detect.py` | **KI-Scanner**: Analysiert Excel-Strukturen und identifiziert Kontenrahmen-Bereiche. |
| `src/mapping.py` | **Mappers**: Das "Gehirn", das Konten auf LucaNet-Positionen (Targets) projiziert. |
| `src/validate.py` | **Guardrails**: Mathematische Prüfung der Ergebnisse und KI-gestützte Fehlerkorrektur. |
| `src/xml_export.py` | **Generator**: Erstellt die finale `AccountFramework.xml` für den LucaNet-Import. |
| `src/normalize.py` | **Sanitizer**: Bereinigt Beträge, Formate und Sonderzeichen. |

---

## 🚦 Schnellstart

### 1. Voraussetzungen
*   Python 3.9 oder höher
*   OpenAI API Key

### 2. Installation
```bash
# Repository klonen
git clone https://github.com/Lugier/Kontenrahmen-b-p.git
cd Kontenrahmen-b-p

# Virtuelle Umgebung erstellen (empfohlen)
python -m venv venv
source venv/bin/activate  # Auf Windows: venv\Scripts\activate

# Abhängigkeiten installieren
pip install -r requirements.txt
```

### 3. Konfiguration
Erstelle eine `.env` Datei basierend auf dem Beispiel:
```bash
cp .env.example .env
```
Füge deinen `OPENAI_API_KEY` in die `.env` ein.

### 4. Ausführung
```bash
python main.py --susa "Eingabe/SuSa_Kunde_X.xlsx" --targets "Konfig/LucaNet_Mapping.xlsx"
```

---

## 📊 Pipeline-Workflow

1.  **Ingestion**: Einlesen der Excel/CSV Quelldateien.
2.  **Detection**: LLM identifiziert relevante Spalten (Konto, Name, Salden).
3.  **Extraction**: Python-basierte Extraktion der Rohdaten.
4.  **Semantic Mapping**: Batch-Verarbeitung der Konten durch das LLM.
5.  **Sign Normalization**: Mathematische Korrektur der Vorzeichen für LucaNet.
6.  **Validation**: Prüfung auf Vollständigkeit und Bilanzgleichheit.
7.  **Auto-Repair**: (Optional) Korrektur-Loop bei Validierungsfehlern.
8.  **Output**: Generierung von CSV-Berichten und der XML-Datei.

---

## 🔒 Sicherheit & Datenschutz

*   **Keine lokalen Datenbanken**: Die Pipeline verarbeitet Daten im Arbeitsspeicher (Cache ist optional und verschlüsselt/lokal).
*   **Environment Variables**: API-Keys werden niemals im Code gespeichert.
*   **Ignore-Policies**: Rohdaten-Exporte (`output/`), lokale Umgebungsvariablen (`.env`) und Excel-Quelldateien sind strikt von der Versionskontrolle ausgeschlossen (`.gitignore`).

---

## 📄 Lizenz
Dieses Projekt ist für die interne Nutzung bei der **Bachert Unternehmensberatung GmbH & Co. KG** bestimmt. Alle Rechte vorbehalten.

---
*Entwickelt mit ❤️ für effizientere Finanzprozesse.*
