# SuSa → LucaNet Kontenrahmen XML Pipeline

Diese Pipeline automatisiert die Verarbeitung von Summen- und Saldenlisten (SuSa) und deren Mapping auf einen LucaNet-Zielkontenrahmen unter Einsatz von Large Language Models (LLM).

## Hauptfunktionen

1.  **Tabellenerkennung**: Automatisierte Erkennung von Tabellenstart, -ende und Spaltenstruktur in komplexen Excel-Sheets via LLM.
2.  **KI-Mapping**: Intelligente Zuordnung von Buchungskonten zu LucaNet-Positionen basierend auf Kontonummer und Beschreibung.
3.  **Normalisierung**: Vereinheitlichung von Beträgen, Vorzeichenkonventionen und Datumsformaten.
4.  **Validierung & Reparatur**: Prüfung der Bilanzsummen und automatisierte Korrektur fehlerhafter Mappings durch iterative LLM-Calls.
5.  **XML-Export**: Erzeugung einer LucaNet-kompatiblen XML-Datei für den Import des Kontenrahmens.

## Installation

```bash
git clone <repository-url>
cd Kontenrahmen
pip install -r requirements.txt
```

## Konfiguration

Erstellen Sie eine `.env` Datei im Hauptverzeichnis mit Ihrem OpenAI API-Key:

```env
OPENAI_API_KEY=your_api_key_here
```

## Nutzung

Die Pipeline wird über die `main.py` gestartet:

```bash
python main.py --susa "Pfad/zu/SuSa.xlsx" --targets "Pfad/zu/LucaNet_Zuordnung.xlsx"
```

### Argumente:
- `--susa`: Pfad zur SuSa Excel-Datei (Erforderlich)
- `--targets`: Pfad zur LucaNet Zuordnungs-Datei (Erforderlich)
- `--out`: Zielverzeichnis für die Ergebnisse (Standard: `./output`)
- `--model`: Zu verwendendes OpenAI Modell (Standard: `gpt-5-mini-2025-08-07`)

## Projektstruktur

- `src/`: Kernlogik der Pipeline (IO, LLM-Client, Mapping, Validierung).
- `tests/`: Unit-Tests für die verschiedenen Komponenten.
- `main.py`: CLI-Einstiegspunkt.
- `requirements.txt`: Python-Abhängigkeiten.

## Lizenz

Interne Nutzung - Bachert Unternehmensberatung GmbH & Co. KG
