# 📊 SuSa → LucaNet Kontenrahmen XML Pipeline

Diese Pipeline automatisiert die hochkomplexe Verarbeitung von Summen- und Saldenlisten (SuSa) und deren Mapping auf einen LucaNet-Zielkontenrahmen. Durch den Einsatz von modernsten Large Language Models (LLM) werden manuelle Mapping-Fehler minimiert und die Verarbeitungsgeschwindigkeit drastisch erhöht.

---

## 🚀 Kernfunktionen

### 1. 🔍 Intelligente Tabellenerkennung (`table_detect.py`)
Die Pipeline erkennt automatisch den Aufbau verschiedenster SuSa-Formate. 
- Identifiziert Start- und Endzeilen der Kontentabellen.
- Erkennt Spaltenzuordnungen (Konto-Nr, Beschreibung, Salden).
- Erkennt automatisch die verwendete Vorzeichenkonvention (z.B. Soll/Haben-Spalten vs. Vorzeichenlogik).

### 2. 🤖 KI-gestütztes Mapping (`mapping.py`)
Buchungskonten werden nicht starr, sondern semantisch zugeordnet.
- Nutzt LLMs, um Kontenbeschreibungen zu verstehen und in den LucaNet-Zielrahmen einzusortieren.
- Beachtet Whitelists und vordefinierte Zielpositionen.

### 3. 🧪 Validierung & Iterative Reparatur (`validate.py`)
Sicherheit steht an erster Stelle. 
- **Bilanz-Check**: Prüft, ob die Summe aller gemappten Konten weiterhin ausgeglichen ist.
- **Auto-Repair**: Bei Fehlmapping oder unklaren Positionen führt die Pipeline bis zu 2 Korrekturdurchläufe (Repair Rounds) durch, um die Konsistenz sicherzustellen.

### 4. 📄 XML-Export für LucaNet (`xml_export.py`)
Erzeugt direkt importierbare `AccountFramework.xml` Dateien.
- Unterstützt Vorlagen (Templates) zur Beibehaltung globaler Einstellungen.
- Automatische Normalisierung der Vorzeichen für den LucaNet-Import.

---

## 🛠 Installation & Setup

1. **Repository klonen**:
   ```bash
   git clone https://github.com/Lugier/Kontenrahmen-b-p.git
   cd Kontenrahmen-b-p
   ```

2. **Abhängigkeiten installieren**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Konfiguration**:
   Erstellen Sie eine `.env` Datei im Hauptverzeichnis:
   ```env
   OPENAI_API_KEY=sk-xxxx...
   ```

---

## 💻 Nutzung (CLI)

Starten Sie die Verarbeitung über die `main.py`:

```bash
python main.py --susa "Pfad/zu/Ihrer_SuSa.xlsx" --targets "Pfad/zu/LucaNet_Zuordnung.xlsx"
```

### Optionale Parameter:
- `--out`: Zielordner für CSV-Mapping, Log und XML (Default: `./output`).
- `--model`: Das zu verwendende KI-Modell (Default: `gpt-5-mini-2025-08-07`).
- `--period`: Spezifiziert den Zeitraum (z.B. `2023-12`).
- `--verbose`: Zeigt detaillierte Debug-Informationen während des Laufs.

---

## 📁 Projektstruktur

```text
├── main.py            # Zentraler Einstiegspunkt
├── src/               # Modulare Kernlogik
│   ├── table_detect.py # KI-Tabellenerkennung
│   ├── mapping.py      # LLM-Mapping-Logik
│   ├── signs.py        # Vorzeichen-Normalisierung
│   ├── validate.py     # Konsistenzprüfung & Repair
│   └── xml_export.py   # LucaNet XML Generator
├── tests/             # Automatisierte Test-Suite
└── requirements.txt   # Benötigte Python-Pakete
```

---

## 🛡 Disclaimer
*Internes Tool der Bachert Unternehmensberatung GmbH & Co. KG. Nur für befugtes Personal.*
