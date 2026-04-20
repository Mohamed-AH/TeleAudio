# TeleAudio – Source of Truth

## Project Overview

Extract audio metadata from Sheikh Hassan Al-Dugheiri's Telegram channel export (4 HTML files,
~5,121 total messages) into structured Excel spreadsheets, matching the format of the two sample
files already in this repo.

**Channel:** قناة الشيخ حسن الدغريري  
**Sheikh:** حسن بن محمد منصور الدغريري  
**Content type:** Islamic sermons (خطب جمعة), lesson series (Aqeedah, Fiqh, Hadeeth), and special lectures

---

## Project State

**Current Phase:** Phase 1 – Not yet started  
**Records Parsed:** 0 / ~5,121 messages → ? audio records  
**Records Extracted:** 0 / ?  
**Last Session:** 2026-04-20  
**Checkpoint file:** `checkpoints/progress.json` (does not exist yet — run Phase 1 first)

---

## To-Do List

- [ ] Phase 1: Run `python src/parse_html.py` → produces `checkpoints/raw_messages.json`
- [ ] Phase 2: Run `python src/extract_metadata.py` → produces `checkpoints/progress.json` (safe to stop/restart)
- [ ] Phase 3: Run `python src/export_excel.py` → produces `output/full_archive.xlsx` and `output/khutba_only.xlsx`
- [ ] Manually review spot-check 10–20 records in output files against sample files
- [ ] Final commit with output files (or upload separately)

---

## Repository Layout

```
TeleAudio/
├── CLAUDE.md                     ← YOU ARE HERE (Source of Truth)
├── docs/
│   └── implementation-plan.md    ← Full technical plan
├── src/
│   ├── parse_html.py             ← Phase 1: HTML → raw_messages.json
│   ├── extract_metadata.py       ← Phase 2: AI extraction → progress.json
│   └── export_excel.py           ← Phase 3: progress.json → Excel files
├── checkpoints/
│   ├── raw_messages.json         ← (gitignored) Phase 1 output
│   └── progress.json             ← (gitignored) Phase 2 checkpoint
├── output/                       ← (gitignored) Final Excel outputs
├── messages.html                 ← Telegram export part 1
├── messages2.html                ← Telegram export part 2
├── messages3.html                ← Telegram export part 3
├── messages4.html                ← Telegram export part 4
├── updatedData5Feb2026.xlsx      ← Sample format: series/lesson detail (11 cols)
├── khutba_archive.xlsx           ← Sample format: khutba archive (7 cols)
├── requirements.txt
└── .env.example
```

---

## Running Instructions

### Setup (once)
```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

### Phase 1 – Parse HTML
```bash
python src/parse_html.py
# Output: checkpoints/raw_messages.json
# Expected: ~1,000–1,500 audio records extracted from ~5,121 total messages
```

### Phase 2 – AI Extraction (checkpointed, safe to interrupt)
```bash
python src/extract_metadata.py
# Processes in batches of 50; saves progress after each batch
# Safe to Ctrl+C and restart — resumes from last saved record
# Output: checkpoints/progress.json
```

### Phase 3 – Export to Excel
```bash
python src/export_excel.py
# Output: output/full_archive.xlsx (all records, 11 cols)
#         output/khutba_only.xlsx  (khutba only, 7 cols)
```

---

## Target Output Formats

### Full Archive (updatedData format) — 11 columns
| # | Column | Example |
|---|--------|---------|
| 1 | S.No | 1 |
| 2 | TelegramFileName | S-LD0gmmtd4.m4a |
| 3 | Type | Khutba |
| 4 | SeriesName | وقفات رمضان |
| 5 | SequenceInSeries | 3 |
| 6 | OriginalAuthor | (blank if not applicable) |
| 7 | Location/Online | Online |
| 8 | Sheikh | حسن بن محمد منصور الدغريري |
| 9 | DateInGreg | 09/10/2018 |
| 10 | ClipLength | 17:25 |
| 11 | Category | Khutba |

### Khutba Archive — 7 columns
S.No, TelegramFileName, Type, SeriesName, Sheikh, DateInGreg, Category

---

## Data Notes

*(Update this section as patterns are discovered during processing)*

- Most non-audio messages are announcements, links, or forwarded content — skipped in Phase 1
- Audio filenames are either Telegram-generated IDs (e.g. `S-LD0gmmtd4.m4a`) or Arabic-named files
- Dates appear in the HTML as `DD.MM.YYYY` in the `title` attribute of the date element
- Message text often contains the full series name, lesson number (الدرس), and Hijri date
- Location indicators: "عن بُعد" = Online; explicit mosque name = in-person

---

## Reference Implementations

- **intelliExtract** – `https://github.com/Mohamed-AH/intelliExtract` – core extraction logic, series keyword matching, confidence/doubts system
- **khutba archive** – `https://github.com/Mohamed-AH/khutba/tree/claude/telegram-archive-script-65Zk5` – checkpoint/resume pattern, JSON status tracking
