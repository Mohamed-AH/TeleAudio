# TeleAudio: Implementation Plan

## Context

Sheikh Hassan Al-Dugheiri's Telegram channel (`قناة الشيخ حسن الدغريري`) has been exported as
four HTML files (`messages.html` through `messages4.html`), containing approximately 5,121 total
messages. The objective is to extract every audio file record and produce two structured Excel
spreadsheets matching the format of the existing sample files in this repository.

This plan was committed on 2026-04-20 for branch `claude/telegram-data-upload-i7Bs8`.

---

## Sample File Formats

### `updatedData5Feb2026.xlsx` — Full Archive (11 columns)
Sheet name: `update5Feb`

| Column | Description |
|--------|-------------|
| S.No | Serial number |
| TelegramFileName | Audio filename attached to the message |
| Type | `Khutba` or series-type label |
| SeriesName | Arabic name of the series or sermon title |
| SequenceInSeries | Lesson/episode number within the series |
| OriginalAuthor | Author of source book (if applicable) |
| Location/Online | Mosque name or `Online` |
| Sheikh | Speaker name |
| DateInGreg | Gregorian date (DD/MM/YYYY) |
| ClipLength | Audio duration (MM:SS) |
| Category | `Khutba` / `Aqeedah` / `Fiqh` / `Hadeeth` / `Other` |

### `khutba_archive.xlsx` — Khutba Archive (7 columns)
Sheet name: `Khutba Archive`

S.No, TelegramFileName, Type, SeriesName, Sheikh, DateInGreg, Category

---

## Reference Implementations

### intelliExtract (`Mohamed-AH/intelliExtract`)
- HTML export → parsed JSON → series keyword matching → AI extraction
- 15-column CSV output with `doubtsStatus` confidence field
- Three-stage pipeline: series match → khutba detection → unknown fallback
- Key utilities: `parse_date()`, `extract_arabic_date()`, `is_online()`, `extract_serial()`

### khutba archive (`Mohamed-AH/khutba`, `claude/telegram-archive-script-65Zk5`)
- JSON checkpoint pattern: load existing → skip done → save after each batch
- Status tracking per record: `"downloaded"` / `"youtube"` / `"dl_failed"` / `"no_media"`
- Telethon session persistence for safe API re-authentication

---

## Architecture

```
messages*.html (×4)
       │
       ▼
[Phase 1: parse_html.py]
       │
       ▼
checkpoints/raw_messages.json   ← all audio records, no extraction yet
       │
       ▼
[Phase 2: extract_metadata.py]  ← Claude API, batches of 50, checkpoint after each
       │
       ▼
checkpoints/progress.json       ← same records + extracted fields + status
       │
       ▼
[Phase 3: export_excel.py]
       │
       ├──▶ output/full_archive.xlsx    (all records, 11 cols)
       └──▶ output/khutba_only.xlsx     (khutba only, 7 cols)
```

---

## Phase 1 — HTML Parsing (`src/parse_html.py`)

### Goal
Extract every audio-attachment message from the 4 HTML files into a single JSON array.

### Telegram HTML Structure
```html
<div class="message default clearfix" id="messageNNN">

  <!-- Date/time is in the title attribute -->
  <div class="pull_right date details" title="DD.MM.YYYY HH:MM:SS UTC+03:00">
    HH:MM
  </div>

  <!-- Audio attachment block -->
  <div class="media_wrap clearfix">
    <a class="media clearfix pull_left block_link media_audio_file"
       href="files/FILENAME.m4a">
      <div class="body">
        <div class="title bold">Audio title text (Arabic)</div>
        <div class="status details">MM:SS</div>   <!-- clip duration -->
      </div>
    </a>
  </div>

  <!-- Arabic text body with series info, lesson number, date -->
  <div class="text">Arabic message text…</div>

</div>
```

### Output Record Schema (`checkpoints/raw_messages.json`)
```json
[
  {
    "id": "messages.html::message1264",
    "source_file": "messages.html",
    "message_id": "1264",
    "telegram_filename": "S-LD0gmmtd4.m4a",
    "audio_title": "وقفات رمضان ( 3 ) – خطبة جمعة للشيخ...",
    "clip_length": "17:25",
    "date_raw": "09.10.2018",
    "message_text": "Full Arabic text body..."
  }
]
```

### Processing Notes
- Process files in order: messages.html → messages2.html → messages3.html → messages4.html
- Skip messages without `media_audio_file` class (text-only announcements)
- Log count of skipped vs extracted per file
- Expected yield: ~1,000–1,500 audio records from ~5,121 total messages

---

## Phase 2 — AI Extraction (`src/extract_metadata.py`)

### Goal
Call Claude API to extract structured metadata for each raw record. Checkpoint after every
batch so processing can be interrupted and resumed without re-processing completed records.

### Checkpoint File Schema (`checkpoints/progress.json`)
```json
{
  "metadata": {
    "total": 1452,
    "processed": 300,
    "batch_size": 50,
    "last_updated": "2026-04-20T14:00:00Z"
  },
  "records": [
    {
      "id": "messages.html::message1264",
      "telegram_filename": "S-LD0gmmtd4.m4a",
      "audio_title": "...",
      "clip_length": "17:25",
      "date_raw": "09.10.2018",
      "message_text": "...",
      "status": "done",
      "extracted": {
        "Type": "Khutba",
        "SeriesName": "وقفات رمضان",
        "SequenceInSeries": "3",
        "OriginalAuthor": null,
        "Location_Online": "Online",
        "Sheikh": "حسن بن محمد منصور الدغريري",
        "DateInGreg": "09/10/2018",
        "Category": "Khutba"
      },
      "doubts": "none"
    }
  ]
}
```

### Resume Logic
1. If `progress.json` exists → load it, preserving all `"done"` records
2. Otherwise → load `raw_messages.json`, set all `status = "pending"`
3. Filter to `status != "done"` and process in batches of 50
4. After each batch → overwrite `progress.json` atomically (write to `.tmp`, rename)
5. On Ctrl+C → graceful exit after current batch saves

### Claude API Design
- **Model:** `claude-haiku-4-5-20251001` (fast, low cost for structured extraction)
- **Input per call:** `audio_title` + `message_text` + `date_raw` + `clip_length`
- **Output:** JSON object with all 8 extraction fields
- **Few-shot examples:** Include 2–3 examples from sample Excel rows in the system prompt
- **Rate limiting:** 0.5s delay between calls; exponential backoff (1s, 2s, 4s) on errors
- **doubts field:** `"none"` if confident; otherwise a short description of uncertainty

### Extraction Fields
| Field | Source | Notes |
|-------|--------|-------|
| Type | Title + text | "خطبة" → Khutba; else Series |
| SeriesName | Title + text | Full Arabic series name |
| SequenceInSeries | Text | "الدرس (N)" pattern |
| OriginalAuthor | Text | Author of cited book, if present |
| Location_Online | Text | "عن بُعد" → Online; else mosque name |
| Sheikh | Title + text | Default: حسن بن محمد منصور الدغريري |
| DateInGreg | `date_raw` | Convert DD.MM.YYYY → DD/MM/YYYY |
| Category | Type + series | Khutba / Aqeedah / Fiqh / Hadeeth / Other |

---

## Phase 3 — Excel Export (`src/export_excel.py`)

### Goal
Read completed records from `progress.json` and write two styled Excel files.

### Output 1: `output/full_archive.xlsx`
- Sheet: `Full Archive`
- All records (both Khutba and series)
- 11 columns in updatedData order
- S.No auto-incremented

### Output 2: `output/khutba_only.xlsx`
- Sheet: `Khutba Archive`
- Filtered to records where `Type == "Khutba"`
- 7 columns in khutba_archive order
- S.No auto-incremented

### Formatting (matching sample files)
- Header row: dark blue fill (`1F4E79`), white bold text, font size 11
- Odd data rows: white fill
- Even data rows: light blue fill (`D6E4F0`)
- Arabic text columns (SeriesName, Sheikh): right-aligned, RTL
- Numeric/date columns: left-aligned
- Frozen header row (row 1)
- Auto-filter enabled
- Column widths: auto-sized with a minimum of 12 and maximum of 50

---

## Verification Checklist

| Step | Command | Expected Result |
|------|---------|-----------------|
| Phase 1 | `python src/parse_html.py` | `raw_messages.json` with 1,000–1,500 records |
| Phase 2 start | `python src/extract_metadata.py` | Progress bar shows batches processing |
| Phase 2 resume | Ctrl+C, then re-run | Resumes from last saved batch, no duplicates |
| Phase 2 complete | Check `progress.json` | `metadata.processed == metadata.total` |
| Phase 3 | `python src/export_excel.py` | Two Excel files in `output/` |
| Manual QA | Open Excel files | Headers, Arabic text, dates match sample files |

---

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|-----------|
| API rate limit errors | Processing halts | Exponential backoff; batch checkpoint prevents rework |
| Arabic encoding corruption | Data loss | Enforce UTF-8 everywhere; openpyxl native Unicode |
| Inconsistent HTML across 4 files | Missing records | Test parser on all 4 files in Phase 1 before Phase 2 |
| Audio records without text | Missing metadata | Use `audio_title` as fallback; mark `doubts` appropriately |
| High API cost | Budget overrun | Haiku model (~$0.25/MTok); ~1,500 × 500 tokens ≈ $0.19 |

---

## Dependencies

```
beautifulsoup4>=4.12   # HTML parsing
lxml>=5.0              # Fast BS4 parser backend
openpyxl>=3.1          # Excel read/write
anthropic>=0.40        # Claude API client
python-dotenv>=1.0     # .env loading
tqdm>=4.66             # Progress bars
```

Install: `pip install -r requirements.txt`
