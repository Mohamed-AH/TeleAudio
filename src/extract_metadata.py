"""
Phase 2 – Rule-Based Metadata Extraction (no API required)
Reads checkpoints/raw_messages.json, applies Arabic text pattern matching,
and writes checkpoints/progress.json with checkpoint/resume support.
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

ROOT = Path(__file__).parent.parent
RAW_PATH = ROOT / "checkpoints" / "raw_messages.json"
PROGRESS_PATH = ROOT / "checkpoints" / "progress.json"
PROGRESS_TMP = ROOT / "checkpoints" / "progress.json.tmp"

SHEIKH = "حسن بن محمد منصور الدغريري"
DEFAULT_LOCATION = "جامع الورود"

# ---------------------------------------------------------------------------
# Arabic-Indic digit normalisation
# ---------------------------------------------------------------------------
_ARABIC_INDIC = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def normalise_digits(text: str) -> str:
    return text.translate(_ARABIC_INDIC)


# ---------------------------------------------------------------------------
# Arabic ordinal word → integer
# ---------------------------------------------------------------------------
_UNITS = {
    "الأول": 1, "الثاني": 2, "الثالث": 3, "الرابع": 4, "الخامس": 5,
    "السادس": 6, "السابع": 7, "الثامن": 8, "التاسع": 9, "العاشر": 10,
    "الحادي": 1, "الثانية": 2, "الثانيه": 2,
}
_TENS = {
    "عشر": 10, "عشرين": 20, "العشرون": 20, "العشرين": 20,
    "ثلاثين": 30, "الثلاثون": 30, "الثلاثين": 30,
    "أربعين": 40, "الأربعون": 40, "الأربعين": 40,
    "خمسين": 50, "الخمسون": 50, "الخمسين": 50,
    "ستين": 60, "الستون": 60, "الستين": 60,
    "سبعين": 70, "السبعون": 70, "السبعين": 70,
    "ثمانين": 80, "الثمانون": 80, "الثمانين": 80,
    "تسعين": 90, "التسعون": 90, "التسعين": 90,
    "مئة": 100, "المئة": 100, "مائة": 100,
}


def ordinal_to_int(word: str) -> int | None:
    """Convert a multi-word Arabic ordinal to integer, e.g. 'الثامن والثلاثون' → 38."""
    word = word.strip()
    # Direct unit match
    if word in _UNITS:
        return _UNITS[word]
    # Compound: "الحادي عشر", "الثاني عشر", ...
    m = re.match(r"(الحادي|الثاني|الثالث|الرابع|الخامس|السادس|السابع|الثامن|التاسع)\s+عشر", word)
    if m:
        unit_map = {
            "الحادي": 11, "الثاني": 12, "الثالث": 13, "الرابع": 14,
            "الخامس": 15, "السادس": 16, "السابع": 17, "الثامن": 18, "التاسع": 19,
        }
        return unit_map.get(m.group(1))
    # Compound: "unit والـtens", e.g. "الثامن والثلاثون"
    m = re.match(r"(\S+)\s+و(\S+)", word)
    if m:
        unit_part = m.group(1)
        tens_part = m.group(2).lstrip("ال")
        u = _UNITS.get(unit_part, 0)
        t = _TENS.get("ال" + tens_part, _TENS.get(tens_part, 0))
        if u and t:
            return u + t
    # Pure tens
    for k, v in _TENS.items():
        if word == k:
            return v
    return None


def extract_lesson_number(text: str) -> str | None:
    """Extract lesson number from Arabic text."""
    text_norm = normalise_digits(text)

    # "الدرس رقم 107" or "الدرس رقم ١٠٧"
    m = re.search(r"الدرس\s+رقم\s*[:\-]?\s*(\d+)", text_norm)
    if m:
        return m.group(1)

    # "الدرس - 45 -" or "الدرس ٤٥"
    m = re.search(r"الدرس\s+[-–]?\s*(\d+)\s*[-–]?", text_norm)
    if m:
        return m.group(1)

    # Plain ordinal: "الدرس الأول" / "الدرس الثامن والثلاثون"
    m = re.search(r"الدرس\s+([\u0600-\u06ff\s]+?)(?:\s*[:\.\|]|\s*\n|$)", text, re.UNICODE)
    if m:
        ordinal_text = m.group(1).strip()
        num = ordinal_to_int(ordinal_text)
        if num:
            return str(num)
        # Try ordinal word as-is (e.g. "الأول" alone)
        for word in ordinal_text.split():
            n = ordinal_to_int(word)
            if n:
                return str(n)

    return None


# ---------------------------------------------------------------------------
# Type detection
# ---------------------------------------------------------------------------
KHUTBA_KEYWORDS = [
    "خطبة الجمعة", "خطبة جمعة", "خطبة العيد", "خطبة عيد",
    "خطبة صلاة الاستسقاء", "خطبة الاستسقاء",
    "#خطبة_الجمعة",
]


def detect_type(title: str, text: str) -> str:
    combined = title + " " + text
    for kw in KHUTBA_KEYWORDS:
        if kw in combined:
            return "Khutba"
    # Audio title that contains "خطبة" as standalone word
    if re.search(r"\bخطبة\b", title):
        return "Khutba"
    return "Series"


# ---------------------------------------------------------------------------
# Category detection
# ---------------------------------------------------------------------------
CATEGORY_MAP = {
    # Aqeedah
    "الأفنان_الندية": "Aqeedah",
    "التعليق_على_شرح_كتاب_التوحيد": "Aqeedah",
    "كتاب_التوحيد": "Aqeedah",
    "التعليقات_الأثرية_على_العقيدة_الواسطية": "Aqeedah",
    "شرح_كتاب_شرح_السنة_للبربهاري": "Aqeedah",
    "التعليقات_المختصرة": "Aqeedah",
    "الملخص_في_شرح": "Aqeedah",
    "الملخص_شرح": "Aqeedah",
    "العقيدة_الواسطية": "Aqeedah",
    "التعليقات_البهية": "Aqeedah",
    # Fiqh
    "فتاوى_أركان_الإسلام": "Fiqh",
    "شرح_كتاب_الفقه_الميسر": "Fiqh",
    "تأسيس_الأحكام": "Fiqh",
    "كتاب_الصيام": "Fiqh",
    "كتاب_الحج": "Fiqh",
    "فقه_الميسر": "Fiqh",
    "الفقه_الميسر": "Fiqh",
    "أحكام_الطهارة": "Fiqh",
    # Hadeeth
    "التعليق_على_كتاب_الأربعين_النووية": "Hadeeth",
    "المورد_العذب_الزلال": "Hadeeth",
    "الأربعين_النووية": "Hadeeth",
    # Quran/Tafsir
    "التفسير_الميسر": "Quran",
    "تفسير": "Quran",
    # Khutba
    "خطبة_الجمعة": "Khutba",
    "خطبة": "Khutba",
}

CATEGORY_TEXT_MAP = {
    "عقيدة": "Aqeedah",
    "فقه": "Fiqh",
    "حديث": "Hadeeth",
    "تفسير": "Quran",
    "خطبة": "Khutba",
}


def detect_category(msg_type: str, title: str, text: str) -> str:
    if msg_type == "Khutba":
        return "Khutba"
    combined = title + " " + text
    # Check hashtag patterns
    for key, cat in CATEGORY_MAP.items():
        if key in combined:
            return cat
    # Generic text keywords
    for kw, cat in CATEGORY_TEXT_MAP.items():
        if kw in combined:
            return cat
    # Fallback: infer from any hashtag containing known subjects
    if re.search(r"توحيد|عقيدة|واسطية|سنة|بربهاري|الإيمان", combined):
        return "Aqeedah"
    if re.search(r"فقه|أحكام|طهارة|صلاة|زكاة|صيام|حج|نكاح|طلاق|بيع|جنايات", combined):
        return "Fiqh"
    if re.search(r"حديث|سنة.*نبو|أربعين|مورد|زلال", combined):
        return "Hadeeth"
    if re.search(r"تفسير|سورة|قرآن|تلاوة", combined):
        return "Quran"
    if re.search(r"سيرة|صحابة|أبوبكر|عمر|عثمان|علي", combined):
        return "Seerah"
    return "Other"


# ---------------------------------------------------------------------------
# SeriesName extraction
# ---------------------------------------------------------------------------
_FILENAME_RE = re.compile(
    r"^(AUD-|AUDIO-|[\d_]+_|<unknown>|LGE|Huawei|Samsung|iPhone)",
    re.IGNORECASE,
)
_DEVICE_PREFIX_RE = re.compile(r"^(?:<unknown>|LGE|Huawei|Samsung|iPhone|\w{2,6})\s*[–\-]\s*")
_AUDIO_EXT_RE = re.compile(r"\.(m4a|mp3|aac|amr|ogg|opus|3gp)$", re.IGNORECASE)

_SKIP_HASHTAGS = {
    "جديد_الدروس", "جديد_الصوتيات", "جديد_الكلمات", "جديد_التعليقات",
    "التعليق_على_كتاب", "خطبة_الجمعة", "من_الأرشيف",
    "حسن_بن_محمد_منصور_الدغريري", "حسن_بن_محمد_الدغريري", "حسن_بن_محمد_دغريري",
}


def _is_filename(s: str) -> bool:
    """Return True if the string looks like a bare filename rather than a title."""
    return bool(_FILENAME_RE.match(s) or _AUDIO_EXT_RE.search(s))


def extract_series_name(title: str, text: str) -> str:
    """Extract the series/lecture title from title and text."""

    # 1. Strip device/unknown prefixes from title ("LGE – title", "<unknown> – title")
    clean_title = _DEVICE_PREFIX_RE.sub("", title).strip()

    # 2. If title looks like a real human-written title (not a filename), use it
    if clean_title and not _is_filename(clean_title) and len(clean_title) > 4:
        # Remove trailing sheikh attribution "– للشيخ ..." / "للشيخ حسن..."
        clean_title = re.sub(r"\s*[–\-]\s*(خطبة جمعة|للشيخ|الشيخ).*", "", clean_title)
        clean_title = re.sub(r"\s*(للشيخ|الشيخ)\s+.*", "", clean_title)
        clean_title = re.sub(r"\s*[–\-]\s*$", "", clean_title).strip()
        if clean_title and len(clean_title) > 3:
            return clean_title

    # 3. Look for "كلمة بعنوان: <title>" or "درس بعنوان: <title>"
    m = re.search(r"(?:كلمة|درس)\s+بعنوان\s*[:\-]\s*([^\n\|]+)", text)
    if m:
        return m.group(1).strip()

    # 4. Look for labeled hashtag title like "#عنوان_الدرس"
    hashtags = re.findall(r"#([^\s#\|🔸🔹📌📕]+)", text)
    for tag in hashtags:
        if tag not in _SKIP_HASHTAGS:
            # Prefer longer, more specific tags
            return tag.replace("_", " ")

    # 5. Look for text in brackets "[◈ series title]"
    m = re.search(r"\[◈\s*(.+?)\]", text)
    if m:
        candidate = re.sub(r"#\S+\s*", "", m.group(1)).strip()
        if candidate and len(candidate) > 3:
            return candidate

    # 6. Scan text lines for a meaningful title
    for line in text.split("\n"):
        line = re.sub(r"[#🔸🔹📌📕✏️🎙☑🔊◀️📥🔗🌀🎧◈]\S*", "", line)
        line = re.sub(r"https?://\S+", "", line).strip()
        line = re.sub(r"^[\|\-–:\s]+|[\|\-–:\s]+$", "", line).strip()
        if len(line) > 6 and not _is_filename(line) and not re.match(r"^[\d\W]+$", line):
            return line

    return clean_title or title  # last resort


def clean_series_name_for_display(name: str) -> str:
    """Clean up series name for final output."""
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"^[\s\-–:]+|[\s\-–:]+$", "", name)
    return name


# ---------------------------------------------------------------------------
# OriginalAuthor extraction
# ---------------------------------------------------------------------------
AUTHOR_PATTERNS = [
    r"تأليف\s+فضيلة\s+الشيخ\s+العلامة\s+([\u0600-\u06ff\s]+?)(?:\s+(?:حفظه|رحمه|وفقه|\|))",
    r"تأليف\s+فضيلة\s+الشيخ\s+([\u0600-\u06ff\s]+?)(?:\s+(?:حفظه|رحمه|وفقه|\|))",
    r"تأليف\s+الشيخ\s+([\u0600-\u06ff\s]+?)(?:\s+(?:حفظه|رحمه|وفقه|\|))",
    r"للعلامة\s+([\u0600-\u06ff\s]+?)(?:\s+(?:رحمه|حفظه|وفقه|\|))",
    r"تأليف\s+([\u0600-\u06ff\s]{10,50}?)(?:\s*\||\n)",
]


def extract_original_author(text: str) -> str | None:
    for pattern in AUTHOR_PATTERNS:
        m = re.search(pattern, text, re.UNICODE)
        if m:
            author = m.group(1).strip()
            # Skip if it's the main sheikh
            if "الدغريري" not in author and len(author) > 5:
                return author
    return None


# ---------------------------------------------------------------------------
# Location detection
# ---------------------------------------------------------------------------
ONLINE_KEYWORDS = ["عن بُعد", "عن بعد", "بث مباشر", "مباشرة عبر", "بشكل مباشر"]
LOCATION_PATTERNS = [
    ("جامع الورود", r"جامع\s+الورود"),
    ("جامع", r"جامع\s+[\u0600-\u06ff]+"),
    ("مسجد", r"مسجد\s+[\u0600-\u06ff]+"),
]


def detect_location(text: str, telegram_filename: str) -> str:
    for kw in ONLINE_KEYWORDS:
        if kw in text:
            return "Online"
    for name, pattern in LOCATION_PATTERNS:
        m = re.search(pattern, text)
        if m:
            return m.group(0)
    # AUD-/AUDIO- WhatsApp format suggests local mosque recording
    if re.match(r"AUD-|AUDIO-", telegram_filename):
        return DEFAULT_LOCATION
    return DEFAULT_LOCATION


# ---------------------------------------------------------------------------
# Date conversion
# ---------------------------------------------------------------------------
def convert_date(date_raw: str) -> str:
    """Convert DD.MM.YYYY → DD/MM/YYYY."""
    return date_raw.replace(".", "/") if date_raw else ""


# ---------------------------------------------------------------------------
# Main extraction logic
# ---------------------------------------------------------------------------
def extract_record(raw: dict) -> dict:
    title = raw.get("audio_title", "")
    text = raw.get("message_text", "")
    date_raw = raw.get("date_raw", "")
    filename = raw.get("telegram_filename", "")

    msg_type = detect_type(title, text)
    category = detect_category(msg_type, title, text)
    series_name = clean_series_name_for_display(extract_series_name(title, text))
    lesson_num = extract_lesson_number(text) if msg_type != "Khutba" else None
    author = extract_original_author(text)
    location = detect_location(text, filename)
    date_greg = convert_date(date_raw)

    doubts = []
    if not series_name or len(series_name) < 3:
        doubts.append("series_name_uncertain")
    if msg_type == "Series" and not lesson_num:
        doubts.append("lesson_number_not_found")
    if category == "Other":
        doubts.append("category_uncertain")

    return {
        "Type": msg_type,
        "SeriesName": series_name,
        "SequenceInSeries": lesson_num,
        "OriginalAuthor": author,
        "Location_Online": location,
        "Sheikh": SHEIKH,
        "DateInGreg": date_greg,
        "Category": category,
        "doubts": ", ".join(doubts) if doubts else "none",
    }


# ---------------------------------------------------------------------------
# Checkpoint I/O
# ---------------------------------------------------------------------------
def load_progress() -> dict:
    if PROGRESS_PATH.exists():
        with open(PROGRESS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        done = sum(1 for r in data["records"] if r["status"] == "done")
        print(f"Resuming: {done}/{data['metadata']['total']} records already done.")
        return data

    if not RAW_PATH.exists():
        print("ERROR: checkpoints/raw_messages.json not found. Run parse_html.py first.")
        sys.exit(1)

    with open(RAW_PATH, encoding="utf-8") as f:
        raw = json.load(f)

    records = [{**r, "status": "pending", "extracted": None, "doubts": None} for r in raw]
    return {
        "metadata": {
            "total": len(records),
            "processed": 0,
            "batch_size": 100,
            "last_updated": "",
        },
        "records": records,
    }


def save_progress(data: dict):
    data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
    PROGRESS_TMP.parent.mkdir(exist_ok=True)
    with open(PROGRESS_TMP, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    PROGRESS_TMP.rename(PROGRESS_PATH)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    data = load_progress()
    pending = [r for r in data["records"] if r["status"] != "done"]
    print(f"\n=== Phase 2: Rule-Based Extraction ===")
    print(f"Pending: {len(pending)} records\n")

    BATCH = 200
    processed = 0

    with tqdm(total=len(pending), unit="rec") as pbar:
        for record in pending:
            extracted = extract_record(record)
            record["extracted"] = extracted
            record["doubts"] = extracted.get("doubts", "none")
            record["status"] = "done"
            processed += 1
            pbar.update(1)

            if processed % BATCH == 0:
                data["metadata"]["processed"] += BATCH
                save_progress(data)

    data["metadata"]["processed"] = sum(1 for r in data["records"] if r["status"] == "done")
    save_progress(data)

    done = sum(1 for r in data["records"] if r["status"] == "done")
    with_doubts = sum(1 for r in data["records"] if r.get("doubts") and r["doubts"] != "none")
    print(f"\nDone: {done}/{data['metadata']['total']} records")
    print(f"Records with doubts: {with_doubts}")
    print(f"Checkpoint → {PROGRESS_PATH.relative_to(ROOT)}")
    print("\nPhase 2 complete. Run src/export_excel.py next.")


if __name__ == "__main__":
    main()
