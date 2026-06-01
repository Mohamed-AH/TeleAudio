"""
Gap Analysis: DB (data-export.txt) vs HTML Telegram channel exports.

Produces output/gap_analysis_report.txt describing:
  - Continuations: existing DB series with new lessons in HTML files
  - New Series: found in HTML but not in DB
  - Old/Pre-DB: DB series whose first DB date is later than some HTML messages

Usage:
    python scripts/gap_analysis.py
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup

ROOT = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# Arabic helpers
# ---------------------------------------------------------------------------

_ARABIC_INDIC = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def norm_digits(s: str) -> str:
    return s.translate(_ARABIC_INDIC)


def norm_arabic(s: str) -> str:
    """Strip tatweel, normalize alef variants to bare alef, collapse whitespace."""
    s = s.replace("ـ", "")  # tatweel
    s = re.sub(r"[أإآ]", "ا", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def hijri_to_sort_key(h: str) -> tuple:
    """Convert '1447/10/05' or '١٤٤٧/١٠/٠٥' or '5/10/1447' to (yyyy, mm, dd) tuple."""
    if not h:
        return (0, 0, 0)
    h = norm_digits(h.strip())
    # YYYY/MM/DD
    m = re.match(r"(1[34][0-9]{2})/([0-9]{1,2})/([0-9]{1,2})", h)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    # DD/MM/YYYY (with optional spaces around /)
    m = re.match(r"([0-9]{1,2})\s*/\s*([0-9]{1,2})\s*/\s*(1[34][0-9]{2})", h)
    if m:
        return (int(m.group(3)), int(m.group(2)), int(m.group(1)))
    return (0, 0, 0)


def hijri_display(h: str) -> str:
    """Return a normalised display form like 1447/10/05."""
    if not h:
        return ""
    t = hijri_to_sort_key(h)
    if t == (0, 0, 0):
        return h
    return f"{t[0]}/{t[1]:02d}/{t[2]:02d}"


# ---------------------------------------------------------------------------
# 1. Parse DB (data-export.txt)
# ---------------------------------------------------------------------------

def parse_db(path: Path) -> dict:
    """
    Returns dict: series_name -> list of {short_id, title_ar, hijri_date, audio}
    """
    db: dict = {}
    current_series: str | None = None
    current_lecture: dict | None = None

    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    for raw in lines:
        line = raw.rstrip("\n")

        # Series header
        m = re.match(r"^SERIES:\s*(.+)$", line)
        if m:
            if current_series and current_lecture:
                db.setdefault(current_series, []).append(current_lecture)
                current_lecture = None
            current_series = m.group(1).strip()
            if current_series not in db:
                db[current_series] = []
            continue

        # New lecture block
        m = re.match(r"^\s+Lecture #(\d+)$", line)
        if m:
            if current_series and current_lecture:
                db[current_series].append(current_lecture)
            current_lecture = {"short_id": None, "title_ar": None, "hijri_date": None, "audio": None}
            continue

        if current_lecture is None:
            continue

        m = re.match(r"^\s+Short ID:\s*(.+)$", line)
        if m:
            current_lecture["short_id"] = m.group(1).strip()
            continue

        m = re.match(r"^\s+Title \(AR\):\s*(.+)$", line)
        if m:
            current_lecture["title_ar"] = m.group(1).strip()
            continue

        m = re.match(r"^\s+Audio:\s*(.+)$", line)
        if m:
            current_lecture["audio"] = m.group(1).strip()
            continue

        m = re.match(r"^\s+Date Recorded \(Hijri\):\s*(.+)$", line)
        if m:
            raw_date = m.group(1).strip()
            # Normalise: might be Arabic-Indic or ASCII
            current_lecture["hijri_date"] = hijri_display(raw_date)
            continue

    # Flush last lecture
    if current_series and current_lecture:
        db[current_series].append(current_lecture)

    return db


# ---------------------------------------------------------------------------
# 2. Parse HTML files
# ---------------------------------------------------------------------------

HTML_FILES = [
    (ROOT / "messages.html",      "messages.html"),
    (ROOT / "messages2.html",     "messages2.html"),
    (ROOT / "messages3.html",     "messages3.html"),
    (ROOT / "messages4.html",     "messages4.html"),
    (ROOT / "messages_new.html",  "messages_new.html"),
    (ROOT / "messages_june1.html","messages_june1.html"),
]

# Hijri date patterns in message text
_HIJRI_YYYYMMDD_AR = re.compile(r"[١٢][٤-٥][٠-٩]{2}/[٠-٩]{1,2}/[٠-٩]{1,2}")
_HIJRI_DDMMYYYY_AR = re.compile(r"([١-٩][٠-٩]?)\s*/\s*([١-٩][٠-٩]?)\s*/\s*([١٢][٤-٥][٠-٩]{2})")
_HIJRI_ASCII      = re.compile(r"(1[34][0-9]{2})/([0-9]{1,2})/([0-9]{1,2})")

# Duration pattern
_DURATION_RE = re.compile(r"مدة الصوتية[^0-9٠-٩]*([٠-٩0-9]+:[٠-٩0-9]+)")

# Lesson-number patterns (reused from build_series_index.py logic)
_LESSON_NUM_RE = re.compile(
    r"الدرس\s*رقم\s*[:\-]?\s*([٠-٩0-9]+)"
    r"|الدرس\s*\(\s*([٠-٩0-9]+)\s*\)"
    r"|الدرس\s+[-–]?\s*([٠-٩0-9]+)\s*[-–]?"
)


def extract_hijri_from_text(text: str) -> str:
    """Try all known Hijri date patterns in the message text."""
    # Pattern 1: YYYY/MM/DD with Arabic-Indic digits
    m = _HIJRI_YYYYMMDD_AR.search(text)
    if m:
        return hijri_display(m.group(0))

    # Pattern 2: DD/MM/YYYY with Arabic-Indic digits
    m = _HIJRI_DDMMYYYY_AR.search(text)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        raw = f"{d}/{mo}/{y}"
        return hijri_display(raw)

    # Pattern 3: ASCII YYYY/MM/DD
    m = _HIJRI_ASCII.search(text)
    if m:
        return hijri_display(m.group(0))

    return ""


def extract_audio_filename(div) -> str:
    """Extract audio filename from media_wrap."""
    media = div.select_one("div.media_wrap a.media_audio_file")
    if media:
        href = media.get("href", "")
        # href is like "files/AUDIO-2026-04-20-18-06-20.m4a"
        return Path(href).name
    return ""


def extract_duration(div, text: str) -> str:
    """Extract duration from media status or text."""
    status = div.select_one("div.media_wrap .status")
    if status:
        return status.get_text(strip=True)
    m = _DURATION_RE.search(text)
    if m:
        return norm_digits(m.group(1))
    return ""


def extract_lesson_number(text: str) -> int | None:
    """Extract lesson number, handling Arabic-Indic and ordinal words."""
    text_norm = text.replace("ـ", "")
    text_ascii = norm_digits(text_norm)
    m = _LESSON_NUM_RE.search(text_ascii)
    if m:
        val = m.group(1) or m.group(2) or m.group(3)
        if val:
            return int(val)

    # Try Arabic ordinals
    _UNITS = {
        "الأول": 1, "الاول": 1, "الثاني": 2, "الثالث": 3, "الرابع": 4, "الخامس": 5,
        "السادس": 6, "السابع": 7, "الثامن": 8, "التاسع": 9, "العاشر": 10,
        "الحادي": 1,
    }
    _TENS = {
        "عشر": 10, "عشرون": 20, "عشرين": 20, "العشرون": 20, "العشرين": 20,
        "ثلاثون": 30, "ثلاثين": 30, "الثلاثون": 30, "الثلاثين": 30,
        "أربعون": 40, "أربعين": 40, "الأربعون": 40, "الأربعين": 40,
        "خمسون": 50, "خمسين": 50, "الخمسون": 50, "الخمسين": 50,
        "ستون": 60, "ستين": 60, "الستون": 60, "الستين": 60,
        "سبعون": 70, "سبعين": 70, "السبعون": 70, "السبعين": 70,
        "ثمانون": 80, "ثمانين": 80, "الثمانون": 80, "الثمانين": 80,
        "تسعون": 90, "تسعين": 90, "التسعون": 90, "التسعين": 90,
        "مئة": 100, "المئة": 100, "مائة": 100,
    }

    # "الدرس [ordinal words]"
    m2 = re.search(
        r"الدرس\s*:?\s*([؀-ۿ\s]+?)(?:\s*[-–(/\d:\.\|،]|\s*\n|$)",
        text_norm, re.UNICODE
    )
    if m2:
        word = m2.group(1).strip()
        # Simple unit
        if word in _UNITS:
            return _UNITS[word]
        # 11–19 patterns
        m3 = re.match(
            r"(الحادي|الثاني|الثالث|الرابع|الخامس|السادس|السابع|الثامن|التاسع)\s+عشر[ةه]?",
            word
        )
        if m3:
            unit_map = {"الحادي": 11, "الثاني": 12, "الثالث": 13, "الرابع": 14,
                        "الخامس": 15, "السادس": 16, "السابع": 17, "الثامن": 18, "التاسع": 19}
            return unit_map.get(m3.group(1).rstrip("ةه"))
        # Compound: unit و tens
        m3 = re.match(r"(\S+)\s+و\s+(\S+)", word)
        if m3:
            u = _UNITS.get(m3.group(1), 0)
            t = _TENS.get(m3.group(2), _TENS.get("ال" + m3.group(2).lstrip("ال"), 0))
            if u and t:
                return u + t
        # Compound: unit tens (no و)
        m3 = re.match(
            r"(الحادي|الثاني|الثالث|الرابع|الخامس|السادس|السابع|الثامن|التاسع)"
            r"\s+(الأربعون|الأربعين|الخمسون|الخمسين|الستون|الستين|السبعون|السبعين"
            r"|الثمانون|الثمانين|التسعون|التسعين)",
            word
        )
        if m3:
            u = _UNITS.get(m3.group(1), 0)
            t = _TENS.get(m3.group(2), 0)
            if u and t:
                return u + t
        # standalone tens
        t = _TENS.get(word)
        if t:
            return t
        # Try word-by-word
        for w in word.split():
            if w in _UNITS:
                return _UNITS[w]

    return None


def extract_series_name(text: str) -> str | None:
    """
    Extract series name from the 🔸-prefixed lines.
    First 🔸 line is the series title; second 🔸 line is the author.

    Strategy:
    1. If a hashtag-like series name (e.g., #صحيح_البخاري) appears near the first 🔸,
       use the hashtag text (normalised).
    2. Otherwise use the cleaned text of the first 🔸 line, skipping author-indicator
       phrases like "للعلامة", "لفضيلة", etc.
    """
    lines = text.split("\n")

    # Strip invisible Unicode (ZWNJ U+200C, RLM U+200F, LRM U+200E, BOM U+FEFF etc.)
    _INVIS_RE = re.compile(r"[​-‏‪-‮﻿­]+")

    # Find first 🔸 line index
    first_idx = -1
    for i, line in enumerate(lines):
        stripped = _INVIS_RE.sub("", line).strip()
        if stripped.startswith("🔸"):
            first_idx = i
            break

    if first_idx == -1:
        return None

    # Collect the content of the first 🔸 block (may span multiple lines up to next 🔸/🔹)
    block_lines = []
    for j in range(first_idx, min(first_idx + 5, len(lines))):
        l = _INVIS_RE.sub("", lines[j]).strip()
        if j > first_idx and (l.startswith("🔸") or l.startswith("🔹")):
            break
        block_lines.append(l)

    block_text = " ".join(block_lines)

    # Non-series hashtags to filter out
    _NON_SERIES_TAGS = re.compile(
        r"جديد_الدروس|من_الأرشيف|كتاب_الصيام|كتاب_الصلاة|كتاب_الحج|كتاب_الزكاة"
        r"|جديد_الدروس|جديد_الصوتيات|جديد_المحاضرات|حسن_بن_محمد"
    )

    # Try to extract hashtag from the block (most reliable series name)
    hashtag_m = re.search(r"#([^\s#]+)", block_text)
    if hashtag_m:
        name = hashtag_m.group(1).replace("_", " ")
        if not _NON_SERIES_TAGS.search(name):
            return name

    # Clean the first line of the block
    first_line = _INVIS_RE.sub("", lines[first_idx]).strip()
    name = re.sub(r"^[🔸\s\[\]]+", "", first_line).strip()
    name = re.sub(r"[\[\]]", "", name).strip()
    name = re.sub(r"#\S+", "", name).strip()

    # Skip author-indicator lines (second 🔸 is author)
    _AUTHOR_INDICATORS = ("للعلامة", "لفضيلة", "للشيخ", "لإمام", "للإمام", "من إنتاج", "إعداد")
    if any(name.startswith(ind) for ind in _AUTHOR_INDICATORS):
        # Try hashtag from FULL message text as fallback
        full_tags = re.findall(r"#([^\s#]+)", text)
        for tag in full_tags:
            tag_clean = tag.replace("_", " ")
            if not _NON_SERIES_TAGS.search(tag_clean) and len(tag_clean) > 3:
                return tag_clean
        return None

    # Skip session-number labels (e.g. "الـمجلس{01}")
    if re.search(r"\{0*\d+\}", name):
        # Fall back to full-text hashtag
        full_tags = re.findall(r"#([^\s#]+)", text)
        for tag in full_tags:
            tag_clean = tag.replace("_", " ")
            if not _NON_SERIES_TAGS.search(tag_clean) and len(tag_clean) > 3:
                return tag_clean
        return None

    # Strip trailing/leading noise
    name = name.strip("•-–—,. ")

    if len(name) > 3:
        return name

    # Try next few lines in block for the actual title
    for j in range(first_idx + 1, min(first_idx + 4, len(lines))):
        l = _INVIS_RE.sub("", lines[j]).strip()
        if l.startswith("🔸") or l.startswith("🔹"):
            break
        l_clean = re.sub(r"[\[\]#🔸🔹]", "", l).strip()
        if len(l_clean) > 3 and not any(l_clean.startswith(ind) for ind in _AUTHOR_INDICATORS):
            return l_clean.strip("•-–—,. ")

    return None


def extract_location(text: str) -> str:
    """Extract location indicator from message text."""
    if "عن بُعد" in text or "عن بعد" in text or "عن بُعد" in text:
        return "عن بعد"
    # Common mosque references
    if "جامع الورود" in text:
        return "جامع الورود"
    if "مسجد" in text:
        m = re.search(r"مسجد\s+\S+", text)
        if m:
            return m.group(0)
    return ""


def is_lesson_message(text: str, div) -> bool:
    """Return True if this message looks like an audio lesson post."""
    has_audio = bool(div.select_one("div.media_wrap a.media_audio_file"))
    has_lesson_keyword = "الدرس" in text
    has_duration_keyword = "مدة الصوتية" in text
    # Also allow messages with audio + 🔸 (series indicator)
    has_series_marker = "🔸" in text
    return has_audio and (has_lesson_keyword or has_duration_keyword or has_series_marker)


def parse_html_messages() -> list[dict]:
    """
    Parse all HTML files and return list of lesson messages.
    Each item: {msg_id, html_file, date_greg, hijri_date, text,
                series_name, lesson_num, audio, duration, location}

    Deduplicates by msg_id (messages4.html and messages_new.html overlap at
    ids 6636-6657; messages_new.html takes precedence as the 'new' file).
    """
    results = []
    seen_msg_ids: set[int] = set()

    for html_path, fname in HTML_FILES:
        if not html_path.exists():
            print(f"WARNING: {fname} not found — skipping", file=sys.stderr)
            continue

        with open(html_path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "lxml")

        for div in soup.select("div.message.default"):
            text_el = div.find("div", class_="text")
            if not text_el:
                continue
            text = text_el.get_text(separator="\n", strip=True)

            if not is_lesson_message(text, div):
                continue

            # Message ID
            msg_id_raw = div.get("id", "")
            msg_id = msg_id_raw.replace("message", "").strip()
            if not msg_id:
                continue
            msg_id_int = int(msg_id)
            if msg_id_int in seen_msg_ids:
                continue  # skip duplicate (overlap between messages4.html and messages_new.html)
            seen_msg_ids.add(msg_id_int)

            # Gregorian date from div.date title attribute
            date_el = div.find("div", class_="date") or div.find("span", class_="date")
            date_greg = ""
            if date_el and date_el.get("title"):
                title_attr = date_el["title"]
                m = re.match(r"(\d{2}\.\d{2}\.\d{4})", title_attr)
                if m:
                    date_greg = m.group(1)  # DD.MM.YYYY

            # Hijri date from text
            hijri = extract_hijri_from_text(text)

            # Series name
            series_name = extract_series_name(text)

            # Lesson number
            lesson_num = extract_lesson_number(text)

            # Audio filename
            audio = extract_audio_filename(div)

            # Duration
            duration = extract_duration(div, text)

            # Location
            location = extract_location(text)

            results.append({
                "msg_id": msg_id_int,
                "html_file": fname,
                "date_greg": date_greg,
                "hijri_date": hijri,
                "text": text,
                "series_name": series_name,
                "lesson_num": lesson_num,
                "audio": audio,
                "duration": duration,
                "location": location,
            })

    return results


# ---------------------------------------------------------------------------
# 3. DB-to-HTML series mapping
# ---------------------------------------------------------------------------

# Each entry: (db_series_name_pattern, html_match_patterns)
# html_match_patterns are substrings (normalised) to look for in the html series_name
# Mapping: (db_series_name, html_text_patterns, location_filter)
# location_filter: None = any, "عن بعد" = remote only, "in-person" = in-person only
# archive_filter: None = any, True = must have من_الأرشيف, False = must NOT have من_الأرشيف
# Each rule: (db_series_name, [html_patterns], location_filter, archive_filter)
_SERIES_MAP_RULES: list[tuple[str, list[str], str | None, bool | None]] = [
    # Non-variant series first (most specific match wins via order)
    ("الملخص شرح كتاب التوحيد",    ["الملخص في شرح", "الملخص شرح كتاب التوحيد", "الملخص_في_شرح"], None, None),
    ("الملخص الفقهي - أرشيف رمضان", ["الملخص الفقهي", "الملخص_الفقهي"], None, True),
    ("الملخص الفقهي",               ["الملخص الفقهي", "الملخص_الفقهي"], None, False),
    ("التفسير الميسر",               ["التفسير الميسر", "التفسير_الميسر"], None, None),
    ("إرشاد الساري شرح السنة للبربهاري", ["إرشاد الساري", "البربهاري"], None, None),
    ("صحيح البخاري",                ["صحيح البخاري", "صحيح_البخاري"], None, None),
    ("المورد العذب الزلال",          ["المورد العذب الزلال", "المورد_العذب_الزلال"], None, None),
    ("التحفة النجمية بشرح الأربعين النووية", ["الأربعين النووية", "التحفة النجمية", "الأربعين_النووية"], None, None),
    # Khutba/Seera: disambiguate by whether مختصر السيرة appears
    ("خطبة الجمعة - مختصر السيرة النبوية", ["مختصر السيرة", "مختصر_السيرة"], None, None),
    ("مختصر السيرة النبوية",         ["مختصر السيرة", "مختصر_السيرة"], None, None),
    # تنبيه
    ("تنبيه الأنام على ما في كتاب سبل السلام من الفوائد والأحكام - أرشيف رمضان", ["تنبيه"], None, True),
    ("تنبيه الانام على ما في كتاب سبل السلام من الفوائد والأحكام", ["تنبيه", "سبل_السلام"], None, False),
    # Khutba
    ("خطبة الجمعة - مختصر السيرة النبوية", ["مختصر السيرة"], None, None),
    ("خطب الجمعة",                   ["خطبة_الجمعة", "خطبة الجمعة"], None, None),
    ("محاضرات متفرقة",               ["جديد المحاضرات", "جديد_المحاضرات"], None, None),
    ("التعليقات البهية على الرسائل العقدية", ["التعليقات البهية", "التعليقات_البهية"], None, None),
    # الأفنان الندية: archive → أرشيف رمضان; remote → عن بعد; else → (none currently in DB without variant)
    ("الأفنان الندية - أرشيف رمضان", ["الأفنان الندية", "الأفنان_الندية"], None, True),
    ("الأفنان الندية - عن بعد",      ["الأفنان الندية", "الأفنان_الندية"], "عن بعد", False),
    # معارج القبول
    ("معارج القبول شرح منظومة سلم الوصول - عن بعد", ["معارج القبول", "معارج_القبول"], "عن بعد", None),
    # إتمام / شرح موجز
    ("إتمام المنة بشرح أصول السنة", ["إتمام المنة", "إتمام_المنة", "تمام المنة"], None, None),
    ("الشرح الموجز الممهد لتوحيد الخالق الممجد الذي ألفه شيخ الإسلام محمد",
                                      ["الشرح الموجز الممهد", "الموجز_الممهد"], None, None),
    # وزارة
    ("دروس رمضان - وزارة الشؤون الإسلامية", ["وزارة الشؤون الإسلامية", "وزارة_الشؤون_الإسلامية"], None, None),
    ("تيسير العلي القدير لاختصار تفسير ابن كثير", ["تيسير العلي القدير", "تيسير_العلي_القدير"], None, None),
    # تأسيس الأحكام variants — order matters: archive > remote > طهارة > main
    ("تأسيس الأحكام شرح عمدة الأحكام - أرشيف رمضان", ["تأسيس الاحكام", "تأسيس_الأحكام"], None, True),
    ("تأسيس الأحكام شرح عمدة الأحكام - عن بعد", ["تأسيس الاحكام", "تأسيس_الأحكام"], "عن بعد", False),
    # الطهارة sub-series: match ONLY messages that explicitly mention كتاب_الطهارة or الطهارة
    # NOTE: The الطهارة series (Oct 2025) and the main series (Dec 2025+, كتاب الصلاة onward)
    # are both in-person sessions; we route to الطهارة only if "الطهارة" appears in the text.
    ("تأسيس الأحكام شرح عمدة الأحكام - الطهارة", ["كتاب_الطهارة", "كتاب الطهارة"], "in-person", False),
    ("تأسيس الأحكام شرح عمدة الأحكام", ["تأسيس الاحكام", "تأسيس_الأحكام"], "in-person", False),
    # Archive series
    ("الممتع شرح زاد المستقنع - أرشيف رمضان", ["الممتع شرح زاد", "الممتع_شرح_زاد"], None, None),
    ("شرح كتاب الفقه الميسر - أرشيف رمضان", ["الفقه الميسر", "شرح_كتاب_الفقه_الميسر"], None, True),
    ("كتاب آداب المشي إلى الصلاة- كتاب_الصيام - أرشيف رمضان", ["آداب المشي", "آداب_المشي"], None, None),
    # NOTE: التفسير الميسر in DB ≠ تيسير العلي القدير (different books)
]

# SPECIAL NOTE: "التفسير الميسر" in DB maps to "التفسير الميسر" hashtag in HTML,
# but "تيسير العلي القدير" is a separate DB series that maps to "تيسير العلي القدير" in HTML.
# These are different books.

_TAFSEER_DIFFERENT_BOOKS_NOTE = (
    "NOTE: 'التفسير الميسر' (DB) is a different book from 'تيسير العلي القدير لاختصار تفسير ابن كثير' "
    "(also in DB). The DB contains both as separate series. In HTML, التفسير_الميسر hashtag "
    "corresponds to the DB series 'التفسير الميسر', while تيسير العلي القدير corresponds to the "
    "Ramadan series. These are NOT the same book."
)


def _msg_has_archive_marker(text: str) -> bool:
    return "من_الأرشيف" in text or "من الأرشيف" in text


def _msg_is_remote(text: str) -> bool:
    return "عن بُعد" in text or "عن بعد" in text


def build_series_mapping(
    db: dict, html_msgs: list[dict]
) -> tuple[dict, list[dict]]:
    """
    Returns:
      mapped: db_series_name -> list of matched HTML messages
      unmatched_html: HTML messages whose series_name didn't match any DB series

    Uses location and archive markers to pick the correct DB variant when multiple
    DB series match the same HTML series text pattern.
    """
    # Normalise DB series names for matching
    db_norm = {norm_arabic(k): k for k in db.keys()}

    # Build lookup: for each HTML message, which DB series does it belong to?
    mapped: dict = {k: [] for k in db.keys()}
    unmatched: list[dict] = []

    for msg in html_msgs:
        if msg["series_name"] is None:
            unmatched.append(msg)
            continue

        msg_series_norm = norm_arabic(msg["series_name"])
        text_norm = norm_arabic(msg["text"])
        is_remote = _msg_is_remote(msg["text"])
        is_archive = _msg_has_archive_marker(msg["text"])

        best_match: str | None = None

        # Try direct lookup rules (order matters: most specific first)
        for db_series_name, html_patterns, loc_filter, arch_filter in _SERIES_MAP_RULES:
            db_norm_key = norm_arabic(db_series_name)

            # Check location filter
            if loc_filter == "عن بعد" and not is_remote:
                continue
            if loc_filter == "in-person" and is_remote:
                continue

            # Check archive filter
            if arch_filter is True and not is_archive:
                continue
            if arch_filter is False and is_archive:
                continue

            # Check if any HTML pattern appears in the normalised message text or series name
            pattern_matched = False
            for pat in html_patterns:
                pat_norm = norm_arabic(pat)
                if pat_norm in msg_series_norm or pat_norm in text_norm:
                    pattern_matched = True
                    break

            if not pattern_matched:
                continue

            # Validate that this DB series actually exists
            actual_db_key = db_norm.get(db_norm_key)
            if actual_db_key is None:
                # Try prefix match
                for nk, ok in db_norm.items():
                    if db_norm_key in nk or nk in db_norm_key:
                        actual_db_key = ok
                        break
            if actual_db_key:
                best_match = actual_db_key
                break

        # Fallback: direct substring matching on DB series names
        if not best_match:
            for db_norm_key, original_key in db_norm.items():
                # Try if normalised db name appears in message text/series
                # Use key fragments (skip variant suffixes like " - عن بعد")
                base = re.sub(r"\s*[-–]\s*(عن بعد|ارشيف رمضان|الطهارة).*$", "", db_norm_key)
                if len(base) > 5 and (base in msg_series_norm or base in text_norm):
                    best_match = original_key
                    break

        if best_match:
            mapped[best_match].append(msg)
        else:
            unmatched.append(msg)

    return mapped, unmatched


# ---------------------------------------------------------------------------
# 4. Gap analysis
# ---------------------------------------------------------------------------

def analyse_gaps(db: dict, mapped: dict) -> dict:
    """
    For each DB series, compute:
      - db_first_date, db_last_date (from hijri_date of lectures)
      - continuations: HTML messages AFTER db_last_date
      - pre_db: HTML messages BEFORE db_first_date
    """
    results = {}

    for series_name, db_lectures in db.items():
        # Compute DB date range
        dates = [l["hijri_date"] for l in db_lectures if l["hijri_date"]]
        sort_keys = [hijri_to_sort_key(d) for d in dates]
        valid_keys = [k for k in sort_keys if k != (0, 0, 0)]

        if valid_keys:
            db_first_key = min(valid_keys)
            db_last_key = max(valid_keys)
        else:
            db_first_key = (0, 0, 0)
            db_last_key = (0, 0, 0)

        db_first_date = hijri_display(next((d for d in dates if hijri_to_sort_key(d) == db_first_key), ""))
        db_last_date = hijri_display(next((d for d in dates if hijri_to_sort_key(d) == db_last_key), ""))

        html_msgs = mapped.get(series_name, [])

        continuations = []
        pre_db = []
        within_db = []

        for msg in html_msgs:
            h = msg["hijri_date"]
            if not h:
                # Fall back to checking if the audio filename is already in DB
                audio_in_db = any(
                    l["audio"] == msg["audio"]
                    for l in db_lectures
                    if msg["audio"] and l["audio"]
                )
                if not audio_in_db:
                    continuations.append(msg)  # assume new if no date
                continue

            hk = hijri_to_sort_key(h)
            if hk == (0, 0, 0):
                continue

            if db_last_key != (0, 0, 0) and hk > db_last_key:
                continuations.append(msg)
            elif db_first_key != (0, 0, 0) and hk < db_first_key:
                pre_db.append(msg)
            else:
                within_db.append(msg)

        results[series_name] = {
            "db_count": len(db_lectures),
            "db_first_date": db_first_date,
            "db_last_date": db_last_date,
            "continuations": sorted(continuations, key=lambda m: (hijri_to_sort_key(m["hijri_date"]) if m["hijri_date"] else (9999,9,9))),
            "pre_db": sorted(pre_db, key=lambda m: hijri_to_sort_key(m["hijri_date"])),
            "within_db": within_db,
        }

    return results


def group_unmatched_by_series(unmatched: list[dict]) -> dict:
    """Group unmatched HTML messages by their extracted series_name."""
    groups: dict = defaultdict(list)
    for msg in unmatched:
        key = msg["series_name"] or "(no series name)"
        groups[key].append(msg)
    return dict(groups)


# ---------------------------------------------------------------------------
# 5. Report generation
# ---------------------------------------------------------------------------

def msg_line(msg: dict) -> str:
    """Format a single HTML message as a report line."""
    hd = msg["hijri_date"] or "—"
    ln = f"درس {msg['lesson_num']}" if msg["lesson_num"] else "درس (?)"
    audio = msg["audio"] or "—"
    greg = msg["date_greg"] or "—"
    loc = f" [{msg['location']}]" if msg["location"] else ""
    return f"    - msg {msg['msg_id']:>6} | hijri {hd} | {ln} | file {audio} | greg {greg}{loc}"


def write_report(
    gap_results: dict,
    unmatched_groups: dict,
    output_path: Path,
) -> None:
    lines = []

    # ── CONTINUATIONS ─────────────────────────────────────────────────────────
    lines.append("=" * 80)
    lines.append("=== CONTINUATIONS (existing DB series with new lessons in HTML files) ===")
    lines.append("=" * 80)
    lines.append("")

    cont_count = 0
    for series_name, info in sorted(gap_results.items(), key=lambda x: x[0]):
        cont = info["continuations"]
        if not cont:
            continue
        cont_count += 1
        lines.append(f"Series: {series_name}")
        lines.append(f"  DB: {info['db_count']} lessons, date range: {info['db_first_date']} → {info['db_last_date']}")
        lines.append(f"  New in HTML: {len(cont)} message(s)")
        for msg in cont:
            lines.append(msg_line(msg))
        lines.append("")

    if cont_count == 0:
        lines.append("  (none found)")
        lines.append("")

    # ── NEW SERIES ─────────────────────────────────────────────────────────────
    lines.append("=" * 80)
    lines.append("=== NEW SERIES (found in HTML but not matched to any DB series) ===")
    lines.append("=" * 80)
    lines.append("")

    new_series_sorted = sorted(
        unmatched_groups.items(),
        key=lambda kv: -len(kv[1])
    )

    for series_name, msgs in new_series_sorted:
        if series_name == "(no series name)":
            continue
        if len(msgs) < 2:  # skip noise
            continue
        html_files = sorted(set(m["html_file"] for m in msgs))
        hijri_dates = [hijri_to_sort_key(m["hijri_date"]) for m in msgs if m["hijri_date"]]
        hijri_dates_valid = [k for k in hijri_dates if k != (0, 0, 0)]
        if hijri_dates_valid:
            first_hijri = hijri_display(f"{min(hijri_dates_valid)[0]}/{min(hijri_dates_valid)[1]}/{min(hijri_dates_valid)[2]}")
            last_hijri  = hijri_display(f"{max(hijri_dates_valid)[0]}/{max(hijri_dates_valid)[1]}/{max(hijri_dates_valid)[2]}")
        else:
            first_hijri = last_hijri = "—"

        lines.append(f"Series: {series_name}")
        lines.append(f"  Source: {', '.join(html_files)}")
        lines.append(f"  Count: {len(msgs)}")
        lines.append(f"  Date range: {first_hijri} → {last_hijri}")
        lines.append("  Lessons:")
        for msg in sorted(msgs, key=lambda m: m["msg_id"]):
            lines.append(msg_line(msg))
        lines.append("")

    # Also list the "no series name" group
    no_name = unmatched_groups.get("(no series name)", [])
    if no_name:
        lines.append(f"  [Messages with no series name extracted: {len(no_name)}]")
        for msg in no_name[:5]:
            lines.append(msg_line(msg))
        if len(no_name) > 5:
            lines.append(f"    ... and {len(no_name) - 5} more")
        lines.append("")

    # ── OLD / PRE-DB ───────────────────────────────────────────────────────────
    lines.append("=" * 80)
    lines.append("=== OLD MESSAGES (DB series with HTML messages BEFORE first DB date) ===")
    lines.append("=" * 80)
    lines.append("")

    old_count = 0
    for series_name, info in sorted(gap_results.items(), key=lambda x: x[0]):
        pre = info["pre_db"]
        if not pre:
            continue
        old_count += 1
        lines.append(f"Series: {series_name}")
        lines.append(f"  DB: {info['db_count']} lessons, first DB date: {info['db_first_date']}")
        lines.append(f"  Pre-DB in HTML: {len(pre)} message(s)")
        for msg in pre:
            lines.append(msg_line(msg))
        lines.append("")

    if old_count == 0:
        lines.append("  (none found)")
        lines.append("")

    # ── SERIES WITH WITHIN-DB HTML MATCHES ────────────────────────────────────
    lines.append("=" * 80)
    lines.append("=== WITHIN-DB MATCHES (HTML messages in same date range as DB — already imported) ===")
    lines.append("=" * 80)
    lines.append("")
    for series_name, info in sorted(gap_results.items(), key=lambda x: x[0]):
        within = info["within_db"]
        cont = info["continuations"]
        pre = info["pre_db"]
        total_html = len(within) + len(cont) + len(pre)
        if total_html == 0:
            continue
        lines.append(
            f"  {series_name}: DB={info['db_count']}, "
            f"HTML total={total_html} "
            f"(within={len(within)}, new={len(cont)}, pre={len(pre)})"
        )
    lines.append("")

    # ── DB SERIES WITH ZERO HTML MATCHES ──────────────────────────────────────
    lines.append("=" * 80)
    lines.append("=== DB SERIES WITH NO HTML MESSAGES FOUND ===")
    lines.append("=" * 80)
    lines.append("")
    for series_name, info in sorted(gap_results.items(), key=lambda x: x[0]):
        total_html = len(info["within_db"]) + len(info["continuations"]) + len(info["pre_db"])
        if total_html == 0:
            lines.append(f"  {series_name} (DB: {info['db_count']} lessons, last: {info['db_last_date']})")
    lines.append("")

    # ── SPECIAL NOTES ──────────────────────────────────────────────────────────
    lines.append("=" * 80)
    lines.append("=== SPECIAL NOTES ===")
    lines.append("=" * 80)
    lines.append("")
    lines.append(_TAFSEER_DIFFERENT_BOOKS_NOTE)
    lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[report] Written to {output_path}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    sys.stdout.reconfigure(encoding="utf-8")

    db_path = ROOT / "data-export.txt"
    out_path = ROOT / "output" / "gap_analysis_report.txt"

    print("[1] Parsing DB ...", file=sys.stderr)
    db = parse_db(db_path)
    print(f"    {len(db)} series, {sum(len(v) for v in db.values())} lectures", file=sys.stderr)

    print("[2] Parsing HTML files ...", file=sys.stderr)
    html_msgs = parse_html_messages()
    print(f"    {len(html_msgs)} lesson messages found", file=sys.stderr)

    print("[3] Mapping DB series to HTML messages ...", file=sys.stderr)
    mapped, unmatched = build_series_mapping(db, html_msgs)
    total_mapped = sum(len(v) for v in mapped.values())
    print(f"    {total_mapped} mapped, {len(unmatched)} unmatched", file=sys.stderr)

    print("[4] Analysing gaps ...", file=sys.stderr)
    gap_results = analyse_gaps(db, mapped)

    total_cont = sum(len(v["continuations"]) for v in gap_results.values())
    total_pre  = sum(len(v["pre_db"]) for v in gap_results.values())
    print(f"    Continuations: {total_cont} messages across "
          f"{sum(1 for v in gap_results.values() if v['continuations'])} series",
          file=sys.stderr)
    print(f"    Pre-DB: {total_pre} messages", file=sys.stderr)

    unmatched_groups = group_unmatched_by_series(unmatched)
    new_series = {k: v for k, v in unmatched_groups.items() if k != "(no series name)" and len(v) >= 2}
    print(f"    New series: {len(new_series)} (with >=2 messages)", file=sys.stderr)

    print("[5] Writing report ...", file=sys.stderr)
    write_report(gap_results, unmatched_groups, out_path)

    # Print report to stdout
    print(out_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
