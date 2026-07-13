"""
Build a Telegram-ready formatted index for the series شرح كتاب الفقه الميسر.
Parses messages.html through messages4.html, extracts matching lesson posts,
groups them into old series (pre-2025) and new series (2025+), flags duplicates,
and chunks the output at 4,000 characters (Telegram message limit).

Usage:
    python scripts/build_fiqh_index.py
"""

import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup

ROOT = Path(__file__).parent.parent

HTML_FILES = [
    ROOT / "messages.html",
    ROOT / "messages2.html",
    ROOT / "messages3.html",
    ROOT / "messages4.html",
]

CHANNEL = "daririhasan"
SERIES_TITLE = "شرح كتاب الفقه الميسر"

# Must contain this hashtag (with underscores) to be a genuine series post
_SERIES_HASHTAG_RE = re.compile(r"شرح_كتاب_الفقه_الميسر", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Arabic helpers (mirror extract_metadata.py)
# ---------------------------------------------------------------------------
_ARABIC_INDIC = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def normalise_digits(text: str) -> str:
    return text.translate(_ARABIC_INDIC)


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
    word = word.strip()
    if word in _UNITS:
        return _UNITS[word]
    m = re.match(
        r"(الحادي|الثاني|الثالث|الرابع|الخامس|السادس|السابع|الثامن|التاسع)\s+عشر",
        word,
    )
    if m:
        unit_map = {
            "الحادي": 11, "الثاني": 12, "الثالث": 13, "الرابع": 14,
            "الخامس": 15, "السادس": 16, "السابع": 17, "الثامن": 18, "التاسع": 19,
        }
        return unit_map.get(m.group(1))
    m = re.match(r"(\S+)\s+و(\S+)", word)
    if m:
        unit_part = m.group(1)
        tens_part = m.group(2).lstrip("ال")
        u = _UNITS.get(unit_part, 0)
        t = _TENS.get("ال" + tens_part, _TENS.get(tens_part, 0))
        if u and t:
            return u + t
    for k, v in _TENS.items():
        if word == k:
            return v
    return None


def extract_lesson_number(text: str) -> int | None:
    """Return lesson number as integer, or None if not found."""
    text_norm = normalise_digits(text)

    # "الدرس رقم 107"
    m = re.search(r"الدرس\s+رقم\s*[:\-]?\s*(\d+)", text_norm)
    if m:
        return int(m.group(1))

    # "الدرس - 45 -" or "الدرس ٤٥"
    m = re.search(r"الدرس\s+[-–]?\s*(\d+)\s*[-–]?", text_norm)
    if m:
        return int(m.group(1))

    # Ordinal: "الدرس الأول" / "الدرس الثامن والثلاثون"
    m = re.search(
        r"الدرس\s+([؀-ۿ\s]+?)(?:\s*[:\.\|]|\s*\n|$)",
        text,
        re.UNICODE,
    )
    if m:
        ordinal_text = m.group(1).strip()
        num = ordinal_to_int(ordinal_text)
        if num:
            return num
        for word in ordinal_text.split():
            n = ordinal_to_int(word)
            if n:
                return n

    return None


# ---------------------------------------------------------------------------
# Arabic integer → Arabic-Indic numeral string for display
# ---------------------------------------------------------------------------
def to_arabic_numeral(n: int) -> str:
    arabic_digits = "٠١٢٣٤٥٦٧٨٩"
    return "".join(arabic_digits[int(d)] for d in str(n))


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_file(html_path: Path) -> list[dict]:
    with open(html_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "lxml")

    records = []
    for div in soup.select("div.message.default"):
        text_el = div.find("div", class_="text")
        if not text_el:
            continue

        raw_html = str(text_el)
        # Must contain the series hashtag (genuine lesson post)
        if not _SERIES_HASHTAG_RE.search(raw_html):
            continue

        text = text_el.get_text(separator="\n", strip=True)
        msg_id_raw = div.get("id", "")
        msg_id = msg_id_raw.replace("message", "").strip()
        if not msg_id:
            continue

        date_el = div.find("div", class_="date")
        date_raw = ""
        if date_el and date_el.get("title"):
            date_raw = date_el["title"].split()[0]  # "DD.MM.YYYY"

        lesson_num = extract_lesson_number(text)
        has_audio = bool(div.find("a", class_=re.compile(r"media_audio_file")))

        records.append({
            "source_file": html_path.name,
            "msg_id": msg_id,
            "date_raw": date_raw,
            "lesson_num": lesson_num,
            "has_audio": has_audio,
            "link": f"https://t.me/{CHANNEL}/{msg_id}",
        })

    return records


def collect_all() -> list[dict]:
    all_records = []
    for html_file in HTML_FILES:
        if not html_file.exists():
            print(f"WARNING: {html_file.name} not found — skipping", file=sys.stderr)
            continue
        recs = parse_file(html_file)
        all_records.extend(recs)
    return all_records


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

def mark_duplicates(records: list[dict]) -> list[dict]:
    """Add 'is_dup' flag to records sharing the same lesson_num."""
    seen: dict[int, int] = {}  # lesson_num → count
    for r in records:
        if r["lesson_num"] is not None:
            seen[r["lesson_num"]] = seen.get(r["lesson_num"], 0) + 1
    for r in records:
        r["is_dup"] = r["lesson_num"] is not None and seen.get(r["lesson_num"], 1) > 1
    return records


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_entry(sno: int, rec: dict) -> str:
    num_arabic = to_arabic_numeral(sno)
    lesson_part = (
        f"الدرس رقم {rec['lesson_num']}" if rec["lesson_num"] is not None
        else "درس (رقم غير محدد)"
    )
    dup_flag = " [DUPLICATE - MANUAL CHECK]" if rec["is_dup"] else ""
    return f"* {num_arabic}) {lesson_part} 📎 [{rec['link']}]{dup_flag}"


def build_section(header: str, records: list[dict]) -> str:
    if not records:
        return ""
    lines = [header, ""]
    for i, rec in enumerate(records, 1):
        lines.append(format_entry(i, rec))
    return "\n".join(lines)


def chunk_text(text: str, limit: int = 4000) -> list[str]:
    """Split text into chunks at newline boundaries respecting Telegram limit."""
    chunks: list[str] = []
    current_lines: list[str] = []
    current_len = 0

    for line in text.split("\n"):
        # +1 for newline
        line_len = len(line) + 1
        if current_lines and current_len + line_len > limit:
            chunks.append("\n".join(current_lines))
            current_lines = []
            current_len = 0
        current_lines.append(line)
        current_len += line_len

    if current_lines:
        chunks.append("\n".join(current_lines))

    return chunks


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    sys.stdout.reconfigure(encoding="utf-8")

    all_records = collect_all()
    print(f"[INFO] Total series posts found: {len(all_records)}", file=sys.stderr)

    # Split by series (pre-2025 = old, 2025+ = new)
    old_records: list[dict] = []
    new_records: list[dict] = []
    for rec in all_records:
        year = 0
        if rec["date_raw"]:
            try:
                year = int(rec["date_raw"].split(".")[-1])
            except ValueError:
                pass
        if year >= 2025:
            new_records.append(rec)
        else:
            old_records.append(rec)

    # Sort each group: known lesson numbers first (ascending), then unknowns
    def sort_key(r: dict):
        n = r["lesson_num"]
        return (0 if n is not None else 1, n if n is not None else 9999)

    old_records.sort(key=sort_key)
    new_records.sort(key=sort_key)

    # Mark duplicates within each group independently
    mark_duplicates(old_records)
    mark_duplicates(new_records)

    old_dup = sum(1 for r in old_records if r["is_dup"])
    new_dup = sum(1 for r in new_records if r["is_dup"])
    print(
        f"[INFO] Old series: {len(old_records)} posts ({old_dup} duplicates) | "
        f"New series (2025+): {len(new_records)} posts ({new_dup} duplicates)",
        file=sys.stderr,
    )

    # Build text
    old_header = (
        f"📚 فهرس دروس {SERIES_TITLE}\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎙 الشيخ حسن الدغريري\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )
    new_header = (
        f"📚 فهرس دروس {SERIES_TITLE} — المجموعة الجديدة (٢٠٢٥–٢٠٢٦)\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎙 الشيخ حسن الدغريري\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )

    sections: list[str] = []
    if old_records:
        sections.append(build_section(old_header, old_records))
    if new_records:
        sections.append(build_section(new_header, new_records))

    full_text = "\n\n".join(sections)

    # Chunk and print
    chunks = chunk_text(full_text)
    total = len(chunks)
    for i, chunk in enumerate(chunks, 1):
        if total > 1:
            part_label = f"\n\n📌 الجزء {to_arabic_numeral(i)} من {to_arabic_numeral(total)}"
            print(chunk + part_label)
        else:
            print(chunk)
        if i < total:
            print("\n" + "═" * 30 + "\n")


if __name__ == "__main__":
    main()
