"""
Build Telegram-ready formatted indices for Islamic lecture series.
Parses messages.html–messages4.html for configured series, extracts lesson posts,
groups by old/new series (split at 2025), flags duplicates, and chunks output at
4,000 characters (Telegram message limit).

Usage:
    python scripts/build_series_index.py                  # all series → output/*.txt
    python scripts/build_series_index.py --series fiqh    # single series → stdout
    python scripts/build_series_index.py --series fatawa
    python scripts/build_series_index.py --series qawl
"""

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from bs4 import BeautifulSoup

ROOT = Path(__file__).parent.parent
CHANNEL = "daririhasan"

HTML_FILES = [
    ROOT / "messages.html",
    ROOT / "messages2.html",
    ROOT / "messages3.html",
    ROOT / "messages4.html",
]

# ---------------------------------------------------------------------------
# Arabic helpers
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
        return {"الحادي": 11, "الثاني": 12, "الثالث": 13, "الرابع": 14,
                "الخامس": 15, "السادس": 16, "السابع": 17, "الثامن": 18, "التاسع": 19}[m.group(1)]
    m = re.match(r"(\S+)\s+و(\S+)", word)
    if m:
        u = _UNITS.get(m.group(1), 0)
        t = _TENS.get("ال" + m.group(2).lstrip("ال"), _TENS.get(m.group(2).lstrip("ال"), 0))
        if u and t:
            return u + t
    return _TENS.get(word)


def extract_ordinal_lesson(text: str) -> int | None:
    text_norm = normalise_digits(text)
    m = re.search(r"الدرس\s+رقم\s*[:\-]?\s*(\d+)", text_norm)
    if m:
        return int(m.group(1))
    m = re.search(r"الدرس\s+[-–]?\s*(\d+)\s*[-–]?", text_norm)
    if m:
        return int(m.group(1))
    m = re.search(r"الدرس\s+([؀-ۿ\s]+?)(?:\s*[:\.\|]|\s*\n|$)", text, re.UNICODE)
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


# Circled digits ①②③… used in القول السديد series
_CIRCLED = {
    "⓪": 0, "①": 1, "②": 2, "③": 3, "④": 4,
    "⑤": 5, "⑥": 6, "⑦": 7, "⑧": 8, "⑨": 9,
    "⑩": 10, "⑪": 11, "⑫": 12, "⑬": 13, "⑭": 14,
    "⑮": 15, "⑯": 16, "⑰": 17, "⑱": 18, "⑲": 19, "⑳": 20,
}
_CIRCLED_RE = re.compile(r"الدرس\s+([⓪①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]+)")


def extract_circled_lesson(text: str) -> int | None:
    m = _CIRCLED_RE.search(text)
    if not m:
        return None
    chars = m.group(1)
    # Single pre-composed char (⑩–⑳)
    if len(chars) == 1:
        return _CIRCLED.get(chars)
    # Multi-char: digits written RTL (units first) → reverse for decimal
    digits = []
    for c in reversed(chars):
        d = _CIRCLED.get(c)
        if d is None:
            return None
        digits.append(str(d))
    try:
        return int("".join(digits))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Series configuration
# ---------------------------------------------------------------------------

@dataclass
class SeriesConfig:
    key: str
    title: str                          # display title
    hashtag_re: re.Pattern              # must match raw HTML of the post
    extract_lesson: Callable            # function(text: str) → int | None
    output_file: str                    # filename under output/


SERIES: dict[str, SeriesConfig] = {
    "fiqh": SeriesConfig(
        key="fiqh",
        title="شرح كتاب الفقه الميسر",
        hashtag_re=re.compile(r"شرح_كتاب_الفقه_الميسر"),
        extract_lesson=extract_ordinal_lesson,
        output_file="fiqh_muyassar_index.txt",
    ),
    "fatawa": SeriesConfig(
        key="fatawa",
        title="فتاوى أركان الإسلام",
        hashtag_re=re.compile(r"فتاوى_أركان_الإسلام"),
        extract_lesson=extract_ordinal_lesson,
        output_file="fatawa_arkan_index.txt",
    ),
    "qawl": SeriesConfig(
        key="qawl",
        title="القول السديد شرح كتاب التوحيد",
        hashtag_re=re.compile(r"القول_السديد_شرح_كتاب_التوحيد"),
        extract_lesson=extract_circled_lesson,
        output_file="qawl_sadeed_index.txt",
    ),
}

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_series(cfg: SeriesConfig) -> list[dict]:
    all_records: list[dict] = []
    for html_path in HTML_FILES:
        if not html_path.exists():
            print(f"WARNING: {html_path.name} not found — skipping", file=sys.stderr)
            continue
        with open(html_path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "lxml")
        for div in soup.select("div.message.default"):
            text_el = div.find("div", class_="text")
            if not text_el:
                continue
            raw_html = str(text_el)
            if not cfg.hashtag_re.search(raw_html):
                continue
            msg_id = div.get("id", "").replace("message", "").strip()
            if not msg_id:
                continue
            text = text_el.get_text(separator="\n", strip=True)
            date_el = div.find("div", class_="date")
            date_raw = ""
            if date_el and date_el.get("title"):
                date_raw = date_el["title"].split()[0]
            all_records.append({
                "msg_id": msg_id,
                "date_raw": date_raw,
                "lesson_num": cfg.extract_lesson(text),
                "link": f"https://t.me/{CHANNEL}/{msg_id}",
            })
    return all_records


# ---------------------------------------------------------------------------
# Duplicate flagging
# ---------------------------------------------------------------------------

def mark_duplicates(records: list[dict]) -> None:
    counts: dict[int, int] = {}
    for r in records:
        if r["lesson_num"] is not None:
            counts[r["lesson_num"]] = counts.get(r["lesson_num"], 0) + 1
    for r in records:
        r["is_dup"] = r["lesson_num"] is not None and counts.get(r["lesson_num"], 1) > 1


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
_ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"


def to_arabic_numeral(n: int) -> str:
    return "".join(_ARABIC_DIGITS[int(d)] for d in str(n))


def format_entry(sno: int, rec: dict) -> str:
    lesson = (
        f"الدرس رقم {rec['lesson_num']}"
        if rec["lesson_num"] is not None
        else "درس (رقم غير محدد)"
    )
    dup = " [DUPLICATE - MANUAL CHECK]" if rec["is_dup"] else ""
    return f"* {to_arabic_numeral(sno)}) {lesson} 📎 [{rec['link']}]{dup}"


def build_section(header: str, records: list[dict]) -> str:
    lines = [header, ""]
    for i, rec in enumerate(records, 1):
        lines.append(format_entry(i, rec))
    return "\n".join(lines)


def chunk_text(text: str, limit: int = 4000) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in text.split("\n"):
        line_len = len(line) + 1
        if current and current_len + line_len > limit:
            chunks.append("\n".join(current))
            current, current_len = [], 0
        current.append(line)
        current_len += line_len
    if current:
        chunks.append("\n".join(current))
    return chunks


# ---------------------------------------------------------------------------
# Build index for one series → string
# ---------------------------------------------------------------------------

def build_index(cfg: SeriesConfig) -> str:
    records = parse_series(cfg)
    print(
        f"[{cfg.key}] {len(records)} posts found",
        file=sys.stderr,
    )

    old_records = []
    new_records = []
    for rec in records:
        year = 0
        if rec["date_raw"]:
            try:
                year = int(rec["date_raw"].split(".")[-1])
            except ValueError:
                pass
        (new_records if year >= 2025 else old_records).append(rec)

    def sort_key(r):
        n = r["lesson_num"]
        return (0 if n is not None else 1, n if n is not None else 9999)

    old_records.sort(key=sort_key)
    new_records.sort(key=sort_key)
    mark_duplicates(old_records)
    mark_duplicates(new_records)

    old_dup = sum(1 for r in old_records if r["is_dup"])
    new_dup = sum(1 for r in new_records if r["is_dup"])
    print(
        f"[{cfg.key}] Old: {len(old_records)} posts ({old_dup} dups) | "
        f"New (2025+): {len(new_records)} posts ({new_dup} dups)",
        file=sys.stderr,
    )

    sections = []
    if old_records:
        header = (
            f"📚 فهرس دروس {cfg.title}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🎙 الشيخ حسن الدغريري\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )
        sections.append(build_section(header, old_records))
    if new_records:
        header = (
            f"📚 فهرس دروس {cfg.title} — المجموعة الجديدة (٢٠٢٥–٢٠٢٦)\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🎙 الشيخ حسن الدغريري\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )
        sections.append(build_section(header, new_records))

    full_text = "\n\n".join(sections)
    chunks = chunk_text(full_text)
    total = len(chunks)
    parts = []
    for i, chunk in enumerate(chunks, 1):
        label = f"\n\n📌 الجزء {to_arabic_numeral(i)} من {to_arabic_numeral(total)}" if total > 1 else ""
        parts.append(chunk + label)

    separator = "\n\n" + "═" * 30 + "\n\n"
    return separator.join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Build Telegram series index")
    parser.add_argument(
        "--series",
        choices=list(SERIES.keys()),
        help="Build index for a single series and print to stdout",
    )
    args = parser.parse_args()

    if args.series:
        cfg = SERIES[args.series]
        print(build_index(cfg))
    else:
        # Build all series → write to output/ files
        out_dir = ROOT / "output"
        out_dir.mkdir(exist_ok=True)
        for cfg in SERIES.values():
            text = build_index(cfg)
            out_path = out_dir / cfg.output_file
            out_path.write_text(text, encoding="utf-8")
            print(f"[{cfg.key}] Saved → output/{cfg.output_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
