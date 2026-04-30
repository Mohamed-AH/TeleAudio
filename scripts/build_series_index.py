"""
Build Telegram-ready formatted indices for Islamic lecture series.
Parses messages.html–messages4.html for configured series, extracts lesson posts,
groups by old/new series (split at 2025), flags duplicates, and chunks output at
4,000 characters (Telegram message limit).

Usage:
    python scripts/build_series_index.py                   # all series → output/*.txt
    python scripts/build_series_index.py --series fiqh     # single series → stdout
    python scripts/build_series_index.py --series fatawa
    python scripts/build_series_index.py --series qawl
    python scripts/build_series_index.py --series khutba
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from bs4 import BeautifulSoup
from hijridate import Gregorian

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
    if len(chars) == 1:
        return _CIRCLED.get(chars)
    # Multi-char RTL: digits stored units-first → reverse for decimal
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
# Khutba title extraction
# ---------------------------------------------------------------------------
_NOISE_LINE_RE = re.compile(
    r"^(https?://|🎙|🔸|🔹|📌|لفضيلة|للشيخ|للإستماع|حسن\s|غفر|⏰|🗓|🔗|🖇|══|╭|╰|رابط|00:00|\s*$)",
)
# Leading emoji / decorative chars to strip from titles
_LEAD_DECO_RE = re.compile(r"^[\U0001F300-\U0001FFFF☀-⟿🔖🌀🎗•✿❁]+\s*")
_TRAIL_DECO_RE = re.compile(r"[\s\-/•]+$")


def _clean_title(raw: str) -> str:
    t = raw.replace("\n", " ").strip()
    t = t.strip("•[]/◈")
    t = re.sub(r"[​-‏‪-‮⁦-⁩﻿]", "", t)   # direction marks
    t = _LEAD_DECO_RE.sub("", t)
    t = _TRAIL_DECO_RE.sub("", t)
    return t.strip()


def extract_khutba_title(text: str) -> str:
    # 1. •[ TITLE ]• or •/ TITLE /• — may span lines (2023+, some 2018-2019)
    m = re.search(r"•[\[/]\s*([\s\S]+?)\s*[\]/]•", text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 2. [◈ TITLE] — 2022 format
    m = re.search(r"\[◈\s*(.+?)\]", text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 3. عنوان الخطبة: \n TITLE — 2025-2026
    m = re.search(r"عنوان الخطبة\s*[:\s]*\n+\s*(.+)", text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 4. 🌀 TITLE 🌀 or 🎗 TITLE 🎗 — 2018-2021
    m = re.search(r"[🌀🎗]\s*(.+?)\s*[🌀🎗]", text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 5. «TITLE» Arabic quotes — 2017
    m = re.search(r'[«"]\s*(.+?)\s*[»"]', text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 6. Title on same line BEFORE خطبة keyword, or on the immediately preceding line
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(r"خطبة.{0,6}(الجمعة|جمعة)", line):
            # Same line: text before keyword
            before = re.split(r"خطبة.{0,6}(الجمعة|جمعة)", line)[0].strip()
            before = _clean_title(before)
            if len(before) > 5 and not before.startswith("http"):
                return before
            # Previous line as title
            if i > 0:
                prev = _clean_title(lines[i - 1].strip())
                if len(prev) > 3 and not _NOISE_LINE_RE.match(lines[i - 1].strip()):
                    return prev
            break
    lines = text.split("\n")  # reset for pattern 7

    # 7. First meaningful line after خطبة keyword, collecting multi-line titles
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(r"خطبة.{0,6}(الجمعة|جمعة)", line):
            # Collect up to 2 non-noise lines to handle split titles
            title_parts: list[str] = []
            for j in range(i + 1, min(i + 5, len(lines))):
                candidate = lines[j].strip()
                if not candidate or _NOISE_LINE_RE.match(candidate):
                    if title_parts:
                        break
                    continue
                cleaned = _clean_title(candidate)
                if len(cleaned) > 3:
                    title_parts.append(cleaned)
                    # Only grab 2nd line if it looks like a title continuation (no emoji start)
                    if len(title_parts) == 1 and not _LEAD_DECO_RE.match(candidate):
                        next_j = j + 1
                        if next_j < len(lines):
                            nxt = _clean_title(lines[next_j].strip())
                            if (len(nxt) > 3
                                    and not _NOISE_LINE_RE.match(lines[next_j].strip())
                                    and re.search(r"[اإأآ-ي]", nxt)
                                    and not re.search(r"[🎙🔸🔹]", lines[next_j])):
                                title_parts.append(nxt)
                    break
            if title_parts:
                return " ".join(title_parts)
            break

    return ""


# ---------------------------------------------------------------------------
# Series configuration
# ---------------------------------------------------------------------------

@dataclass
class SeriesConfig:
    key: str
    title: str                                   # display title for header
    hashtag_re: re.Pattern                       # must match raw HTML of the post
    output_file: str                             # filename under output/
    extract_lesson: Callable | None = None       # text → int | None (lesson series)
    extract_title: Callable | None = None        # text → str (khutba / titled series)
    sort_by_date: bool = False                   # True → sort by date, False → lesson num


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
    "khutba": SeriesConfig(
        key="khutba",
        title="خطبة الجمعة",
        # خطبة_الجمعة hashtag anywhere, OR خطبة (الجمعة|جمعة) within first 120 raw chars
        # (older posts open with the title immediately; false positives cite it much later)
        hashtag_re=re.compile(
            r"خطبة_الجمعة|(?:^[\s\S]{0,120}خطبة.{0,4}(?:الجمعة|جمعة))"
        ),
        extract_title=extract_khutba_title,
        sort_by_date=True,
        output_file="khutba_index.txt",
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

            rec: dict = {
                "msg_id": msg_id,
                "date_raw": date_raw,
                "link": f"https://t.me/{CHANNEL}/{msg_id}",
                "lesson_num": cfg.extract_lesson(text) if cfg.extract_lesson else None,
                "title": cfg.extract_title(text) if cfg.extract_title else "",
                "is_dup": False,
            }
            all_records.append(rec)
    return all_records


# ---------------------------------------------------------------------------
# Duplicate flagging (lesson-number series only)
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


def to_hijri_display(date_raw: str) -> str:
    """Convert DD.MM.YYYY Gregorian to 'DD MonthNameAr YYYYهـ' (Umm al-Qura)."""
    if not date_raw:
        return "—"
    try:
        dd, mm, yyyy = map(int, date_raw.split("."))
        h = Gregorian(yyyy, mm, dd).to_hijri()
        day_ar = to_arabic_numeral(h.day)
        year_ar = to_arabic_numeral(h.year)
        return f"{day_ar} {h.month_name('ar')} {year_ar}هـ"
    except Exception:
        return date_raw


def format_entry(sno: int, rec: dict) -> str:
    num = to_arabic_numeral(sno)
    if rec.get("title"):
        # Khutba / titled series: show title + Hijri date
        date_display = to_hijri_display(rec["date_raw"])
        return f"* {num}) {rec['title']} | {date_display} 📎 [{rec['link']}]"
    else:
        # Lesson-number series
        lesson = (
            f"الدرس رقم {rec['lesson_num']}"
            if rec["lesson_num"] is not None
            else "درس (رقم غير محدد)"
        )
        dup = " [DUPLICATE - MANUAL CHECK]" if rec["is_dup"] else ""
        return f"* {num}) {lesson} 📎 [{rec['link']}]{dup}"


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

def _date_sort_key(r: dict) -> tuple:
    d = r["date_raw"]
    if d:
        try:
            dd, mm, yyyy = d.split(".")
            return (int(yyyy), int(mm), int(dd))
        except ValueError:
            pass
    return (9999, 99, 99)


def build_index(cfg: SeriesConfig) -> str:
    records = parse_series(cfg)
    print(f"[{cfg.key}] {len(records)} posts found", file=sys.stderr)

    old_records: list[dict] = []
    new_records: list[dict] = []
    for rec in records:
        year = 0
        if rec["date_raw"]:
            try:
                year = int(rec["date_raw"].split(".")[-1])
            except ValueError:
                pass
        (new_records if year >= 2025 else old_records).append(rec)

    if cfg.sort_by_date:
        old_records.sort(key=_date_sort_key)
        new_records.sort(key=_date_sort_key)
    else:
        def lesson_sort_key(r: dict):
            n = r["lesson_num"]
            return (0 if n is not None else 1, n if n is not None else 9999)
        old_records.sort(key=lesson_sort_key)
        new_records.sort(key=lesson_sort_key)
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
            f"📚 فهرس {cfg.title}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🎙 الشيخ حسن الدغريري\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )
        sections.append(build_section(header, old_records))
    if new_records:
        header = (
            f"📚 فهرس {cfg.title} — ٢٠٢٥–٢٠٢٦\n"
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
        out_dir = ROOT / "output"
        out_dir.mkdir(exist_ok=True)
        for cfg in SERIES.values():
            text = build_index(cfg)
            out_path = out_dir / cfg.output_file
            out_path.write_text(text, encoding="utf-8")
            print(f"[{cfg.key}] Saved → output/{cfg.output_file}", file=sys.stderr)


if __name__ == "__main__":
    main()
