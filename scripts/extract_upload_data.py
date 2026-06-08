#!/usr/bin/env python3
"""Extract lesson title, Gregorian date, and audio filename for a specific series."""

import sys
import re
from pathlib import Path
from bs4 import BeautifulSoup

ROOT = Path(__file__).parent.parent

HTML_FILES = [
    ROOT / "messages.html",
    ROOT / "messages2.html",
    ROOT / "messages3.html",
    ROOT / "messages4.html",
    ROOT / "messages_new.html",
    ROOT / "messages_june1.html",
]

CHANNEL = "daririhasan"

# ── Arabic ordinals → integer ────────────────────────────────────────────────
ORDINALS = {
    "الأول": 1, "الثاني": 2, "الثالث": 3, "الرابع": 4, "الخامس": 5,
    "السادس": 6, "السابع": 7, "الثامن": 8, "التاسع": 9, "العاشر": 10,
    "الحادي عشر": 11, "الثاني عشر": 12, "الثالث عشر": 13,
    "الرابع عشر": 14, "الخامس عشر": 15, "السادس عشر": 16,
    "السابع عشر": 17, "الثامن عشر": 18, "التاسع عشر": 19,
    "العشرون": 20, "العشرين": 20,
    "الحادي والعشرون": 21, "الثاني والعشرون": 22, "الثالث والعشرون": 23,
    "الرابع والعشرون": 24, "الخامس والعشرون": 25, "السادس والعشرون": 26,
    "السابع والعشرون": 27, "الثامن والعشرون": 28, "التاسع والعشرون": 29,
    "الثلاثون": 30, "الثلاثين": 30,
    "الحادي والثلاثون": 31, "الثاني والثلاثون": 32, "الثالث والثلاثون": 33,
    "الرابع والثلاثون": 34, "الخامس والثلاثون": 35, "السادس والثلاثون": 36,
    "السابع والثلاثون": 37, "الثامن والثلاثون": 38, "التاسع والثلاثون": 39,
    "الأربعون": 40, "الأربعين": 40,
    "الحادي والأربعون": 41, "الثاني والأربعون": 42, "الثالث والأربعون": 43,
    "الرابع والأربعون": 44, "الخامس والأربعون": 45, "السادس والأربعون": 46,
    "السابع والأربعون": 47, "الثامن والأربعون": 48, "التاسع والأربعون": 49,
    "الخمسون": 50, "الخمسين": 50, "الحادي والخمسون": 51,
}


def extract_lesson_num(text: str) -> int | None:
    # "الدرس رقم 37" or "الدرس 37"
    m = re.search(r"الدرس\s+رقم\s+(\d+)", text)
    if m:
        return int(m.group(1))
    m = re.search(r"الدرس\s+(\d+)", text)
    if m:
        return int(m.group(1))
    # ordinals: longest match first
    for word, num in sorted(ORDINALS.items(), key=lambda x: -len(x[0])):
        if word in text:
            return num
    return None


def format_greg_date(date_raw: str) -> str:
    """Convert DD.MM.YYYY → DD/MM/YYYY (matching Excel sample format)."""
    if not date_raw:
        return ""
    parts = date_raw.split(".")
    if len(parts) == 3:
        return f"{parts[0]}/{parts[1]}/{parts[2]}"
    return date_raw


def load_messages(match_re: re.Pattern, exclude_re: re.Pattern | None = None,
                  also_match_re: re.Pattern | None = None) -> list[dict]:
    results = []
    seen_ids = set()

    for html_path in HTML_FILES:
        if not html_path.exists():
            continue
        soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "lxml")

        for div in soup.select("div.message.default"):
            msg_id_str = div.get("id", "").replace("message", "").strip()
            if not msg_id_str:
                continue
            try:
                msg_id = int(msg_id_str)
            except ValueError:
                continue
            if msg_id in seen_ids:
                continue

            text_el = div.find("div", class_="text")
            if not text_el:
                continue

            raw_html = str(text_el)
            if not match_re.search(raw_html):
                continue
            if also_match_re and not also_match_re.search(raw_html):
                continue
            if exclude_re and exclude_re.search(raw_html):
                continue

            seen_ids.add(msg_id)
            text = text_el.get_text(separator="\n", strip=True)

            # Date from div.date (with title attribute)
            date_el = div.find("div", class_="date")
            date_raw = ""
            if date_el and date_el.get("title"):
                date_raw = date_el["title"].split()[0]  # DD.MM.YYYY

            # Audio filename from media_audio_file link
            audio_link = div.find("a", class_=lambda c: c and "media_audio_file" in c)
            audio_file = ""
            if audio_link:
                href = audio_link.get("href", "")
                audio_file = Path(href).name

            lesson_num = extract_lesson_num(text)
            greg_date = format_greg_date(date_raw)

            results.append({
                "msg_id": msg_id,
                "lesson_num": lesson_num,
                "date": greg_date,
                "audio": audio_file,
                "text": text,
                "link": f"https://t.me/{CHANNEL}/{msg_id}",
            })

    results.sort(key=lambda x: x["msg_id"])
    return results


def main():
    sys.stdout.reconfigure(encoding="utf-8")

    # ── الملخص شرح كتاب التوحيد ─────────────────────────────────────────────
    match_re = re.compile(
        r"الملخص_في_شرح|الملخص_شرح|#الملخص_في_شرح|#كتاب_التوحيد"
    )

    records = load_messages(match_re)
    print(f"Found {len(records)} messages for الملخص شرح كتاب التوحيد")
    print()
    print(f"{'#':<4} {'Msg':>6} {'Lesson':>6}  {'Date':>12}  {'Audio File':<40}  Title snippet")
    print("-" * 120)

    for i, r in enumerate(records, 1):
        lesson = str(r["lesson_num"]) if r["lesson_num"] else "?"
        # Extract title line (first meaningful line of text)
        title_lines = [ln.strip() for ln in r["text"].split("\n")
                       if ln.strip() and len(ln.strip()) > 2
                       and not ln.strip().startswith("http")
                       and "الدرس" in ln.strip()]
        title = title_lines[0] if title_lines else r["text"].split("\n")[0][:60]
        print(f"{i:<4} {r['msg_id']:>6} {lesson:>6}  {r['date']:>12}  {r['audio']:<40}  {title[:60]}")

    # Write upload data file
    out_path = ROOT / "output" / "corrected" / "mulakhkhas_tawheed_upload_data.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("الملخص في شرح كتاب التوحيد — Upload Data\n")
        f.write("DB has 36 lessons (sort orders 0–35). The following are ALL records from HTML.\n")
        f.write("Records from #37 onwards need to be uploaded.\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"{'Serial':<8} {'Msg ID':>8} {'Lesson':>7}  {'Gregorian Date':>14}  {'Audio Filename'}\n")
        f.write("-" * 80 + "\n")
        for i, r in enumerate(records, 1):
            lesson = str(r["lesson_num"]) if r["lesson_num"] else "?"
            f.write(f"{i:<8} {r['msg_id']:>8} {lesson:>7}  {r['date']:>14}  {r['audio']}\n")
        f.write("\n\n")
        f.write("=" * 80 + "\n")
        f.write("FULL TEXT OF EACH MESSAGE (for title extraction)\n")
        f.write("=" * 80 + "\n\n")
        for i, r in enumerate(records, 1):
            lesson = str(r["lesson_num"]) if r["lesson_num"] else "?"
            f.write(f"--- Record {i} | Msg {r['msg_id']} | Lesson {lesson} | {r['date']} ---\n")
            f.write(f"Audio: {r['audio']}\n")
            f.write(f"Link:  {r['link']}\n")
            f.write(f"Text:\n{r['text']}\n\n")

    print(f"\nFull data written to: {out_path}")


if __name__ == "__main__":
    main()
