"""Build Telegram-ready formatted indices for Islamic lecture series.
Parses messages.html-messages4.html for all configured series, extracts lesson
posts, groups by period (pre-2025 / 2025+), flags duplicates, and chunks output
at 4,000 characters (Telegram message limit).

Usage:
    python scripts/build_series_index.py                   # all series -> output/*.txt
    python scripts/build_series_index.py --series fiqh     # single series -> stdout
    python scripts/build_series_index.py --report          # report only (no index files)
"""

import argparse
import re
import sys
from dataclasses import dataclass
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
    "الأول": 1, "الاول": 1,  # with and without hamza
    "الثاني": 2, "الثالث": 3, "الرابع": 4, "الخامس": 5,
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
        return {
            "الحادي": 11, "الثاني": 12, "الثالث": 13, "الرابع": 14,
            "الخامس": 15, "السادس": 16, "السابع": 17, "الثامن": 18, "التاسع": 19,
        }[m.group(1)]
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


# Circled digits ①②③ used in القول السديد series (RTL encoded, units-first)
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
# Title extraction
# ---------------------------------------------------------------------------
_NOISE_LINE_RE = re.compile(
    r"^(https?://|🎙|🔸|🔹|📌|لفضيلة|للشيخ|للإستماع|حسن\s|غفر|⏰|🗓|🔗|🖇|══|╭|╰|رابط|00:00|\s*$)",
)
_LEAD_DECO_RE = re.compile(r"^[\U0001F300-\U0001FFFF☀-⟿🔖🌀🎗•✿❁]+\s*")
_TRAIL_DECO_RE = re.compile(r"[\s\-/•]+$")


def _clean_title(raw: str) -> str:
    t = raw.replace("\n", " ").strip()
    t = t.strip("•[]/◈")
    t = re.sub(r"[​-‏‪-‮⁦-⁩﻿]", "", t)
    t = _LEAD_DECO_RE.sub("", t)
    t = _TRAIL_DECO_RE.sub("", t)
    return t.strip()


def extract_khutba_title(text: str) -> str:
    # 1. •[ TITLE ]• or •/ TITLE /•
    m = re.search(r"•[\[/]\s*([\s\S]+?)\s*[\]/]•", text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 2. [◈ TITLE]
    m = re.search(r"\[◈\s*(.+?)\]", text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 3. عنوان الخطبة: \n TITLE
    m = re.search(r"عنوان الخطبة\s*[:\s]*\n+\s*(.+)", text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 4. 🌀 TITLE 🌀 or 🎗 TITLE 🎗
    m = re.search(r"[🌀🎗]\s*(.+?)\s*[🌀🎗]", text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 5. «TITLE» Arabic quotes
    m = re.search(r'[«"]\s*(.+?)\s*[»"]', text)
    if m:
        t = _clean_title(m.group(1))
        if len(t) > 3:
            return t

    # 6. Title on same line before or on immediately preceding line
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(r"خطبة.{0,6}(الجمعة|جمعة)", line):
            before = re.split(r"خطبة.{0,6}(الجمعة|جمعة)", line)[0].strip()
            before = _clean_title(before)
            if len(before) > 5 and not before.startswith("http"):
                return before
            if i > 0:
                prev = _clean_title(lines[i - 1].strip())
                if len(prev) > 3 and not _NOISE_LINE_RE.match(lines[i - 1].strip()):
                    return prev
            break

    # 7. First meaningful line after خطبة keyword
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(r"خطبة.{0,6}(الجمعة|جمعة)", line):
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


def extract_lecture_title(text: str) -> str:
    """Return first meaningful non-noise line as lecture title."""
    for line in text.split("\n"):
        line = line.strip()
        if not line or _NOISE_LINE_RE.match(line):
            continue
        t = _clean_title(line)
        if len(t) > 5 and re.search(r"[اإأآ-ي]", t):
            return t
    return ""


# ---------------------------------------------------------------------------
# Series configuration
# ---------------------------------------------------------------------------

@dataclass
class SeriesConfig:
    key: str
    title: str
    match_re: re.Pattern
    output_file: str
    also_match_re: re.Pattern | None = None
    exclude_re: re.Pattern | None = None
    extract_lesson: Callable | None = None
    extract_title: Callable | None = None
    sort_by_date: bool = False


SERIES: dict[str, SeriesConfig] = {
    # ── Previously generated ──────────────────────────────────────────────
    "fiqh": SeriesConfig(
        key="fiqh",
        title="شرح كتاب الفقه الميسر",
        match_re=re.compile(r"شرح_كتاب_الفقه_الميسر"),
        exclude_re=re.compile(r"من_الأرشيف"),
        extract_lesson=extract_ordinal_lesson,
        output_file="fiqh_muyassar_index.txt",
    ),
    "fatawa": SeriesConfig(
        key="fatawa",
        title="فتاوى أركان الإسلام",
        match_re=re.compile(r"فتاوى_أركان_الإسلام"),
        extract_lesson=extract_ordinal_lesson,
        output_file="fatawa_arkan_index.txt",
    ),
    "qawl": SeriesConfig(
        key="qawl",
        title="القول السديد شرح كتاب التوحيد",
        match_re=re.compile(r"القول_السديد_شرح_كتاب_التوحيد"),
        extract_lesson=extract_circled_lesson,
        output_file="qawl_sadeed_index.txt",
    ),
    "khutba": SeriesConfig(
        key="khutba",
        title="خطبة الجمعة",
        match_re=re.compile(
            r"خطبة_الجمعة|(?:^[\s\S]{0,120}خطبة.{0,4}(?:الجمعة|جمعة))"
        ),
        extract_title=extract_khutba_title,
        sort_by_date=True,
        output_file="khutba_index.txt",
    ),

    # ── User-specified series 1–29 ─────────────────────────────────────────

    # 1. تنبيه الأنام على ما في كتاب سبل السلام
    "tanbeeh": SeriesConfig(
        key="tanbeeh",
        title="تنبيه الأنام على ما في كتاب سبل السلام من الفوائد والأحكام",
        match_re=re.compile(r"تنبيه.*الأنام|سبل_السلام"),
        exclude_re=re.compile(r"من_الأرشيف"),
        extract_lesson=extract_ordinal_lesson,
        output_file="tanbeeh_anam_index.txt",
    ),

    # 2. التعليقات البهية على الرسائل العقدية
    "ta3leeqat_bahiyya": SeriesConfig(
        key="ta3leeqat_bahiyya",
        title="التعليقات البهية على الرسائل العقدية",
        match_re=re.compile(r"التعليقات[\s_]البهية"),
        extract_lesson=extract_ordinal_lesson,
        output_file="ta3leeqat_bahiyya_index.txt",
    ),

    # 3. تأسيس الأحكام — عن بعد: NOT FOUND in data (0 hits)

    # 4. تأسيس الأحكام — أرشيف رمضان
    "ta2sees_archive": SeriesConfig(
        key="ta2sees_archive",
        title="تأسيس الأحكام شرح عمدة الأحكام — أرشيف رمضان",
        match_re=re.compile(r"تأسيس_الأحكام"),
        also_match_re=re.compile(r"من_الأرشيف"),
        extract_lesson=extract_ordinal_lesson,
        output_file="ta2sees_archive_index.txt",
    ),

    # 5. التعليق على كتاب مجالس شهر رمضان
    "majaalis": SeriesConfig(
        key="majaalis",
        title="التعليق على كتاب مجالس شهر رمضان",
        match_re=re.compile(r"كتاب_مجالس_شهر_رمضان"),
        extract_lesson=extract_ordinal_lesson,
        output_file="majaalis_ramadan_index.txt",
    ),

    # 6. الملخص الفقهي (main, no archive)
    "mulakhkhas_fiqhi": SeriesConfig(
        key="mulakhkhas_fiqhi",
        title="الملخص الفقهي",
        match_re=re.compile(r"الملخص[\s_]الفقهي"),
        exclude_re=re.compile(r"من_الأرشيف"),
        extract_lesson=extract_ordinal_lesson,
        output_file="mulakhkhas_fiqhi_index.txt",
    ),

    # 7. صحيح البخاري
    "bukhari": SeriesConfig(
        key="bukhari",
        title="صحيح البخاري",
        match_re=re.compile(r"صحيح[\s_]البخاري"),
        extract_lesson=extract_ordinal_lesson,
        output_file="bukhari_index.txt",
    ),

    # 8. خطبة الجمعة — مختصر السيرة النبوية
    "khutba_seera": SeriesConfig(
        key="khutba_seera",
        title="خطبة الجمعة — مختصر السيرة النبوية",
        match_re=re.compile(
            r"خطبة_الجمعة|(?:^[\s\S]{0,120}خطبة.{0,4}(?:الجمعة|جمعة))"
        ),
        also_match_re=re.compile(r"مختصر[\s_]السير"),
        extract_title=extract_khutba_title,
        sort_by_date=True,
        output_file="khutba_seera_index.txt",
    ),

    # 9. مختصر السيرة النبوية (standalone lessons, not khutba)
    "mukhtasar_seera": SeriesConfig(
        key="mukhtasar_seera",
        title="مختصر السيرة النبوية",
        match_re=re.compile(r"مختصر[\s_]السير"),
        exclude_re=re.compile(
            r"خطبة_الجمعة|(?:^[\s\S]{0,120}خطبة.{0,4}(?:الجمعة|جمعة))"
        ),
        extract_lesson=extract_ordinal_lesson,
        output_file="mukhtasar_seera_index.txt",
    ),

    # 10. الأفنان الندية — عن بعد: NOT FOUND in data (0 hits)

    # 11. الممتع شرح زاد المستقنع — أرشيف رمضان
    "mumti3_archive": SeriesConfig(
        key="mumti3_archive",
        title="الممتع شرح زاد المستقنع — أرشيف رمضان",
        match_re=re.compile(r"الممتع_شرح_زاد_المستقنع"),
        also_match_re=re.compile(r"من_الأرشيف"),
        extract_lesson=extract_ordinal_lesson,
        output_file="mumti3_archive_index.txt",
    ),

    # 12. تنبيه الأنام — أرشيف رمضان
    "tanbeeh_archive": SeriesConfig(
        key="tanbeeh_archive",
        title="تنبيه الأنام على ما في كتاب سبل السلام — أرشيف رمضان",
        match_re=re.compile(r"تنبيه.*الأنام|سبل_السلام"),
        also_match_re=re.compile(r"من_الأرشيف"),
        extract_lesson=extract_ordinal_lesson,
        output_file="tanbeeh_anam_archive_index.txt",
    ),

    # 13. تأسيس الأحكام شرح عمدة الأحكام (main — no archive, no طهارة subset)
    "ta2sees": SeriesConfig(
        key="ta2sees",
        title="تأسيس الأحكام شرح عمدة الأحكام",
        match_re=re.compile(r"تأسيس_الأحكام"),
        exclude_re=re.compile(r"من_الأرشيف|الطهارة"),
        extract_lesson=extract_ordinal_lesson,
        output_file="ta2sees_index.txt",
    ),

    # 14. الملخص في شرح كتاب التوحيد
    "mulakhkhas_tawheed": SeriesConfig(
        key="mulakhkhas_tawheed",
        title="الملخص في شرح كتاب التوحيد",
        match_re=re.compile(r"الملخص[\s_]في[\s_]شرح"),
        also_match_re=re.compile(r"كتاب[\s_]التوحيد|كتاب_التوحيد"),
        extract_lesson=extract_ordinal_lesson,
        output_file="mulakhkhas_tawheed_index.txt",
    ),

    # 15. التفسير الميسر
    "tafseer_muyassar": SeriesConfig(
        key="tafseer_muyassar",
        title="التفسير الميسر",
        match_re=re.compile(r"التفسير[\s_]الميسر"),
        extract_lesson=extract_ordinal_lesson,
        output_file="tafseer_muyassar_index.txt",
    ),

    # 16. التحفة النجمية بشرح الأربعين النووية
    "tuhfa": SeriesConfig(
        key="tuhfa",
        title="التحفة النجمية بشرح الأربعين النووية",
        match_re=re.compile(r"التعليق_على_كتاب_الأربعين_النووية|الأربعين[\s_]النووية"),
        extract_lesson=extract_ordinal_lesson,
        output_file="tuhfa_najmiyya_index.txt",
    ),

    # 17. محاضرات متفرقة
    "muhadarat": SeriesConfig(
        key="muhadarat",
        title="محاضرات متفرقة",
        match_re=re.compile(r"جديد[\s_]المحاضرات|جديد_المحاضرات"),
        extract_title=extract_lecture_title,
        sort_by_date=True,
        output_file="muhadarat_index.txt",
    ),

    # 18. الملخص الفقهي — أرشيف رمضان
    "mulakhkhas_fiqhi_archive": SeriesConfig(
        key="mulakhkhas_fiqhi_archive",
        title="الملخص الفقهي — أرشيف رمضان",
        match_re=re.compile(r"الملخص[\s_]الفقهي"),
        also_match_re=re.compile(r"من_الأرشيف"),
        extract_lesson=extract_ordinal_lesson,
        output_file="mulakhkhas_fiqhi_archive_index.txt",
    ),

    # 19. إتمام المنة بشرح أصول السنة
    "itmam": SeriesConfig(
        key="itmam",
        title="إتمام المنة بشرح أصول السنة",
        match_re=re.compile(r"إتمام[\s_]المنة|إتمام_المنة"),
        extract_lesson=extract_ordinal_lesson,
        output_file="itmam_minna_index.txt",
    ),

    # 20. الشرح الموجز الممهد لتوحيد الخالق الممجد
    "sharh_mujaz": SeriesConfig(
        key="sharh_mujaz",
        title="الشرح الموجز الممهد لتوحيد الخالق الممجد",
        match_re=re.compile(r"التعليق_على_كتاب|التعليق[\s_]على[\s_]كتاب"),
        also_match_re=re.compile(r"الموجز[\s_]الممهد|الموجز_الممهد"),
        extract_lesson=extract_ordinal_lesson,
        output_file="sharh_mujaz_index.txt",
    ),

    # 21. تأسيس الأحكام — الطهارة
    "ta2sees_tahara": SeriesConfig(
        key="ta2sees_tahara",
        title="تأسيس الأحكام شرح عمدة الأحكام — الطهارة",
        match_re=re.compile(r"تأسيس_الأحكام"),
        also_match_re=re.compile(r"الطهارة"),
        extract_lesson=extract_ordinal_lesson,
        output_file="ta2sees_tahara_index.txt",
    ),

    # 22. دروس رمضان — وزارة الشؤون الإسلامية
    "durus_wazara": SeriesConfig(
        key="durus_wazara",
        title="دروس رمضان — وزارة الشؤون الإسلامية",
        match_re=re.compile(r"وزارة[\s_]الشؤون[\s_]الإسلامية|وزارة_الشؤون_الإسلامية"),
        extract_lesson=extract_ordinal_lesson,
        output_file="durus_wazara_index.txt",
    ),

    # 23. إرشاد الساري شرح السنة للبربهاري
    "irshad_sari": SeriesConfig(
        key="irshad_sari",
        title="إرشاد الساري شرح السنة للبربهاري",
        match_re=re.compile(r"شرح_كتاب_شرح_السنة_للبربهاري|للبربهاري"),
        extract_lesson=extract_ordinal_lesson,
        output_file="irshad_sari_index.txt",
    ),

    # 24. المورد العذب الزلال
    "mawrid": SeriesConfig(
        key="mawrid",
        title="المورد العذب الزلال",
        match_re=re.compile(r"المورد[\s_]العذب[\s_]الزلال|المورد_العذب_الزلال"),
        extract_lesson=extract_ordinal_lesson,
        output_file="mawrid_index.txt",
    ),

    # 25. معارج القبول شرح منظومة سلم الوصول — عن بعد
    "ma3arij": SeriesConfig(
        key="ma3arij",
        title="معارج القبول شرح منظومة سلم الوصول",
        match_re=re.compile(r"معارج[\s_]القبول"),
        extract_lesson=extract_ordinal_lesson,
        output_file="ma3arij_index.txt",
    ),

    # 26. الأفنان الندية — أرشيف رمضان
    "afnan_archive": SeriesConfig(
        key="afnan_archive",
        title="الأفنان الندية — أرشيف رمضان",
        match_re=re.compile(r"الأفنان[\s_]الندية|الأفنان_الندية"),
        also_match_re=re.compile(r"من_الأرشيف"),
        extract_lesson=extract_ordinal_lesson,
        output_file="afnan_archive_index.txt",
    ),

    # 27. شرح كتاب الفقه الميسر — أرشيف رمضان
    "fiqh_archive": SeriesConfig(
        key="fiqh_archive",
        title="شرح كتاب الفقه الميسر — أرشيف رمضان",
        match_re=re.compile(r"شرح_كتاب_الفقه_الميسر"),
        also_match_re=re.compile(r"من_الأرشيف"),
        extract_lesson=extract_ordinal_lesson,
        output_file="fiqh_archive_index.txt",
    ),

    # 28. كتاب آداب المشي إلى الصلاة — أرشيف رمضان
    "adab_mashi": SeriesConfig(
        key="adab_mashi",
        title="كتاب آداب المشي إلى الصلاة",
        match_re=re.compile(r"كتاب_آداب_المشي_إلى_الصلاة|آداب[\s_]المشي[\s_]إلى[\s_]الصلاة"),
        extract_lesson=extract_ordinal_lesson,
        output_file="adab_mashi_index.txt",
    ),

    # 29. تيسير العلي القدير لاختصار تفسير ابن كثير
    "tayseer": SeriesConfig(
        key="tayseer",
        title="تيسير العلي القدير لاختصار تفسير ابن كثير",
        match_re=re.compile(r"تيسير[\s_]العلي[\s_]القدير"),
        extract_lesson=extract_ordinal_lesson,
        output_file="tayseer_ali_index.txt",
    ),

    # ── Newly discovered series (not in user's list of 29) ─────────────────

    # التعليق على شرح كتاب التوحيد (~50 posts)
    "ta3leeq_tawheed": SeriesConfig(
        key="ta3leeq_tawheed",
        title="التعليق على شرح كتاب التوحيد",
        match_re=re.compile(r"التعليق_على_شرح_كتاب_التوحيد"),
        extract_lesson=extract_ordinal_lesson,
        output_file="ta3leeq_tawheed_index.txt",
    ),

    # التعليقات الأثرية على العقيدة الواسطية (~19 posts)
    "ta3leeqat_athariyya": SeriesConfig(
        key="ta3leeqat_athariyya",
        title="التعليقات الأثرية على العقيدة الواسطية",
        match_re=re.compile(r"التعليقات_الأثرية_على_العقيدة_الواسطية|التعليقات[\s_]الأثرية"),
        extract_lesson=extract_ordinal_lesson,
        output_file="ta3leeqat_athariyya_index.txt",
    ),

    # التعليقات المختصرة (~12 posts)
    "ta3leeqat_mukhtasara": SeriesConfig(
        key="ta3leeqat_mukhtasara",
        title="التعليقات المختصرة",
        match_re=re.compile(r"التعليقات[\s_]المختصرة|التعليقات_المختصرة"),
        extract_lesson=extract_ordinal_lesson,
        output_file="ta3leeqat_mukhtasara_index.txt",
    ),

    # أعلام السنة المنشورة (~13 posts)
    "a3lam_sunna": SeriesConfig(
        key="a3lam_sunna",
        title="أعلام السنة المنشورة",
        match_re=re.compile(r"أعلام[\s_]السنة[\s_]المنشورة|أعلام_السنة_المنشورة"),
        extract_lesson=extract_ordinal_lesson,
        output_file="a3lam_sunna_index.txt",
    ),

    # الفضل المبين (~8 posts)
    "fadl_mubeen": SeriesConfig(
        key="fadl_mubeen",
        title="الفضل المبين",
        match_re=re.compile(r"الفضل[\s_]المبين|الفضل_المبين"),
        extract_lesson=extract_ordinal_lesson,
        output_file="fadl_mubeen_index.txt",
    ),

    # التعليق على الأصول الثلاثة (~4 posts)
    "usul_thalatha": SeriesConfig(
        key="usul_thalatha",
        title="التعليق على الأصول الثلاثة",
        match_re=re.compile(r"الأصول[\s_]الثلاثة|الأصول_الثلاثة"),
        extract_lesson=extract_ordinal_lesson,
        output_file="usul_thalatha_index.txt",
    ),

    # التعليق على الأصول الستة (~2 posts)
    "usul_sitta": SeriesConfig(
        key="usul_sitta",
        title="التعليق على الأصول الستة",
        match_re=re.compile(r"التعليق[\s_]على[\s_]الأصول[\s_]الستة|التعليق_على_الأصول_الستة"),
        extract_lesson=extract_ordinal_lesson,
        output_file="usul_sitta_index.txt",
    ),

    # الرد الشرعي المعقول على المتصل المجهول (~13 posts)
    "radd_shar3i": SeriesConfig(
        key="radd_shar3i",
        title="الرد الشرعي المعقول على المتصل المجهول",
        match_re=re.compile(r"الرد[\s_]الشرعي[\s_]المعقول|الرد_الشرعي_المعقول_على_المتصل_المجهول"),
        extract_title=extract_lecture_title,
        sort_by_date=True,
        output_file="radd_shar3i_index.txt",
    ),
}

# ---------------------------------------------------------------------------
# Report metadata
# ---------------------------------------------------------------------------

_USER_SERIES_MAP: dict[int, str | None] = {
    1: "tanbeeh",
    2: "ta3leeqat_bahiyya",
    3: None,   # MISSING: تأسيس الأحكام — عن بعد
    4: "ta2sees_archive",
    5: "majaalis",
    6: "mulakhkhas_fiqhi",
    7: "bukhari",
    8: "khutba_seera",
    9: "mukhtasar_seera",
    10: None,  # MISSING: الأفنان الندية — عن بعد
    11: "mumti3_archive",
    12: "tanbeeh_archive",
    13: "ta2sees",
    14: "mulakhkhas_tawheed",
    15: "tafseer_muyassar",
    16: "tuhfa",
    17: "muhadarat",
    18: "mulakhkhas_fiqhi_archive",
    19: "itmam",
    20: "sharh_mujaz",
    21: "ta2sees_tahara",
    22: "durus_wazara",
    23: "irshad_sari",
    24: "mawrid",
    25: "ma3arij",
    26: "afnan_archive",
    27: "fiqh_archive",
    28: "adab_mashi",
    29: "tayseer",
}

_USER_SERIES_NAMES: dict[int, str] = {
    1:  "تنبيه الأنام على ما في كتاب سبل السلام من الفوائد والأحكام",
    2:  "التعليقات البهية على الرسائل العقدية",
    3:  "تأسيس الأحكام شرح عمدة الأحكام - عن بعد",
    4:  "تأسيس الأحكام شرح عمدة الأحكام - أرشيف رمضان",
    5:  "التعليق على كتاب مجالس شهر رمضان",
    6:  "الملخص الفقهي",
    7:  "صحيح البخاري",
    8:  "خطبة الجمعة - مختصر السيرة النبوية",
    9:  "مختصر السيرة النبوية",
    10: "الأفنان الندية - عن بعد",
    11: "الممتع شرح زاد المستقنع - أرشيف رمضان",
    12: "تنبيه الأنام على ما في كتاب سبل السلام من الفوائد والأحكام - أرشيف رمضان",
    13: "تأسيس الأحكام شرح عمدة الأحكام",
    14: "الملخص شرح كتاب التوحيد",
    15: "التفسير الميسر",
    16: "التحفة النجمية بشرح الأربعين النووية",
    17: "محاضرات متفرقة",
    18: "الملخص الفقهي - أرشيف رمضان",
    19: "إتمام المنة بشرح أصول السنة",
    20: "الشرح الموجز الممهد لتوحيد الخالق الممجد",
    21: "تأسيس الأحكام شرح عمدة الأحكام - الطهارة",
    22: "دروس رمضان - وزارة الشؤون الإسلامية",
    23: "إرشاد الساري شرح السنة للبربهاري",
    24: "المورد العذب الزلال",
    25: "معارج القبول شرح منظومة سلم الوصول - عن بعد",
    26: "الأفنان الندية - أرشيف رمضان",
    27: "شرح كتاب الفقه الميسر - أرشيف رمضان",
    28: "كتاب آداب المشي إلى الصلاة - أرشيف رمضان",
    29: "تيسير العلي القدير لاختصار تفسير ابن كثير",
}

_NEW_SERIES_KEYS = [
    "ta3leeq_tawheed",
    "ta3leeqat_athariyya",
    "ta3leeqat_mukhtasara",
    "a3lam_sunna",
    "fadl_mubeen",
    "usul_thalatha",
    "usul_sitta",
    "radd_shar3i",
]

_EXISTING_SERIES_KEYS = ["fiqh", "fatawa", "qawl", "khutba"]

# ---------------------------------------------------------------------------
# Message cache — parse HTML files once, filter per series
# ---------------------------------------------------------------------------

_ALL_MESSAGES: list[dict] | None = None


def _load_all_messages() -> list[dict]:
    global _ALL_MESSAGES
    if _ALL_MESSAGES is not None:
        return _ALL_MESSAGES
    all_msgs: list[dict] = []
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
            msg_id = div.get("id", "").replace("message", "").strip()
            if not msg_id:
                continue
            text = text_el.get_text(separator="\n", strip=True)
            date_el = div.find("div", class_="date")
            date_raw = ""
            if date_el and date_el.get("title"):
                date_raw = date_el["title"].split()[0]
            all_msgs.append({
                "msg_id": msg_id,
                "raw_html": raw_html,
                "text": text,
                "date_raw": date_raw,
            })
    _ALL_MESSAGES = all_msgs
    print(f"[cache] Loaded {len(all_msgs)} messages from HTML files", file=sys.stderr)
    return all_msgs


def parse_series(cfg: SeriesConfig) -> list[dict]:
    records: list[dict] = []
    for msg in _load_all_messages():
        raw_html = msg["raw_html"]
        if not cfg.match_re.search(raw_html):
            continue
        if cfg.also_match_re and not cfg.also_match_re.search(raw_html):
            continue
        if cfg.exclude_re and cfg.exclude_re.search(raw_html):
            continue
        text = msg["text"]
        records.append({
            "msg_id": msg["msg_id"],
            "date_raw": msg["date_raw"],
            "link": f"https://t.me/{CHANNEL}/{msg['msg_id']}",
            "lesson_num": cfg.extract_lesson(text) if cfg.extract_lesson else None,
            "title": cfg.extract_title(text) if cfg.extract_title else "",
            "is_dup": False,
        })
    return records


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


def to_hijri_display(date_raw: str) -> str:
    if not date_raw:
        return "—"
    try:
        dd, mm, yyyy = map(int, date_raw.split("."))
        h = Gregorian(yyyy, mm, dd).to_hijri()
        return f"{to_arabic_numeral(h.day)} {h.month_name('ar')} {to_arabic_numeral(h.year)}هـ"
    except Exception:
        return date_raw


def format_entry(sno: int, rec: dict) -> str:
    num = to_arabic_numeral(sno)
    if rec.get("title"):
        date_display = to_hijri_display(rec["date_raw"])
        return f"* {num}) {rec['title']} | {date_display} 📎 [{rec['link']}]"
    else:
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
# Build index for one series
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


def build_index(cfg: SeriesConfig) -> tuple[str, int]:
    """Return (formatted_text, total_post_count)."""
    records = parse_series(cfg)
    total = len(records)
    print(f"[{cfg.key}] {total} posts found", file=sys.stderr)

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
        f"[{cfg.key}] Old: {len(old_records)} ({old_dup} dups) | "
        f"New (2025+): {len(new_records)} ({new_dup} dups)",
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

    if not sections:
        return (f"[No posts found for: {cfg.title}]", 0)

    full_text = "\n\n".join(sections)
    chunks = chunk_text(full_text)
    total_chunks = len(chunks)
    parts = []
    for i, chunk in enumerate(chunks, 1):
        label = (
            f"\n\n📌 الجزء {to_arabic_numeral(i)} من {to_arabic_numeral(total_chunks)}"
            if total_chunks > 1 else ""
        )
        parts.append(chunk + label)

    separator = "\n\n" + "═" * 30 + "\n\n"
    return (separator.join(parts), total)


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(series_counts: dict[str, int], out_dir: Path) -> None:
    import datetime
    lines = [
        "=" * 65,
        "SERIES INDEX GENERATION REPORT",
        f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 65,
        "",
        "USER-SPECIFIED SERIES (29 requested):",
        "",
    ]

    found_count = 0
    missing_items: list[tuple[int, str]] = []

    for i in range(1, 30):
        key = _USER_SERIES_MAP[i]
        name = _USER_SERIES_NAMES[i]
        if key is None:
            missing_items.append((i, name))
            lines.append(f"  [{i:2d}] MISSING — {name}")
        else:
            count = series_counts.get(key, 0)
            if count == 0:
                missing_items.append((i, name))
                lines.append(f"  [{i:2d}] NOT FOUND (0 posts in data) — {name}")
            else:
                found_count += 1
                lines.append(f"  [{i:2d}] FOUND ({count} posts) — {name}")

    lines += [
        "",
        f"  Result: {found_count} found with posts / {len(missing_items)} not found in data",
        "",
        "-" * 65,
        "",
        "SERIES NOT FOUND IN DATA:",
        "",
    ]
    for i, name in missing_items:
        key = _USER_SERIES_MAP.get(i)
        if key is None:
            reason = "no matching hashtag or text pattern found in export"
        else:
            reason = "0 posts matched — this specific variant is absent from the export"
        lines.append(f"  [{i:2d}] {name}")
        lines.append(f"        ({reason})")

    lines += [
        "",
        "-" * 65,
        "",
        "NEWLY DISCOVERED SERIES (not in user's list of 29):",
        "",
    ]
    for key in _NEW_SERIES_KEYS:
        cfg = SERIES[key]
        count = series_counts.get(key, 0)
        lines.append(f"  {cfg.title}: {count} posts  ->  output/{cfg.output_file}")

    lines += [
        "",
        "-" * 65,
        "",
        "EXISTING SERIES (generated in previous sessions, regenerated):",
        "",
    ]
    for key in _EXISTING_SERIES_KEYS:
        cfg = SERIES[key]
        count = series_counts.get(key, 0)
        lines.append(f"  {cfg.title}: {count} posts  ->  output/{cfg.output_file}")

    lines += ["", "=" * 65, ""]

    report_text = "\n".join(lines)
    report_path = out_dir / "series_report.txt"
    report_path.write_text(report_text, encoding="utf-8")
    print("[report] Saved -> output/series_report.txt", file=sys.stderr)
    print(report_text)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Build Telegram series indices")
    parser.add_argument(
        "--series",
        choices=list(SERIES.keys()),
        help="Build index for a single series and print to stdout",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Print report of series counts without writing index files",
    )
    args = parser.parse_args()

    if args.series:
        cfg = SERIES[args.series]
        text, count = build_index(cfg)
        print(text)
        return

    out_dir = ROOT / "output"
    out_dir.mkdir(exist_ok=True)

    series_counts: dict[str, int] = {}

    if args.report:
        # Count only, no file writes
        for cfg in SERIES.values():
            records = parse_series(cfg)
            series_counts[cfg.key] = len(records)
            print(f"[{cfg.key}] {len(records)} posts", file=sys.stderr)
    else:
        for cfg in SERIES.values():
            text, count = build_index(cfg)
            series_counts[cfg.key] = count
            if count > 0:
                out_path = out_dir / cfg.output_file
                out_path.write_text(text, encoding="utf-8")
                print(f"[{cfg.key}] Saved -> output/{cfg.output_file}", file=sys.stderr)
            else:
                print(f"[{cfg.key}] Skipped (0 posts) — no file written", file=sys.stderr)

    generate_report(series_counts, out_dir)


if __name__ == "__main__":
    main()
