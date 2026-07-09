#!/usr/bin/env python3
"""
extract_standalone_lectures.py — Extract individual standalone lectures/speeches/talks.

Searches all 6 HTML export files for audio messages matching keywords that indicate
standalone lectures (not part of a series, not Juma Khutba):
  - كلمة دعوية بعنوان
  - جديد_الكلمات
  - كلمة بعنوان
  - محاضرة بعنوان
  - محاضرة

Output: output/corrected/standalone_lectures.csv
"""

import csv
import re
from pathlib import Path
from bs4 import BeautifulSoup

try:
    from hijri_converter import convert as hijri_convert
    HAS_HIJRI = True
except ImportError:
    HAS_HIJRI = False

BASE_DIR = Path(__file__).resolve().parent.parent

HTML_FILES = [f for f in [
    BASE_DIR / "messages.html",
    BASE_DIR / "messages2.html",
    BASE_DIR / "messages3.html",
    BASE_DIR / "messages4.html",
    BASE_DIR / "messages_june1.html",
    BASE_DIR / "messages_new.html",
] if f.exists()]

OUT_PATH = BASE_DIR / "output" / "corrected" / "standalone_lectures.csv"

# Arabic digit map
AR = {'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}
def ar2en(s): return ''.join(AR.get(c, c) for c in s)

# Keywords to match (ordered longest-first to capture the most specific form)
KEYWORDS = [
    'كلمة دعوية بعنوان',
    'جديد_الكلمات',
    'كلمة بعنوان',
    'محاضرة بعنوان',
    'محاضرة',
]

HIJRI_MONTHS = {
    'محرم':1,'صفر':2,'ربيع الأول':3,'ربيع الثاني':4,'ربيع الآخر':4,
    'جمادى الأولى':5,'جمادى الأول':5,'جمادى الثانية':6,'جمادى الآخرة':6,'جمادى الآخر':6,
    'رجب':7,'شعبان':8,'رمضان':9,'شوال':10,
    'ذو القعدة':11,'ذي القعدة':11,'ذو الحجة':12,'ذي الحجة':12,
}


def extract_hijri(text):
    # [١٤٤٣/٠٣/٠٧ هـ] or similar bracketed Hijri
    m = re.search(r'[❲\[]\s*([٠-٩]{4})[/٫]\s*([٠-٩]{1,2})[/٫]\s*([٠-٩]{1,2})\s*هـ?\s*[❳\]]', text)
    if m:
        y, mo, d = ar2en(m.group(1)), ar2en(m.group(2)), ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    # bare YYYY/MM/DD in Arabic digits
    m = re.search(r'([٠-٩]{4})[/٫]\s*([٠-٩]{1,2})[/٫]\s*([٠-٩]{1,2})', text)
    if m:
        y, mo, d = ar2en(m.group(1)), ar2en(m.group(2)), ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    # DD/MM/YYYY ه
    m = re.search(r'([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})\s*[هﻫ]', text)
    if m:
        return f"{m.group(3)}/{int(m.group(2)):02d}/{int(m.group(1)):02d}"
    # DD MonthName YYYY
    for mn, mnum in HIJRI_MONTHS.items():
        m = re.search(r'([0-9]{1,2})\s+' + re.escape(mn) + r'\s+([0-9]{4})', text)
        if m:
            return f"{m.group(2)}/{mnum:02d}/{int(m.group(1)):02d}"
        m = re.search(r'([٠-٩]{1,2})\s+' + re.escape(mn) + r'\s+([٠-٩]{4})', text)
        if m:
            d2, y2 = ar2en(m.group(1)), ar2en(m.group(2))
            return f"{y2}/{mnum:02d}/{int(d2):02d}"
    return ''


def greg_to_hijri(gd_raw: str) -> str:
    """Convert DD.MM.YYYY to Hijri YYYY/MM/DD string."""
    if not HAS_HIJRI:
        return ''
    try:
        d, mo, y = gd_raw.split('.')
        h = hijri_convert.Gregorian(int(y), int(mo), int(d)).to_hijri()
        return f"{h.year}/{h.month:02d}/{h.day:02d}"
    except Exception:
        return ''


def extract_duration(text):
    # Normalise Arabic digits first for uniform matching
    norm = ar2en(text)
    m = re.search(r'◉\s*(\d+:\d{2}(?::\d{2})?)', norm)
    if m: return m.group(1)
    m = re.search(r'(?:مدة|المدة)\s*(?:الصوتية)?\s*[:#]\s*(\d+:\d{2})', norm)
    if m: return m.group(1)
    m = re.search(r'(\d{1,2}:\d{2})\s*(?:د\b|دقيقة|دق\b)', norm)
    if m: return m.group(1)
    # Time format anywhere (e.g. 21:52 or 04:45)
    m = re.search(r'\b(\d{1,2}:\d{2})\b', norm)
    if m: return m.group(1)
    return ''


_EMOJI_RE = re.compile(
    r'[\U00002300-\U000027BF'   # Misc Technical + Dingbats (includes ⏪ U+23EA)
    r'\U00002702-\U000027B0'
    r'\U00002B00-\U00002BFF'    # Misc Symbols and Arrows
    r'\U0001F300-\U0001FAFF'    # All modern emoji blocks
    r'‍️♦♣♠♥❤✅✔❌⭕🔸🔹🔺🔻▪▫◆◇●○□■▾▸►◄◀▶]+',
    re.UNICODE
)
_HASHTAG_RE = re.compile(r'#(\S+)')
_SPEAKER_RE = re.compile(
    r'(?:🎙|ألقاها?|إلقاء|لفضيلة|\bفضيلة\b|🎧|شيخنا|للشيخ|الشيخ الفاضل|الشيخ:|الشيخ\b|حسن بن محمد|للاستماع|\bرابط\b|الرابط|🔗|http|www|\bحفظه\b).*',
    re.DOTALL
)
_BRACKET_RE = re.compile(r'[❲❳\[\]«»""]')


def clean_title(raw: str) -> str:
    """Strip emoji, speaker attribution, hashtags → clean readable title."""
    # Remove URLs
    raw = re.sub(r'https?://\S+', '', raw)
    # Convert hashtags to readable text
    raw = _HASHTAG_RE.sub(lambda m: m.group(1).replace('_', ' '), raw)
    # Remove emoji
    raw = _EMOJI_RE.sub(' ', raw)
    # Cut off at speaker attribution
    raw = _SPEAKER_RE.sub('', raw)
    # Remove brackets used as decoration
    raw = _BRACKET_RE.sub('', raw)
    # Remove leftover punctuation noise
    raw = re.sub(r'[|◆◇▸▶►•✿✨]+', ' ', raw)
    # Collapse spaces and strip first — so ^ anchors work below
    raw = re.sub(r'\s+', ' ', raw).strip()
    # Strip leading keyword-phrase noise from titles
    raw = re.sub(r'^(?:قيّمة\s+)?بعنوان\s*[:\-–—]?\s*', '', raw)
    raw = re.sub(r'^(?:كلمة(?:\s+(?:دعوية|قصيرة|مختصرة))?(?:\s+بعنوان)?|محاضرة(?:\s+بعنوان)?)\s*[:\-–—]\s*', '', raw)
    raw = re.sub(r'^_\S+\s+', '', raw)   # leading _word artifact from hashtag parsing
    # Strip trailing lone single letter (e.g. stray "ل")
    raw = re.sub(r'\s+[؀-ۿ]$', '', raw)
    # Final trim
    raw = raw.strip('،,.:-–—').strip()
    return raw


def extract_title(text: str, keyword: str) -> str:
    """
    Extract the lecture title that follows the matched keyword.
    Handles inline titles, bracketed titles, and عنوان الكلمة patterns.
    """
    flat = re.sub(r'\s+', ' ', text).strip()

    # Pattern 1: [bracketed] or «quoted» or ◆|delimited| titles — skip keyword-label brackets
    bracket_pat = re.compile(r'(?:[❲\[«]\s*([^❳\[\]«»]{5,120}?)\s*[❳\]»]|◆\s*\|\s*([^|◆]{5,120}?)\s*\|)')
    for m in bracket_pat.finditer(flat):
        raw_cand = m.group(1) or m.group(2)
        candidate = clean_title(raw_cand)
        if len(candidate) > 5 and candidate not in KEYWORDS:
            return candidate

    # Pattern 2: عنوان الكلمة / standalone بعنوان: — search line-by-line for clean extraction
    lines_raw = [l.strip() for l in text.splitlines() if l.strip()]
    for i, line in enumerate(lines_raw):
        # Case A: title is on the SAME line as بعنوان: (e.g. "📝 بعنوان: حال المرأة...")
        m = re.search(r'(?:عنوان\s+(?:الكلمة|المحاضرة)|(?:📝\s*)?بعنوان)\s*:\s*(.{5,})', line)
        if m:
            candidate = clean_title(m.group(1))
            if len(candidate) > 5 and candidate not in KEYWORDS:
                return candidate
        # Case B: بعنوان: is the whole line; title is the NEXT non-empty line
        if re.fullmatch(r'[^\w]*(?:📝\s*)?بعنوان\s*:?\s*', line) and i + 1 < len(lines_raw):
            candidate = clean_title(lines_raw[i + 1])
            if len(candidate) > 5 and candidate not in KEYWORDS:
                return candidate

    # Prepare line list for pattern 4+
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    kw_line_idx = next((i for i, l in enumerate(lines) if keyword in l), None)

    # Pattern 3: look for title on lines BEFORE the keyword line (title precedes keyword)
    if kw_line_idx is not None and kw_line_idx > 0:
        for line in reversed(lines[:kw_line_idx]):
            candidate = clean_title(line)
            if (len(candidate) > 8 and candidate not in KEYWORDS
                    and not candidate.startswith('http') and not re.match(r'^#', line)):
                return candidate

    # Pattern 4: text after keyword — up to attribution markers
    idx = flat.find(keyword)
    if idx >= 0:
        after = flat[idx + len(keyword):].lstrip(' :–—-')
        for stop in ['🎙', 'ألقاها', 'إلقاء', 'لفضيلة', 'الشيخ الفاضل', 'حسن بن محمد', '(🔊)', '==', '\n']:
            pos = after.find(stop)
            if pos > 0:
                after = after[:pos]
        candidate = clean_title(after[:200])
        if len(candidate) > 5 and candidate not in KEYWORDS:
            return candidate

    # Pattern 5: first meaningful non-keyword line (general fallback)
    for line in lines[:5]:
        candidate = clean_title(line)
        if (len(candidate) > 8 and candidate not in KEYWORDS
                and not candidate.startswith('http') and not re.match(r'^#', line)):
            return candidate

    return ''


def matched_keyword(text: str) -> str:
    """Return the most specific matching keyword found in text, or ''."""
    for kw in KEYWORDS:
        if kw in text:
            return kw
    return ''


def main():
    rows = []
    seen_mids = set()

    for html_file in HTML_FILES:
        print(f"  Scanning {html_file.name} …")
        soup = BeautifulSoup(html_file.read_text(encoding='utf-8'), 'lxml')

        for msg in soup.find_all('div', id=re.compile(r'^message\d+$')):
            mid_str = re.sub(r'\D', '', msg.get('id', ''))
            if not mid_str:
                continue
            mid = int(mid_str)
            if mid in seen_mids:
                continue

            audio_el = msg.select_one('a.media_audio_file')
            if not audio_el:
                continue  # only interested in audio messages

            text_div = msg.find('div', class_='text')
            text = text_div.get_text(separator='\n') if text_div else ''

            kw = matched_keyword(text)
            if not kw:
                continue

            seen_mids.add(mid)

            date_el = msg.find('div', class_='date')
            gd_raw = ''
            greg_date = ''
            if date_el:
                title_attr = date_el.get('title', '')
                m = re.match(r'(\d{2}\.\d{2}\.\d{4})', title_attr)
                if m:
                    gd_raw = m.group(1)
                    d, mo, y = gd_raw.split('.')
                    greg_date = f"{d}/{mo}/{y}"

            hijri = extract_hijri(text)
            if not hijri and gd_raw:
                hijri = greg_to_hijri(gd_raw)

            audio_file = audio_el.get('href', '').split('/')[-1]
            duration = extract_duration(text)
            title = extract_title(text, kw)
            # Don't store keyword itself as title
            if title in KEYWORDS:
                title = ''

            rows.append({
                'mid': mid,
                'gd_raw': gd_raw,
                'greg_date': greg_date,
                'hijri': hijri,
                'audio': audio_file,
                'duration': duration,
                'title': title,
                'keyword': kw,
            })

    # Sort by message ID (chronological)
    rows.sort(key=lambda r: r['mid'])

    print(f"\nFound {len(rows)} standalone lectures\n")
    print(f"{'DB':>4}  {'MsgID':>7}  {'GregDate':12}  {'Dur':8}  {'Keyword':22}  Title")
    print("-" * 100)
    for i, r in enumerate(rows, 1):
        kw_short = r['keyword'][:22]
        title_disp = r['title'][:50]
        print(f"{i:4}  {r['mid']:7}  {r['greg_date']:12}  {r['duration']:8}  {kw_short:22}  {title_disp}")

    # Write CSV
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['DB#', 'MsgID', 'GregDate', 'HijriDate', 'AudioFilename', 'Duration', 'Title', 'Keyword'])
        for i, r in enumerate(rows, 1):
            writer.writerow([i, r['mid'], r['greg_date'], r['hijri'],
                             r['audio'], r['duration'], r['title'], r['keyword']])

    print(f"\nWritten {len(rows)} rows → {OUT_PATH}")


if __name__ == '__main__':
    main()
