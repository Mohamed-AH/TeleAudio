#!/usr/bin/env python3
"""
Extract upload data for three Islamic lecture series:
1. الأفنان الندية - عن بعد
2. معارج القبول شرح منظومة سلم الوصول - عن بعد
3. تأسيس الأحكام شرح عمدة الأحكام - عن بعد
"""

import re
import os
from bs4 import BeautifulSoup

# ── helpers ──────────────────────────────────────────────────────────────────

AR = {'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}
def ar2en(s): return ''.join(AR.get(c,c) for c in s)

MONTHS = {
    'محرم':1,'صفر':2,'ربيع الأول':3,'ربيع الثاني':4,'ربيع الآخر':4,
    'جمادى الأولى':5,'جمادى الأول':5,'جمادى الأوّل':5,
    'جمادى الآخرة':6,'جمادى الثانية':6,'جمادى الآخر':6,
    'رجب':7,'شعبان':8,'رمضان':9,'شوال':10,
    'ذو القعدة':11,'ذي القعدة':11,'ذو الحجة':12,'ذي الحجة':12,
}

def extract_hijri(text):
    # 1. Arabic YYYY/MM/DD: ١٤٤٣/٠٣/١٥
    m = re.search(r'([٠-٩]{4})[/٫]\s*([٠-٩]{1,2})[/٫]\s*([٠-٩]{1,2})', text)
    if m:
        y,mo,d = ar2en(m.group(1)), ar2en(m.group(2)), ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    # 2. ASCII DD/MM/YYYY with هـ
    m = re.search(r'([0-9]{1,2})\s*/\s*([0-9]{1,2})\s*/\s*([0-9]{4})\s*[هﻫ]', text)
    if m: return f"{m.group(3)}/{int(m.group(2)):02d}/{int(m.group(1)):02d}"
    # 3. Arabic DD/MM/YYYY (or MM/DD/YYYY — validate month range, swap if invalid)
    # These Afnan messages use pattern MM/DD / YYYY (first group is month)
    # When both positions are valid months (<=12), we need context to decide;
    # we store both possibilities as a tuple hint — caller resolves with greg date
    m = re.search(r'([٠-٩]{1,2})\s*/\s*([٠-٩]{1,2})\s*/\s*([٠-٩]{4})', text)
    if m:
        a, b, y = ar2en(m.group(1)), ar2en(m.group(2)), ar2en(m.group(3))
        if int(b) > 12 and int(a) <= 12:
            # b can't be month, swap: MM=a, DD=b
            return f"{y}/{int(a):02d}/{int(b):02d}"
        elif int(a) > 12 and int(b) <= 12:
            # a can't be month, a is day: DD/MM/YYYY
            return f"{y}/{int(b):02d}/{int(a):02d}"
        else:
            # Both ambiguous — return special marker for caller to resolve
            return f"AMBIGUOUS:{y}:{a}:{b}"
    # 4. Arabic DD-MM-YYYY
    m = re.search(r'([٠-٩]{1,2})\s*-\s*([٠-٩]{1,2})\s*-\s*([٠-٩]{4})', text)
    if m:
        d,mo,y = ar2en(m.group(1)), ar2en(m.group(2)), ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    # 5. ASCII DD-MM-YYYY
    m = re.search(r'([0-9]{1,2})\s*-\s*([0-9]{1,2})\s*-\s*([0-9]{4})', text)
    if m: return f"{m.group(3)}/{int(m.group(2)):02d}/{int(m.group(1)):02d}"
    # 6. ASCII DD/MM/YYYY without هـ (for patterns like ١٠/١٧ / ١٤٤٧ in Arabic script)
    # 7. MonthName
    for mn, mnum in MONTHS.items():
        m = re.search(r'([0-9]{1,2})\s+' + re.escape(mn) + r'\s+([0-9]{4})', text)
        if m: return f"{m.group(2)}/{mnum:02d}/{int(m.group(1)):02d}"
        m = re.search(r'([٠-٩]{1,2})\s+' + re.escape(mn) + r'\s+([٠-٩]{4})', text)
        if m:
            d2, y2 = ar2en(m.group(1)), ar2en(m.group(2))
            return f"{y2}/{mnum:02d}/{int(d2):02d}"
    return None

def hijri_closest_to_greg(y, a, b, greg_dd_mm_yyyy):
    """Return YYYY/MM/DD for the option (a as MM, b as DD) or (b as MM, a as DD)
    that is closest in time to the Gregorian posting date."""
    try:
        from hijri_converter import convert
        import warnings; warnings.filterwarnings('ignore')
        gd, gm, gy = greg_dd_mm_yyyy.split('.')
        g_date = convert.Gregorian(int(gy), int(gm), int(gd)).to_hijri()
        g_num = g_date.year * 10000 + g_date.month * 100 + g_date.day

        opt1_str = f"{y}/{int(a):02d}/{int(b):02d}"  # a=MM, b=DD
        opt2_str = f"{y}/{int(b):02d}/{int(a):02d}"  # b=MM, a=DD
        o1 = int(y) * 10000 + int(a) * 100 + int(b)
        o2 = int(y) * 10000 + int(b) * 100 + int(a)
        if abs(o1 - g_num) <= abs(o2 - g_num):
            return opt1_str
        else:
            return opt2_str
    except Exception:
        return f"{y}/{int(b):02d}/{int(a):02d}"  # fallback

def extract_hijri_mixed(text, greg_date=None):
    """Handle mixed Arabic/ASCII patterns like '١٠/١١ / ١٤٤٧' or '١٤٤٧/١١/٣٠'
    greg_date: DD.MM.YYYY string for tie-breaking ambiguous cases."""
    h = extract_hijri(text)
    if h:
        if h.startswith('AMBIGUOUS:'):
            # Resolve ambiguity using Gregorian date if available
            _, y, a, b = h.split(':')
            if greg_date:
                return hijri_closest_to_greg(y, a, b, greg_date)
            else:
                return f"{y}/{int(b):02d}/{int(a):02d}"  # fallback: DD/MM
        return h
    # Pattern: ARABIC_DD / ARABIC_MM / ASCII_YYYY (or MM/DD/YYYY — validate)
    m = re.search(r'([٠-٩]{1,2})\s*/\s*([٠-٩]{1,2})\s*/\s*([0-9]{4})', text)
    if m:
        a, b, y = ar2en(m.group(1)), ar2en(m.group(2)), m.group(3)
        if int(b) > 12 and int(a) <= 12:
            return f"{y}/{int(a):02d}/{int(b):02d}"  # MM/DD/YYYY
        elif int(a) > 12 and int(b) <= 12:
            return f"{y}/{int(b):02d}/{int(a):02d}"  # DD/MM/YYYY
        elif greg_date:
            return hijri_closest_to_greg(y, a, b, greg_date)
        else:
            return f"{y}/{int(b):02d}/{int(a):02d}"  # default DD/MM/YYYY
    # Pattern: ASCII_DD / ARABIC_MM / ASCII_YYYY
    m = re.search(r'([0-9]{1,2})\s*/\s*([٠-٩]{1,2})\s*/\s*([0-9]{4})', text)
    if m:
        d, mo, y = m.group(1), ar2en(m.group(2)), m.group(3)
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    # Pattern: ASCII_YYYY / Arabic_MM / ASCII_DD
    m = re.search(r'([0-9]{4})\s*/\s*([٠-٩]{1,2})\s*/\s*([٠-٩]{1,2})', text)
    if m:
        y, mo, d = m.group(1), ar2en(m.group(2)), ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    return None

def extract_duration(text):
    m = re.search(r'◉\s*(\d+:\d{2}(?::\d{2})?)', text)
    if m: return m.group(1)
    m = re.search(r'\[(\d+:\d{2}(?::\d{2})?)\]', text)
    if m: return m.group(1)
    m = re.search(r'(?:مدة|المدة)\s*(?:الصوتية)?\s*[:#]\s*(\d+:\d{2}(?::\d{2})?)', text)
    if m: return m.group(1)
    m = re.search(r'مدة المقطع\s*#?\s*(\d+:\d{2})', text)
    if m: return m.group(1)
    m = re.search(r'(\d{1,3}:\d{2})\s*(?:دقيقة|د)', text)
    if m: return m.group(1)
    return None

def greg_to_hijri(gd_str):
    """Convert DD.MM.YYYY to Hijri YYYY/MM/DD"""
    try:
        from hijri_converter import convert
        d, mo, y = gd_str.split('.')
        h = convert.Gregorian(int(y), int(mo), int(d)).to_hijri()
        return f"{h.year}/{h.month:02d}/{h.day:02d}"
    except Exception as e:
        return f"(conversion error: {e})"

ORDINALS = {
    'الأول':1,'الاول':1,'الأولى':1,'الثاني':2,'الثالث':3,'الرابع':4,'الخامس':5,
    'السادس':6,'السابع':7,'الثامن':8,'التاسع':9,'العاشر':10,
    'الحادي عشر':11,'الحادى عشر':11,'الثاني عشر':12,'الثالث عشر':13,'الرابع عشر':14,
    'الخامس عشر':15,'السادس عشر':16,'السابع عشر':17,'الثامن عشر':18,'التاسع عشر':19,
    'العشرون':20,'الحادي والعشرون':21,'الحادى والعشرون':21,'الواحد والعشرون':21,
    'الثاني والعشرون':22,'الثالث والعشرون':23,'الرابع والعشرون':24,
    'الخامس والعشرون':25,'السادس والعشرون':26,'السابع والعشرون':27,'الثامن والعشرون':28,
    'التاسع والعشرون':29,'الثلاثون':30,'الحادي والثلاثون':31,'الواحد والثلاثون':31,
    'الثاني والثلاثون':32,'الثالث والثلاثون':33,'الرابع والثلاثون':34,'الخامس والثلاثون':35,
    'السادس والثلاثون':36,'السابع والثلاثون':37,'الثامن والثلاثون':38,
    'التاسع والثلاثون':39,'الأربعون':40,'الحادي والأربعون':41,'الثاني والأربعون':42,
    'الثالث والأربعون':43,'الرابع والأربعون':44,'الخامس والأربعون':45,
    'السادس والأربعون':46,'السابع والأربعون':47,'الثامن والأربعون':48,
    'التاسع والأربعون':49,'الخمسون':50,
}
# Sort by length descending for longest-match-first
ORDINALS_SORTED = sorted(ORDINALS.keys(), key=len, reverse=True)

def ordinal_to_int(text):
    """Find and convert Arabic ordinal to integer. Returns (int, matched_text) or (None, None)."""
    for key in ORDINALS_SORTED:
        if key in text:
            return ORDINALS[key], key
    return None, None

def extract_lesson_num(text):
    """Extract lesson number from الدرس X pattern."""
    # Pattern: الدرس X في/- (where X ends before في or -)
    # First try: الدرس X عن بُعد (direct)
    m = re.search(r'الدرس\s+(.+?)\s+عن\s+بُعد', text)
    if m:
        raw = m.group(1).strip()
        # Remove trailing content after في or -
        raw = re.split(r'\s+في\s+|\s+-\s+', raw)[0].strip()
        n, _ = ordinal_to_int(raw)
        if n:
            return n, raw
    # Second try: الدرس X في كتاب Y...عن بُعد (chapter title included)
    m = re.search(r'الدرس\s+(.+?)\s*(?:عن\s+بُعد|-\s*عن\s+بُعد)', text)
    if m:
        raw = m.group(1).strip()
        # Extract just the ordinal
        n, matched = ordinal_to_int(raw)
        if n:
            return n, matched
    return None, None

def get_audio_filename(msg):
    """Get audio filename from message."""
    audio = msg.find('a', class_='media_audio_file')
    if not audio:
        return None
    href = audio.get('href', '')
    return os.path.basename(href)

def get_greg_date(msg):
    """Get DD.MM.YYYY date from message."""
    date_div = msg.find('div', class_='date')
    if not date_div:
        return None
    title = date_div.get('title', '')
    m = re.match(r'(\d{2}\.\d{2}\.\d{4})', title)
    if m:
        return m.group(1)
    return None

def greg_to_slash(gd):
    """Convert DD.MM.YYYY to DD/MM/YYYY"""
    if gd:
        return gd.replace('.', '/')
    return None

# ── HTML loading ─────────────────────────────────────────────────────────────

HTML_FILES = [
    '/home/user/TeleAudio/messages.html',
    '/home/user/TeleAudio/messages2.html',
    '/home/user/TeleAudio/messages3.html',
    '/home/user/TeleAudio/messages4.html',
    '/home/user/TeleAudio/messages_new.html',
    '/home/user/TeleAudio/messages_june1.html',
]

def load_all_messages():
    """Load all messages from HTML files, deduplicating by ID."""
    all_msgs = {}  # mid -> (soup_element, source_file)
    for fname in HTML_FILES:
        if not os.path.exists(fname):
            print(f"WARNING: {fname} not found, skipping")
            continue
        with open(fname, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        msgs = soup.find_all('div', class_='message')
        for m in msgs:
            mid_raw = m.get('id', '')
            mid = mid_raw.replace('message', '').strip()
            if mid and mid not in all_msgs:
                all_msgs[mid] = (m, fname.split('/')[-1])
    print(f"Loaded {len(all_msgs)} unique messages from {len(HTML_FILES)} HTML files")
    return all_msgs

# ── Series 1: الأفنان الندية - عن بعد ────────────────────────────────────────

def extract_afnan_chapter(text):
    """Extract chapter/sub-topic from 'الأفنان الندية - CHAPTER -' pattern."""
    # Pattern: الأفنان الندية - CHAPTER - (may have variants)
    m = re.search(r'الأفنان\s+الندية\s*[-–]\s*(.+?)\s*[-–]', text)
    if m:
        return m.group(1).strip()
    # Fallback: الأفنان الندية - CHAPTER (no trailing dash)
    m = re.search(r'الأفنان\s+الندية\s*[-–]\s*(.+?)(?:\s*🔸|\s*للعلامة|\s*🔹)', text)
    if m:
        return m.group(1).strip()
    # Pattern: [ القراءة والتعليق على كتاب #الأفنان_الندية ... ]
    # followed by تأليف...  🔹 chapter
    # Look for كتاب X باب Y pattern
    m = re.search(r'🔹\s+(?:الدرس[^-]+?[-–]\s*)?(.+?)\s*(?:عن\s+بُعد|🎙|🔸)', text)
    if m:
        chapter = m.group(1).strip()
        # Remove lesson number prefix if present
        chapter = re.sub(r'^الدرس\s+\S+\s*[-–]?\s*', '', chapter).strip()
        if chapter and 'عن بُعد' not in chapter and 'الدرس' not in chapter:
            return chapter
    return None

def extract_afnan_chapter_from_msg(clean):
    """Extract chapter/sub-topic from Afnan message text."""
    # First: "الأفنان الندية - CHAPTER -" or "الأفنان الندية - CHAPTER 🔸"
    m = re.search(r'الأفنان\s+الندية\s*[-–]\s*(.+?)\s*(?:[-–🔸]|للعلامة)', clean)
    if m:
        chapter = m.group(1).strip().rstrip('🔸 -').strip()
        if chapter:
            return chapter
    # Second: "الأفنان الندية - كتاب X 🔸" (no trailing dash)
    m = re.search(r'الأفنان\s+الندية\s*[-–]\s*(.+?)(?=\s*🔸|\s*للعلامة)', clean)
    if m:
        chapter = m.group(1).strip()
        if chapter:
            return chapter
    return None

def extract_afnan_lesson_line(clean):
    """Extract the 🔹 lesson descriptor line."""
    m = re.search(r'🔹\s+(.+?)(?:\s*(?:مع فضيلة|🎙|مدة))', clean)
    if m:
        return m.group(1).strip()
    return None

def extract_afnan_series(all_msgs):
    """Extract Series 1: الأفنان الندية - عن بعد"""
    records = []
    for mid, (msg, src) in all_msgs.items():
        audio = msg.find('a', class_='media_audio_file')
        if not audio:
            continue
        text = msg.get_text()
        if 'الأفنان' not in text:
            continue
        if 'عن بُعد' not in text and 'عن بعد' not in text:
            continue

        # Get audio filename
        audio_file = get_audio_filename(msg)

        # Get dates
        gd = get_greg_date(msg)  # DD.MM.YYYY
        greg_slash = greg_to_slash(gd)  # DD/MM/YYYY

        # Get text clean for parsing
        clean = ' '.join(text.split())

        # Extract hijri date from text first
        hijri = extract_hijri_mixed(clean, gd)
        hijri_source = 'text'
        if not hijri and gd:
            hijri = greg_to_hijri(gd)
            hijri_source = 'computed'

        # Extract duration
        duration = extract_duration(clean)

        # Extract lesson number from the 🔹 line
        lesson_line = extract_afnan_lesson_line(clean)
        lesson_num = None
        raw_ordinal = ''
        if lesson_line:
            lesson_num, raw_ordinal = ordinal_to_int(lesson_line)
            raw_ordinal = raw_ordinal or ''

        if lesson_num is None:
            # Try broader match
            lesson_num, raw_ordinal = extract_lesson_num(clean)
            raw_ordinal = raw_ordinal or ''

        # Extract chapter
        chapter = extract_afnan_chapter_from_msg(clean)

        # Also try from lesson line: "الدرس X في كتاب Y" or "الدرس X - كتاب Y"
        if not chapter and lesson_line:
            m_fi = re.search(r'في\s+(كتاب\s+\S+(?:\s+\S+){0,5})', lesson_line)
            if m_fi:
                chapter = m_fi.group(1).strip()
            else:
                m_dash = re.search(r'[-–]\s*(كتاب\s+.+?)(?:\s*[-–]|\s*$)', lesson_line)
                if m_dash:
                    chapter = m_dash.group(1).strip()

        records.append({
            'mid': mid,
            'src': src,
            'lesson_num': lesson_num,
            'raw_ordinal': raw_ordinal,
            'lesson_line': lesson_line or '',
            'chapter': chapter or '',
            'audio': audio_file or '',
            'duration': duration or '',
            'greg_date': greg_slash or '',
            'hijri': hijri or '',
            'hijri_source': hijri_source,
        })

    # Sort by date order (mid as proxy) — DB numbers are sequential by date
    records.sort(key=lambda r: int(r['mid'] or 0))
    # Assign sequential DB numbers
    for i, r in enumerate(records, 1):
        r['db_num'] = i
    return records

# ── Series 2: معارج القبول ────────────────────────────────────────────────────

# Index mapping from mid=6971
MAAREJ_INDEX = {
    '5550': 1, '5998': 2, '5999': 3, '6966': 4, '6000': 5, '5739': 6,
    '5761': 7, '5814': 8, '5829': 9, '6001': 10, '6968': 11, '6969': 12,
    '5970': 13, '6002': 14, '6058': 15, '6126': 16, '6243': 17, '6292': 18,
    '6366': 19, '6586': 20, '6688': 21, '6810': 22, '6881': 23, '6932': 24,
    '6935': 25, '7136': 26, '7434': 27, '7435': 28,
}

def extract_maarej_series(all_msgs):
    """Extract Series 2: معارج القبول شرح منظومة سلم الوصول"""
    records = []
    seen_mids = set()

    for mid, (msg, src) in all_msgs.items():
        audio = msg.find('a', class_='media_audio_file')
        text = msg.get_text()

        if 'معارج' not in text and 'سلم الوصول' not in text:
            continue

        # L15 mid=6058 has no audio - include it anyway
        if mid not in MAAREJ_INDEX:
            continue  # Skip messages not in the index

        if mid in seen_mids:
            continue
        seen_mids.add(mid)

        lesson_num = MAAREJ_INDEX[mid]

        # Get audio filename
        audio_file = get_audio_filename(msg) if audio else '(لا توجد تسجيل)'

        # Get dates
        gd = get_greg_date(msg)
        greg_slash = greg_to_slash(gd)

        clean = ' '.join(text.split())

        # Extract hijri date from text first
        hijri = extract_hijri_mixed(clean, gd)
        hijri_source = 'text'
        if not hijri and gd:
            hijri = greg_to_hijri(gd)
            hijri_source = 'computed'

        # Extract duration
        duration = extract_duration(clean) if audio else ''

        records.append({
            'mid': mid,
            'src': src,
            'lesson_num': lesson_num,
            'audio': audio_file,
            'duration': duration or '',
            'greg_date': greg_slash or '',
            'hijri': hijri or '',
            'hijri_source': hijri_source,
            'has_audio': audio is not None,
        })

    records.sort(key=lambda r: r['lesson_num'])
    return records

# ── Series 3: تأسيس الأحكام - عن بعد ────────────────────────────────────────

def extract_tasees_chapter(text):
    """Extract كتاب/باب from text."""
    # Pattern: كتاب X: باب Y or كتاب X باب Y
    m = re.search(r'(كتاب\s+[\w\s:،؛]+?)(?=\s*🔸|\s*للعلامة|\s*🔹|\s*الدرس)', text)
    if m:
        ch = m.group(1).strip()
        # Clean up trailing punctuation
        ch = ch.rstrip('.,: ')
        return ch
    return None

def extract_tasees_series(all_msgs):
    """Extract Series 3: تأسيس الأحكام شرح عمدة الأحكام - عن بعد"""
    records = []

    for mid, (msg, src) in all_msgs.items():
        audio = msg.find('a', class_='media_audio_file')
        if not audio:
            continue
        text = msg.get_text()
        if 'تأسيس الأحكام' not in text:
            continue
        if 'عن بُعد' not in text and 'عن بعد' not in text:
            continue

        audio_file = get_audio_filename(msg)
        gd = get_greg_date(msg)
        greg_slash = greg_to_slash(gd)
        clean = ' '.join(text.split())

        hijri = extract_hijri_mixed(clean, gd)
        hijri_source = 'text'
        if not hijri and gd:
            hijri = greg_to_hijri(gd)
            hijri_source = 'computed'

        duration = extract_duration(clean)

        # Extract chapter lesson number (per-chapter)
        chapter_lesson_num, _ = extract_lesson_num(clean)

        # Extract chapter name
        # Pattern: كتاب X: باب Y or كتاب X باب Y or كتاب X
        chapter = None
        m_ch = re.search(r'(كتاب\s+[^\n🔸🔹]+?)(?=\s*🔸|\s*للعلامة|\s*🔹|\s*الدرس|\n)', clean)
        if m_ch:
            chapter = m_ch.group(1).strip()
            # Remove trailing colons/spaces
            chapter = chapter.rstrip('.,: ')
            # Limit to reasonable length
            if len(chapter) > 80:
                chapter = chapter[:80]

        records.append({
            'mid': mid,
            'src': src,
            'chapter_lesson_num': chapter_lesson_num,
            'chapter': chapter or '',
            'audio': audio_file or '',
            'duration': duration or '',
            'greg_date': greg_slash or '',
            'hijri': hijri or '',
            'hijri_source': hijri_source,
        })

    # Sort by date (mid as proxy for date order)
    records.sort(key=lambda r: int(r['mid'] or 0))
    # Assign sequential DB numbers
    for i, r in enumerate(records, 1):
        r['db_num'] = i

    return records

# ── Output writing ────────────────────────────────────────────────────────────

OUTPUT_DIR = '/home/user/TeleAudio/output/corrected'
os.makedirs(OUTPUT_DIR, exist_ok=True)

SHEIKH = 'حسن بن محمد منصور الدغريري'

def write_afnan(records):
    fname = os.path.join(OUTPUT_DIR, 'afnan_buud_upload_data.txt')
    html_used = sorted(set(r['src'] for r in records))
    lines = []
    lines.append('# الأفنان الندية - عن بعد — Upload Data')
    lines.append('# Series: الأفنان الندية شرح السبل السوية في فقه السنن المروية')
    lines.append('# Author: للعلامة زيد بن هادي المدخلي رحمه الله')
    lines.append(f'# Total: {len(records)} lessons')
    lines.append(f'# Source: {", ".join(html_used)}')
    lines.append('# NOTE: LessonNum is per-chapter (resets with each new book/chapter).')
    lines.append('#       DB is sequential by date order (1-50). Use LessonNum + Chapter together.')
    lines.append('')

    for r in records:
        lines.append('---')
        lines.append(f'DB: {r["db_num"]}')
        lines.append(f'LessonNum: {r["lesson_num"] if r["lesson_num"] is not None else "?"}')
        lines.append(f'LessonLine: {r["lesson_line"]}')
        if r['chapter']:
            lines.append(f'Chapter: {r["chapter"]}')
        lines.append('Series: الأفنان الندية شرح السبل السوية في فقه السنن المروية')
        lines.append(f'Sheikh: {SHEIKH}')
        lines.append(f'Audio: {r["audio"]}')
        lines.append(f'Duration: {r["duration"]}')
        lines.append(f'GregDate: {r["greg_date"]}')
        lines.append(f'HijriDate: {r["hijri"]}')
        lines.append(f'HijriSource: {r["hijri_source"]}')
        lines.append('Location: Online')
        lines.append(f'MsgID: {r["mid"]}')
        lines.append('')

    with open(fname, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'DONE: {fname}')
    return fname

def write_maarej(records):
    fname = os.path.join(OUTPUT_DIR, 'maarej_upload_data.txt')
    html_used = sorted(set(r['src'] for r in records))
    lines = []
    lines.append('# معارج القبول شرح منظومة سلم الوصول — Upload Data')
    lines.append('# Author: للعلامة حافظ بن أحمد الحكمي رحمه الله')
    no_audio = [r for r in records if not r['has_audio']]
    lines.append(f'# Total: {len(records)} lessons ({len(no_audio)} without audio)')
    lines.append(f'# Source: {", ".join(html_used)}')
    lines.append(f'# Index from: mid=6971')
    lines.append('')

    for r in records:
        lines.append('---')
        lines.append(f'DB: {r["lesson_num"]}')
        lines.append(f'LessonNum: {r["lesson_num"]}')
        lines.append('Series: معارج القبول شرح منظومة سلم الوصول')
        lines.append(f'Sheikh: {SHEIKH}')
        lines.append(f'Audio: {r["audio"]}')
        if not r['has_audio']:
            lines.append('Note: No audio attachment in Telegram message')
        lines.append(f'Duration: {r["duration"]}')
        lines.append(f'GregDate: {r["greg_date"]}')
        lines.append(f'HijriDate: {r["hijri"]}')
        lines.append(f'HijriSource: {r["hijri_source"]}')
        lines.append('Location: Online')
        lines.append(f'MsgID: {r["mid"]}')
        lines.append('')

    with open(fname, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'DONE: {fname}')
    return fname

def write_tasees(records):
    fname = os.path.join(OUTPUT_DIR, 'tasees_buud_upload_data.txt')
    html_used = sorted(set(r['src'] for r in records))
    lines = []
    lines.append('# تأسيس الأحكام - عن بعد — Upload Data')
    lines.append('# Series: تأسيس الأحكام على ما صح من خير الأنام بشرح عمدة الأحكام')
    lines.append('# Author: للعلامة أحمد بن يحي النجمي رحمه الله')
    lines.append(f'# Total: {len(records)} lessons')
    lines.append(f'# Source: {", ".join(html_used)}')
    lines.append('')

    for r in records:
        lines.append('---')
        lines.append(f'DB: {r["db_num"]}')
        lines.append(f'LessonNum: {r["db_num"]}')
        lines.append(f'ChapterLessonNum: {r["chapter_lesson_num"] if r["chapter_lesson_num"] is not None else "?"}')
        if r['chapter']:
            lines.append(f'Chapter: {r["chapter"]}')
        lines.append('Series: تأسيس الأحكام على ما صح من خير الأنام بشرح عمدة الأحكام')
        lines.append(f'Sheikh: {SHEIKH}')
        lines.append(f'Audio: {r["audio"]}')
        lines.append(f'Duration: {r["duration"]}')
        lines.append(f'GregDate: {r["greg_date"]}')
        lines.append(f'HijriDate: {r["hijri"]}')
        lines.append(f'HijriSource: {r["hijri_source"]}')
        lines.append('Location: Online')
        lines.append(f'MsgID: {r["mid"]}')
        lines.append('')

    with open(fname, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'DONE: {fname}')
    return fname

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    all_msgs = load_all_messages()

    print("\n── Series 1: الأفنان الندية - عن بعد ──")
    afnan_records = extract_afnan_series(all_msgs)
    print(f"Found {len(afnan_records)} records")
    for r in afnan_records:
        print(f"  DB{r['db_num']}: mid={r['mid']} LessonNum={r['lesson_num']} ch={r['chapter'][:35] if r['chapter'] else ''} gd={r['greg_date']} h={r['hijri']}")
    missing_nums = [r for r in afnan_records if r['lesson_num'] is None]
    if missing_nums:
        print(f"WARNING: {len(missing_nums)} records with no lesson number: mids={[r['mid'] for r in missing_nums]}")

    print("\n── Series 2: معارج القبول ──")
    maarej_records = extract_maarej_series(all_msgs)
    print(f"Found {len(maarej_records)} records")
    for r in maarej_records:
        audio_note = '(NO AUDIO)' if not r['has_audio'] else ''
        print(f"  L{r['lesson_num']}: mid={r['mid']} gd={r['greg_date']} h={r['hijri']} dur={r['duration']} {audio_note}")

    print("\n── Series 3: تأسيس الأحكام - عن بعد ──")
    tasees_records = extract_tasees_series(all_msgs)
    print(f"Found {len(tasees_records)} records")
    for r in tasees_records:
        print(f"  DB{r['db_num']}: mid={r['mid']} ChapterL={r['chapter_lesson_num']} ch={r['chapter'][:40] if r['chapter'] else ''} gd={r['greg_date']}")

    print("\n── Writing output files ──")
    write_afnan(afnan_records)
    write_maarej(maarej_records)
    write_tasees(tasees_records)

    print("\n── Summary ──")
    print(f"الأفنان: {len(afnan_records)} lessons")
    print(f"معارج: {len(maarej_records)} lessons")
    print(f"تأسيس: {len(tasees_records)} lessons")

if __name__ == '__main__':
    main()
