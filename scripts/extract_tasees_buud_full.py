#!/usr/bin/env python3
"""تأسيس الأحكام - عن بعد: COMPLETE extraction including external-link lessons."""

from bs4 import BeautifulSoup
import re, csv

AR = {'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}
def ar2en(s): return ''.join(AR.get(c,c) for c in s)

MONTHS = {
    'محرم':1,'صفر':2,'ربيع الأول':3,'ربيع الثاني':4,'ربيع الآخر':4,
    'جمادى الأولى':5,'جمادى الأول':5,'جمادى الثانية':6,'جمادى الآخرة':6,'جمادى الآخر':6,
    'رجب':7,'شعبان':8,'رمضان':9,'شوال':10,
    'ذو القعدة':11,'ذي القعدة':11,'ذو الحجة':12,'ذي الحجة':12,
}

UNITS   = ['الأول','الثاني','الثالث','الرابع','الخامس','السادس','السابع','الثامن','التاسع']
COMPOUND= [('الواحد',1),('الحادي',1),('الثاني',2),('الثالث',3),('الرابع',4),
           ('الخامس',5),('السادس',6),('السابع',7),('الثامن',8),('التاسع',9)]
TENS    = [('عشرون',20),('ثلاثون',30),('أربعون',40),('خمسون',50),
           ('ستون',60),('سبعون',70),('ثمانون',80),('تسعون',90)]
ORD = {}
for i,w in enumerate(UNITS,1): ORD[w] = i
ORD['العاشر'] = 10
for i,u in enumerate(['الحادي','الثاني','الثالث','الرابع','الخامس','السادس','السابع','الثامن','التاسع'],11):
    ORD[f'{u} عشر'] = i
for tw,tv in TENS:
    ORD[f'ال{tw}'] = tv
    for u,uv in COMPOUND:
        ORD[f'{u} وال{tw}'] = tv+uv
CIRCLE = {chr(0x2460+i): i+1 for i in range(20)}

HTML_FILES = [
    '/home/user/TeleAudio/messages.html',
    '/home/user/TeleAudio/messages2.html',
    '/home/user/TeleAudio/messages3.html',
    '/home/user/TeleAudio/messages4.html',
    '/home/user/TeleAudio/messages_new.html',
    '/home/user/TeleAudio/messages_june1.html',
]
SERIES_KEYS = ['تأسيس الأحكام', 'عمدة الأحكام']
MOSQUE_KEYS = ['جامع الورود', 'الورود،', 'جدة']
# External-link-only lessons to include (mid → lesson_label)
EXT_LESSONS = {
    60:   'L24',   # goo.gl, 21.02.2017
    73:   'L25',   # archive.org/25AHKAM, 14.03.2017
    78:   'L26',   # archive.org/26AHKAM, 22.03.2017
    94:   'L27',   # archive.org/27AHKAM, 29.03.2017
    113:  'L29',   # archive.org/29AHKAM, 03.05.2017
    1778: 'L33',   # top4top, 13.10.2021
}
# Skip: duplicates of ext-lesson posts, wrong-category, Google Maps link
SKIP_MIDS = {
    181,                         # L39 ext-link: Telegram audio exists at 1911
    2102,                        # announcement only
    5779,5780,5781,5782,5783,    # Sawm sub-series 2016 (chapter-local, uncertain position)
    5784,5785,5786,5787,5788,    # index / duplicate posts
    5789,5790,5791,5792,5793,    # duplicate of earlier ext posts
    5794,5795,5796,              # duplicate of earlier ext posts
    5798,5799,5800,5801,5802,    # archive batch duplicates of L34-L40 Telegram audio
    5803,5804,5805,
    6784,                        # Google Maps link (not audio)
}

def extract_hijri(text):
    m = re.search(r'[❲\[]\s*([٠-٩]{4})[/٫]\s*([٠-٩]{1,2})[/٫]\s*([٠-٩]{1,2})\s*هـ?\s*[❳\]]', text)
    if m:
        y,mo,d = ar2en(m.group(1)),ar2en(m.group(2)),ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    m = re.search(r'([٠-٩]{4})[/٫]\s*([٠-٩]{1,2})[/٫]\s*([٠-٩]{1,2})', text)
    if m:
        y,mo,d = ar2en(m.group(1)),ar2en(m.group(2)),ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    m = re.search(r'([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})\s*[هﻫ]', text)
    if m: return f"{m.group(3)}/{int(m.group(2)):02d}/{int(m.group(1)):02d}"
    for mn,mnum in MONTHS.items():
        m = re.search(r'([0-9]{1,2})\s+'+re.escape(mn)+r'\s+([0-9]{4})',text)
        if m: return f"{m.group(2)}/{mnum:02d}/{int(m.group(1)):02d}"
        m = re.search(r'([٠-٩]{1,2})\s+'+re.escape(mn)+r'\s+([٠-٩]{4})',text)
        if m:
            d2,y2 = ar2en(m.group(1)),ar2en(m.group(2))
            return f"{y2}/{mnum:02d}/{int(d2):02d}"
    return None

def extract_duration(text):
    m = re.search(r'◉\s*(\d+:\d{2}(?::\d{2})?)', text)
    if m: return m.group(1)
    m = re.search(r'00:00.{1,20}?(\d+:\d{2})', text)
    if m: return m.group(1)
    m = re.search(r'(?:مدة|المدة)\s*(?:الصوتية)?\s*[:#]\s*(\d+:\d{2})', text)
    if m: return m.group(1)
    return None

def extract_lesson_num_str(text):
    m = re.search(r'(?:الدرس|درس)\s*:?\s*(?:رقم\s*)?[\(\[-]?\s*([٠-٩0-9]+)\s*[\)\]-]?', text)
    if m: return ar2en(m.group(1))
    circ = [c for c in text if c in CIRCLE]
    if circ and 'درس' in text: return str(CIRCLE[circ[0]])
    for ordinal, num in sorted(ORD.items(), key=lambda x: -len(x[0])):
        if re.search(r'(?:الدرس|درس)\s*:?\s*'+re.escape(ordinal), text):
            return str(num)
    return ''

def extract_chapter(text):
    flat = re.sub(r'\s+',' ',text)
    m = re.search(r'تأسيس الأحكام\s*[-–]\s*([^\n\[\]]+?)(?:\s*\]|$)', flat)
    if m: return m.group(1).strip()[:60]
    m = re.search(r'\[تأسيس الأحكام\s*[-–]\s*([^\[\]]+?)\]', flat)
    if m: return m.group(1).strip()[:60]
    m = re.search(r'\[\s*(كتاب [^\[\]]+?)\s*\]', flat)
    if m: return m.group(1).strip()[:60]
    m = re.search(r'#((?:كتاب|باب)_\S+)', flat)
    if m: return m.group(1).replace('_',' ')
    m = re.search(r'(كتاب\s+\w+(?:\s+\w+)?)', flat)
    if m: return m.group(1).strip()[:40]
    return ''

def ext_audio_label(links):
    """Return short label for external audio link."""
    for link in links:
        if 'archive.org/download' in link:
            return link.split('/')[-1]  # filename from archive.org
        if 'top4top' in link:
            return link.split('/')[-1]  # filename
        if 'goo.gl' in link:
            return link  # keep full shortened URL
    return links[0] if links else ''

def greg_to_hijri(gd_raw):
    from hijri_converter import convert
    try:
        d,mo,y = gd_raw.split('.')
        h = convert.Gregorian(int(y),int(mo),int(d)).to_hijri()
        return f"{h.year}/{h.month:02d}/{h.day:02d}"
    except: return ''

def reformat(gd):
    m = re.match(r'(\d{2})/(\d{2})/(\d{4})',gd)
    return f"{m.group(1)}.{m.group(2)}.{m.group(3)}" if m else gd

# ── Load ───────────────────────────────────────────────────────────────────────
print("Loading HTML…")
mid_data = {}; seen = set()
for path in HTML_FILES:
    with open(path,encoding='utf-8') as f:
        soup = BeautifulSoup(f,'lxml')
    for div in soup.select('div[id^=message]'):
        mid_str = re.sub(r'[^0-9]','',div.get('id','0'))
        if not mid_str: continue
        mid = int(mid_str)
        if mid in seen: continue
        seen.add(mid)
        text_div = div.select_one('div.text')
        audio_el = div.select_one('a.media_audio_file')
        date_el  = div.select_one('div.date[title]')
        if not date_el: continue
        gd_raw = date_el['title'].split()[0]
        d,mo,y = gd_raw.split('.')
        gd = f"{d}/{mo}/{y}"
        text = text_div.get_text(separator='\n') if text_div else ''
        audio = audio_el['href'].split('/')[-1] if audio_el else ''
        ext_links = [a['href'] for a in div.select('a[href]')
                     if any(d in a.get('href','') for d in ['top4top','archive.org/download','goo.gl'])]
        mid_data[mid] = {
            'audio': audio, 'gd': gd, 'gd_raw': gd_raw, 'text': text,
            'has_tg': bool(audio_el), 'ext_links': ext_links
        }

# ── Build candidate list ───────────────────────────────────────────────────────
# 1. External-link lessons (ordered by mid)
# 2. Telegram audio lessons (ordered by mid)
ext_mids = sorted(m for m in EXT_LESSONS if m in mid_data)
tg_mids  = sorted(
    m for m, r in mid_data.items()
    if m not in SKIP_MIDS and m not in EXT_LESSONS
    and r['has_tg']
    and any(k in r['text'] for k in SERIES_KEYS)
    and not any(k in r['text'] for k in MOSQUE_KEYS)
)

print(f"External-link lessons: {len(ext_mids)}  — Telegram audio: {len(tg_mids)}")

# ── Combine: chronological by mid ─────────────────────────────────────────────
all_mids = sorted(set(ext_mids) | set(tg_mids))
print(f"Total: {len(all_mids)}")

# ── Extract & write ────────────────────────────────────────────────────────────
print(f"\n{'DB':>4}  {'L#':>5}  {'MsgID':>6}  {'GregDate':12}  {'HijriDate':12}  {'Audio':45}  Chapter")
rows = []
for db, mid in enumerate(all_mids, 1):
    r = mid_data[mid]
    text = r['text']
    lnum   = extract_lesson_num_str(text)
    chap   = extract_chapter(text)
    hijri  = extract_hijri(text) or greg_to_hijri(r['gd_raw'])
    dur    = extract_duration(text) or ''
    gd     = reformat(r['gd'])

    if r['has_tg']:
        audio_col = r['audio']
        note = ''
    else:
        # External link lesson
        audio_col = ext_audio_label(r['ext_links'])
        note = '(external link)'

    rows.append([str(db), lnum, str(mid), gd, hijri or '', audio_col, dur, chap, note])
    audio_disp = audio_col[:43] + '..' if len(audio_col) > 45 else audio_col
    print(f"{db:4}  {lnum:>5}  {mid:6}  {gd:12}  {(hijri or ''):12}  {audio_disp:45}  {chap[:35]}")

out = '/home/user/TeleAudio/output/corrected/tasees_buud.csv'
with open(out,'w',encoding='utf-8-sig',newline='') as f:
    w = csv.writer(f)
    w.writerow(['DB#','LessonNumInText','MsgID','GregDate','HijriDate','AudioFilename','Duration','ChapterNote','AudioType'])
    for row in rows:
        w.writerow(row)

print(f"\nWritten {len(rows)} rows → {out}")
