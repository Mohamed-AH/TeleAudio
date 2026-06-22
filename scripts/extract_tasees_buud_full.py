#!/usr/bin/env python3
"""تأسيس الأحكام - عن بعد: final extraction, sorted chronologically by mid."""

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

# Comprehensive ordinals: both الواحد/الحادي forms
UNITS   = ['الأول','الثاني','الثالث','الرابع','الخامس','السادس','السابع','الثامن','التاسع']
COMPOUND= [('الواحد',1),('الحادي',1),('الثاني',2),('الثالث',3),('الرابع',4),
           ('الخامس',5),('السادس',6),('السابع',7),('الثامن',8),('التاسع',9)]
TENS    = [('عشرون',20),('ثلاثون',30),('أربعون',40),('خمسون',50),
           ('ستون',60),('سبعون',70),('ثمانون',80),('تسعون',90)]
ORD = {}
for i,w in enumerate(UNITS,1): ORD[w] = i
ORD['العاشر'] = 10
for i,unit in enumerate(['الحادي','الثاني','الثالث','الرابع','الخامس','السادس','السابع','الثامن','التاسع'],11):
    ORD[f'{unit} عشر'] = i
for tw,tv in TENS:
    ORD[f'ال{tw}'] = tv
    for u,uv in COMPOUND:
        ORD[f'{u} وال{tw}'] = tv + uv
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
SKIP_MIDS   = {5798, 5799, 5800, 5801, 5802, 5803, 5804, 5805}

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
    """Return string description of lesson number (for metadata only)."""
    # Numeric
    m = re.search(r'(?:الدرس|درس)\s*:?\s*(?:رقم\s*)?[\(\[]?\s*([٠-٩0-9]+)\s*[\)\]]?', text)
    if m: return ar2en(m.group(1))
    # Circle
    circ = [c for c in text if c in CIRCLE]
    if circ and 'درس' in text: return str(CIRCLE[circ[0]])
    # Ordinals
    for ordinal, num in sorted(ORD.items(), key=lambda x: -len(x[0])):
        if re.search(r'(?:الدرس|درس)\s*:?\s*'+re.escape(ordinal), text):
            return str(num)
    return ''

def extract_chapter(text):
    flat = re.sub(r'\s+',' ',text)
    # "تأسيس الأحكام - كتاب النكاح (١)" 
    m = re.search(r'تأسيس الأحكام\s*[-–]\s*([^\n\[\]]+?)(?:\s*\]|$)', flat)
    if m: return m.group(1).strip()[:60]
    # "[تأسيس الأحكام - باب الربا ]"
    m = re.search(r'\[تأسيس الأحكام\s*[-–]\s*([^\[\]]+?)\]', flat)
    if m: return m.group(1).strip()[:60]
    # "[كتاب الزكاة (١) ]"
    m = re.search(r'\[\s*(كتاب [^\[\]]+?)\s*\]', flat)
    if m: return m.group(1).strip()[:60]
    # "#كتاب_الصيام"
    m = re.search(r'#((?:كتاب|باب)_\S+)', flat)
    if m: return m.group(1).replace('_',' ')
    # "كتاب الصيام" in context line
    m = re.search(r'(كتاب\s+\w+(?:\s+\w+)?)', flat)
    if m: return m.group(1).strip()[:40]
    return ''

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
        mid_data[mid] = {'audio':audio,'gd':gd,'gd_raw':gd_raw,'text':text,'has_audio':bool(audio_el)}

# ── Filter & sort by mid (chronological) ──────────────────────────────────────
candidates = sorted(
    [mid for mid, r in mid_data.items()
     if mid not in SKIP_MIDS
     and r['has_audio']
     and any(k in r['text'] for k in SERIES_KEYS)
     and not any(k in r['text'] for k in MOSQUE_KEYS)]
)
print(f"Audio candidates: {len(candidates)}")

# ── Extract & write ────────────────────────────────────────────────────────────
print(f"\n{'DB':>4}  {'L#':>5}  {'MsgID':>6}  {'GregDate':12}  {'HijriDate':12}  Chapter")
rows = []
for db, mid in enumerate(candidates, 1):
    r = mid_data[mid]
    text = r['text']
    lnum   = extract_lesson_num_str(text)
    chap   = extract_chapter(text)
    hijri  = extract_hijri(text) or greg_to_hijri(r['gd_raw'])
    dur    = extract_duration(text) or ''
    gd     = reformat(r['gd'])
    rows.append([str(db), lnum, str(mid), gd, hijri or '', r['audio'], dur, chap])
    print(f"{db:4}  {lnum:>5}  {mid:6}  {gd:12}  {(hijri or ''):12}  {chap[:50]}")

out = '/home/user/TeleAudio/output/corrected/tasees_buud.csv'
with open(out,'w',encoding='utf-8-sig',newline='') as f:
    w = csv.writer(f)
    w.writerow(['DB#','LessonNumInText','MsgID','GregDate','HijriDate','AudioFilename','Duration','ChapterNote'])
    for row in rows:
        w.writerow(row)

print(f"\nWritten {len(rows)} rows → {out}")
