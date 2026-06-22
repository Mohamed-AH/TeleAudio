#!/usr/bin/env python3
"""
Extract upload data for three Islamic lecture series from HTML files.
Series: فتاوى أركان الإسلام, شرح كتاب الفقه الميسر, القول السديد شرح كتاب التوحيد
"""

from bs4 import BeautifulSoup
import re, csv, sys

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
    m = re.search(r'([٠-٩]{4})[/٫]\s*([٠-٩]{1,2})[/٫]\s*([٠-٩]{1,2})', text)
    if m:
        y,mo,d = ar2en(m.group(1)), ar2en(m.group(2)), ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    m = re.search(r'([0-9]{1,2})\s*/\s*([0-9]{1,2})\s*/\s*([0-9]{4})\s*[هﻫ]', text)
    if m: return f"{m.group(3)}/{int(m.group(2)):02d}/{int(m.group(1)):02d}"
    m = re.search(r'([٠-٩]{1,2})\s*/\s*([٠-٩]{1,2})\s*/\s*([٠-٩]{4})', text)
    if m:
        d,mo,y = ar2en(m.group(1)), ar2en(m.group(2)), ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    m = re.search(r'([٠-٩]{1,2})\s*-\s*([٠-٩]{1,2})\s*-\s*([٠-٩]{4})', text)
    if m:
        d,mo,y = ar2en(m.group(1)), ar2en(m.group(2)), ar2en(m.group(3))
        return f"{y}/{int(mo):02d}/{int(d):02d}"
    m = re.search(r'([0-9]{1,2})\s*-\s*([0-9]{1,2})\s*-\s*([0-9]{4})', text)
    if m: return f"{m.group(3)}/{int(m.group(2)):02d}/{int(m.group(1)):02d}"
    for mn, mnum in MONTHS.items():
        m = re.search(r'([0-9]{1,2})\s+' + re.escape(mn) + r'\s+([0-9]{4})', text)
        if m: return f"{m.group(2)}/{mnum:02d}/{int(m.group(1)):02d}"
        m = re.search(r'([٠-٩]{1,2})\s+' + re.escape(mn) + r'\s+([٠-٩]{4})', text)
        if m:
            d2, y2 = ar2en(m.group(1)), ar2en(m.group(2))
            return f"{y2}/{mnum:02d}/{int(d2):02d}"
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
    m = re.search(r'(\d{1,3}:\d{2})\s*(?:دقيقة|د\b)', text)
    if m: return m.group(1)
    return None

def greg_to_hijri(gd_str):
    from hijri_converter import convert
    d, m, y = gd_str.split('.')
    try:
        h = convert.Gregorian(int(y), int(m), int(d)).to_hijri()
        return f"{h.year}/{h.month:02d}/{h.day:02d}"
    except: return ''

def reformat_date(d):
    if not d: return ''
    m = re.match(r'(\d{2})/(\d{2})/(\d{4})', d)
    return f"{m.group(1)}.{m.group(2)}.{m.group(3)}" if m else d

# ── Load all HTML files ────────────────────────────────────────────────────────
HTML_FILES = [
    '/home/user/TeleAudio/messages.html',
    '/home/user/TeleAudio/messages2.html',
    '/home/user/TeleAudio/messages3.html',
    '/home/user/TeleAudio/messages4.html',
    '/home/user/TeleAudio/messages_new.html',
    '/home/user/TeleAudio/messages_june1.html',
]

print("Loading HTML files...", flush=True)
mid_data = {}  # mid -> {'audio': filename, 'gd': 'DD/MM/YYYY', 'text': raw_text, 'gd_raw': 'DD.MM.YYYY'}
seen = set()
for path in HTML_FILES:
    try:
        with open(path, encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'lxml')
        count = 0
        for div in soup.select('div[id^=message]'):
            mid_str = re.sub(r'[^0-9]', '', div.get('id','0'))
            if not mid_str: continue
            mid = int(mid_str)
            if mid in seen: continue
            seen.add(mid)
            text_div = div.select_one('div.text')
            audio_el = div.select_one('a.media_audio_file')
            date_el = div.select_one('div.date[title]')
            if not date_el: continue
            gd_raw = date_el['title'].split()[0]  # "DD.MM.YYYY"
            parts = gd_raw.split('.')
            if len(parts) != 3: continue
            d, mo, y = parts
            gd = f"{d}/{mo}/{y}"  # DD/MM/YYYY
            text = text_div.get_text() if text_div else ''
            audio = audio_el['href'].split('/')[-1] if audio_el else ''
            mid_data[mid] = {'audio': audio, 'gd': gd, 'text': text, 'gd_raw': gd_raw}
            count += 1
        print(f"  {path.split('/')[-1]}: {count} messages loaded")
    except FileNotFoundError:
        print(f"  WARNING: {path} not found, skipping")

print(f"Total unique messages loaded: {len(mid_data)}")
print()

# ── Helper ────────────────────────────────────────────────────────────────────
def get_row_arkan(mid, db_num, section, lesson_num):
    if mid not in mid_data:
        return [str(db_num), section, str(lesson_num), str(mid), '', '', '(mid not found)', '']
    r = mid_data[mid]
    audio = r['audio'] if r['audio'] else '(no audio attachment)'
    hijri = extract_hijri(r['text']) or greg_to_hijri(r['gd_raw'])
    duration = extract_duration(r['text']) or ''
    return [str(db_num), section, str(lesson_num), str(mid),
            reformat_date(r['gd']), hijri or '', audio, duration]

def get_row_simple(mid, db_num, lesson_num):
    if mid not in mid_data:
        return [str(db_num), str(lesson_num), str(mid), '', '', '(mid not found)', '']
    r = mid_data[mid]
    audio = r['audio'] if r['audio'] else '(no audio attachment)'
    hijri = extract_hijri(r['text']) or greg_to_hijri(r['gd_raw'])
    duration = extract_duration(r['text']) or ''
    return [str(db_num), str(lesson_num), str(mid),
            reformat_date(r['gd']), hijri or '', audio, duration]

# ═══════════════════════════════════════════════════════════════════════════════
# SERIES 1: فتاوى أركان الإسلام
# ═══════════════════════════════════════════════════════════════════════════════
print("=== SERIES 1: فتاوى أركان الإسلام ===")

arkan_sections = [
    {
        'name': 'فتاوى العقيدة',
        'lessons': {
            1:3544, 2:3555, 3:3566, 4:3574, 5:3587, 6:3613, 7:3619, 8:3637, 9:3654, 10:3659,
            11:3674, 12:3677, 13:3681, 14:3692, 15:3701, 16:3716, 17:3724, 18:3743, 19:3749, 20:3770,
            21:3780, 22:3790, 23:3823, 24:3824, 25:3825, 26:3826,
            # L27 MISSING — skip
            28:3863, 29:3875, 30:3935,
            31:3949, 32:3988, 33:4049, 34:4051, 35:4101, 36:4129, 37:4131, 38:4141, 39:4157, 40:4166,
            41:4173, 42:4186, 43:4191, 44:4217, 45:4227, 46:4232, 47:4234, 48:4236, 49:4321, 50:4331,
        }
    },
    {
        'name': 'كتاب الصيام',
        'lessons': {
            1:4243, 2:4247, 3:4251, 4:4253, 5:4255, 6:4257, 7:4264, 8:4269, 9:4273, 10:4275, 11:4279, 12:4281, 13:4289
        }
    },
    {
        'name': 'كتاب الصلاة',
        'lessons': {
            1:4350, 2:4352, 3:4366, 4:4380, 5:4384, 6:4389, 7:4401, 8:4498, 9:4500, 10:4505,
            11:4509, 12:4518, 13:4526, 14:4528, 15:4542, 16:4544, 17:4546, 18:4552, 19:4556, 20:4558,
            21:4560, 22:4596, 23:4614, 24:4621, 25:4623, 26:4657, 27:4663, 28:4669, 29:4682, 30:4712,
            31:4716, 32:4720, 33:4727, 34:4731, 35:4761, 36:4775, 37:4793, 38:4821, 39:4832, 40:4834,
            41:4857, 42:4863, 43:4883, 44:4885, 45:4921, 46:4960, 47:4962, 48:4989, 49:4993, 50:5068,
            51:5070, 52:5077, 53:5111, 54:5120, 55:5132, 56:5139, 57:5164, 58:5168, 59:5345, 60:5350,
            61:5366, 62:5373, 63:5464, 64:5482, 65:5484, 66:5486, 67:5499, 68:5511,
        }
    },
    {
        'name': 'كتاب الحج',
        'lessons': {
            1:4410, 2:4422, 3:4427, 4:4432, 5:4437, 6:4440, 7:4469, 8:5397, 9:5400, 10:5428, 11:5434, 12:5443, 13:5450
        }
    },
    {
        'name': 'كتاب الزكاة',
        'lessons': {
            1:5521, 2:5584, 3:5586, 4:5599, 5:5605, 6:5629, 7:5637, 8:5643
        }
    },
]

# Find highest mid in arkan index
all_arkan_mids = set()
for sec in arkan_sections:
    all_arkan_mids.update(sec['lessons'].values())
arkan_max_mid = 5643

# Post-index search
print(f"Searching for post-index messages (mid > {arkan_max_mid}) with 'فتاوى أركان' or 'أركان الإسلام'...")
arkan_post = []
for mid in sorted(mid_data.keys()):
    if mid <= arkan_max_mid:
        continue
    r = mid_data[mid]
    if not r['audio']:
        continue
    text = r['text']
    if 'فتاوى أركان' in text or 'أركان الإسلام' in text:
        # Determine section from text
        section = ''
        if 'العقيدة' in text:
            section = 'فتاوى العقيدة'
        elif 'الصيام' in text:
            section = 'كتاب الصيام'
        elif 'الصلاة' in text:
            section = 'كتاب الصلاة'
        elif 'الحج' in text:
            section = 'كتاب الحج'
        elif 'الزكاة' in text:
            section = 'كتاب الزكاة'
        arkan_post.append((mid, section, r['text'][:100]))
        print(f"  Post-index mid={mid}: section='{section}' text={r['text'][:60]!r}")

print(f"Post-index arkan lessons found: {len(arkan_post)}")
print()

# Build arkan rows
arkan_rows = []
db_num = 0
not_found_arkan = []

for sec in arkan_sections:
    for lesson_num in sorted(sec['lessons'].keys()):
        mid = sec['lessons'][lesson_num]
        db_num += 1
        row = get_row_arkan(mid, db_num, sec['name'], lesson_num)
        arkan_rows.append(row)
        if '(mid not found)' in row:
            not_found_arkan.append(mid)

# Add post-index
for (mid, section, _) in arkan_post:
    db_num += 1
    row = get_row_arkan(mid, db_num, section, 'post-index')
    arkan_rows.append(row)

# Write arkan CSV
arkan_csv = '/home/user/TeleAudio/output/corrected/arkan_islam.csv'
with open(arkan_csv, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['DB#', 'Section', 'Lesson#', 'MsgID', 'GregDate', 'HijriDate', 'AudioFilename', 'Duration'])
    writer.writerows(arkan_rows)

print(f"arkan_islam.csv: {len(arkan_rows)} rows written")
if not_found_arkan:
    print(f"  MIDs not found in HTML: {not_found_arkan}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SERIES 2: شرح كتاب الفقه الميسر
# ═══════════════════════════════════════════════════════════════════════════════
print("=== SERIES 2: شرح كتاب الفقه الميسر ===")

# L8, L11, L14, L33 MISSING — include row with MsgID="(missing from index)"
fiqh_lessons = {
    1:3878, 2:307, 3:308, 4:317, 5:318, 6:392, 7:393,
    # L8 MISSING
    9:397, 10:399,
    # L11 MISSING
    12:416, 13:417,
    # L14 MISSING
    15:418, 16:419, 17:428, 18:430, 19:435, 20:447, 21:452, 22:458,
    23:561, 24:562, 25:563, 26:564, 27:565, 28:566, 29:567, 30:568, 31:569, 32:570,
    # L33 MISSING
    34:599, 35:615, 36:626, 37:644, 38:648, 39:668, 40:677, 41:716,
    42:750, 43:796, 44:840, 45:886, 46:887, 47:889, 48:902, 49:903, 50:914,
    51:923, 52:980, 53:1000, 54:1002, 55:1018, 56:1045, 57:1062, 58:1097, 59:1099, 60:1118,
    61:1122, 62:1134, 63:1166, 64:1168, 65:1181, 66:1193, 67:1202, 68:1211, 69:1236, 70:1249,
    71:1264, 72:1283, 73:1294, 74:1303, 75:1309, 76:1327, 77:1338, 78:1370, 79:1377, 80:1394,
    81:1404, 82:1412, 83:1422, 84:1437, 85:1451, 86:1460, 87:1476, 88:1541, 89:1548, 90:1576,
    91:1602, 92:1631, 93:1654, 94:1718, 95:1732, 96:1744, 97:1750, 98:1761, 99:1769, 100:1790,
    101:1801, 102:1817, 103:1830, 104:1845, 105:1858, 106:1881, 107:1898, 108:1924, 109:1950, 110:1959,
    111:1967, 112:1981, 113:2014, 114:2048, 115:2090, 116:2179, 117:2194, 118:2213, 119:2224, 120:2241,
    121:2243, 122:2257, 123:2367, 124:2389, 125:2400, 126:2410, 127:2416, 128:2426,
}

missing_fiqh_lessons = {8, 11, 14, 33}

# دروس من entries at end
durus_min = [
    (129, 'دروس من كتاب الصيام', 944),
    (130, 'دروس من كتاب الطهارة', 1034),
    (131, 'دروس من كتاب الصلاة', 1075),
    (132, 'دروس من كتاب الحج', 1101),
    (133, 'دروس من كتاب الجهاد', 1136),
    (134, 'دروس من كتاب الصيام', 1462),
]

fiqh_max_mid = 2426
print(f"Searching for post-index messages (mid > {fiqh_max_mid}) with 'الفقه الميسر'...")
fiqh_post = []
for mid in sorted(mid_data.keys()):
    if mid <= fiqh_max_mid:
        continue
    r = mid_data[mid]
    if not r['audio']:
        continue
    if 'الفقه الميسر' in r['text']:
        fiqh_post.append(mid)
        print(f"  Post-index mid={mid}: text={r['text'][:60]!r}")

print(f"Post-index fiqh_muyassar lessons found: {len(fiqh_post)}")
print()

# Build fiqh rows
fiqh_rows = []
not_found_fiqh = []

# All lesson numbers 1-128 inclusive
for lnum in range(1, 129):
    db_num = lnum
    if lnum in missing_fiqh_lessons:
        # Missing from index — blank row
        fiqh_rows.append([str(db_num), str(lnum), '(missing from index)', '', '', '', ''])
    elif lnum in fiqh_lessons:
        mid = fiqh_lessons[lnum]
        row = get_row_simple(mid, db_num, lnum)
        fiqh_rows.append(row)
        if '(mid not found)' in row:
            not_found_fiqh.append(mid)
    else:
        # Shouldn't happen
        fiqh_rows.append([str(db_num), str(lnum), '(not in index)', '', '', '', ''])

# دروس من entries DB 129-134
for (db_num, label, mid) in durus_min:
    row = get_row_simple(mid, db_num, label)
    fiqh_rows.append(row)
    if '(mid not found)' in row:
        not_found_fiqh.append(mid)

# Post-index
for i, mid in enumerate(fiqh_post, start=135):
    row = get_row_simple(mid, i, 'post-index')
    fiqh_rows.append(row)

# Write fiqh CSV
fiqh_csv = '/home/user/TeleAudio/output/corrected/fiqh_muyassar_old.csv'
with open(fiqh_csv, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['DB#', 'Lesson#', 'MsgID', 'GregDate', 'HijriDate', 'AudioFilename', 'Duration'])
    writer.writerows(fiqh_rows)

print(f"fiqh_muyassar_old.csv: {len(fiqh_rows)} rows written")
if not_found_fiqh:
    print(f"  MIDs not found in HTML: {not_found_fiqh}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SERIES 3: القول السديد شرح كتاب التوحيد
# ═══════════════════════════════════════════════════════════════════════════════
print("=== SERIES 3: القول السديد شرح كتاب التوحيد ===")

qawl_lessons = {
    1:4678, 2:4709, 3:4722, 4:4759, 5:4825, 6:4861, 7:4878, 8:4947, 9:4980, 10:5031, 11:5065, 12:5091, 13:5107
}

qawl_max_mid = 5107
print(f"Searching for post-index messages (mid > {qawl_max_mid}) with 'القول السديد'...")
qawl_post = []
for mid in sorted(mid_data.keys()):
    if mid <= qawl_max_mid:
        continue
    r = mid_data[mid]
    if not r['audio']:
        continue
    if 'القول السديد' in r['text']:
        qawl_post.append(mid)
        print(f"  Post-index mid={mid}: text={r['text'][:80]!r}")

print(f"Post-index qawl_sadeed lessons found: {len(qawl_post)}")
print()

# Build qawl rows
qawl_rows = []
not_found_qawl = []

for lnum in sorted(qawl_lessons.keys()):
    db_num = lnum
    mid = qawl_lessons[lnum]
    row = get_row_simple(mid, db_num, lnum)
    qawl_rows.append(row)
    if '(mid not found)' in row:
        not_found_qawl.append(mid)

# Post-index
for i, mid in enumerate(qawl_post, start=len(qawl_lessons)+1):
    row = get_row_simple(mid, i, 'post-index')
    qawl_rows.append(row)

# Write qawl CSV
qawl_csv = '/home/user/TeleAudio/output/corrected/qawl_sadeed.csv'
with open(qawl_csv, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['DB#', 'Lesson#', 'MsgID', 'GregDate', 'HijriDate', 'AudioFilename', 'Duration'])
    writer.writerows(qawl_rows)

print(f"qawl_sadeed.csv: {len(qawl_rows)} rows written")
if not_found_qawl:
    print(f"  MIDs not found in HTML: {not_found_qawl}")
print()

# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"arkan_islam.csv       : {len(arkan_rows)} rows  |  post-index: {len(arkan_post)}")
print(f"fiqh_muyassar_old.csv : {len(fiqh_rows)} rows  |  post-index: {len(fiqh_post)}")
print(f"qawl_sadeed.csv       : {len(qawl_rows)} rows  |  post-index: {len(qawl_post)}")
print()
if not_found_arkan:
    print(f"Arkan MIDs not found in HTML: {not_found_arkan}")
if not_found_fiqh:
    print(f"Fiqh MIDs not found in HTML:  {not_found_fiqh}")
if not_found_qawl:
    print(f"Qawl MIDs not found in HTML:  {not_found_qawl}")
print()
print("Files written to /home/user/TeleAudio/output/corrected/")
