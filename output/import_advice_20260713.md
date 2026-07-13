# Import Advice Report — 2026-07-13
**Source: PDF site report 2026-07-13 (1,687 lectures, 73 series, 0 drafts)**

---

## DB SNAPSHOT (authoritative, from PDF)

| Metric | Value |
|--------|-------|
| Total lectures | 1,687 |
| Total series | 73 |
| Drafts | 0 |
| Report date | 2026-07-13 |

---

## PRIORITY 1 — LARGE BACKLOGS (series with many unuploaded lessons)

### 1. التحفة النجمية بشرح الأربعين النووية
- **DB count:** [6] (السلاسل المكتملة)
- **Upload file:** `التحفة_النجمية_بشرح_الأربعين_النووية_upload.txt`
- **Status:** DB had 7 in earlier analysis (now 6 — 1 possibly removed). Upload file identifies **50 entries** to upload, covering:
  - Lessons 8–42 from early channel (msg IDs 737–1031) — ~35 unique lessons
  - Lessons 1–5 from newer recordings (msg IDs 6062–7423) — including duplicate markers
- **Net to upload:** ~35 clean lessons (after deduplication)
- **Action:** This is the biggest clean backlog. Upload lessons 8–42 in order. Resolve the duplicate flags manually.
- **Upload file start:** lesson 8 = t.me/daririhasan/737

---

### 2. دروس رمضان - وزارة الشؤون الإسلامية
- **DB count:** [0] (series exists, completely empty)
- **Telegram content:** 30 files confirmed (newly_identified_series.csv + upload file)
- **Upload file:** `دروس_رمضان_وزارة_الشؤون_الإسلامية_upload.txt` lists lessons 22–30 (9 more). `newly_identified_series.csv` has 30 files labeled "1447هـ"
- **Note:** Upload file says "DB has 22" — this is STALE. PDF is authoritative: DB = 0.
- **Action:** Upload all 30 lessons. Use newly_identified_series.csv for filenames + upload file for lesson 22–30 detail.

---

### 3. تأسيس الأحكام شرح عمدة الأحكام (mosque main series)
- **DB count:** [46]
- **Upload file:** `tasees_alahkam_upload_data.txt`
- **Status:** Upload file was written when DB had 24. It lists DB lessons 25–51 = 27 more. PDF shows 46 → 22 were uploaded. Remaining: **~5 lessons** (approx DB 47–51).
- **KNOWN GAP:** كتاب الصلاة lessons 18–26 (9 lessons) were NEVER posted to channel. DB numbering in the upload file reserves those slots.
- **KNOWN DUPLICATE:** "الثامن والعشرون" used twice (msg 6580 + msg 6664). Need `(تابع)` added to second.
- **EXCLUDED (separate series needed):** 5 lessons on كتاب القصاص/الحدود "عن بُعد" (msg 6375, 6423, 6707, 7139, 7268) — these belong to a new sub-series entry, NOT the main mosque series.
- **Action:** Upload remaining lessons 47–51 from upload file. Separately create a new series "تأسيس الأحكام — كتاب القصاص والحدود (عن بعد)" for the 5 excluded lessons.

---

## PRIORITY 2 — PARTIAL SERIES (moderate gaps)

### 4. الملخص الفقهي
- **DB count:** [51]
- **Upload file:** `الملخص_الفقهي_upload.txt`
- **Status:** File was written when DB had 35; it lists 25 more (through lesson 48). DB is now at 51 → 16 were uploaded. Remaining: ~9 entries from the file.
- **Remaining lessons to check:** lesson 47 (possibly missing — file jumps from 46 to 48), plus several "درس (رقم غير محدد)" entries (msg 5697, 6570, 6689, 6769, 6784).
- **Action:** Check if lesson 47 exists (it may be one of the unidentified msgs). Upload any clean remaining lessons. Flag 5 unidentified msgs for manual review.

---

### 5. إتمام المنة بشرح أصول السنة
- **DB count:** [4] (السلاسل المكتملة)
- **Upload file:** `إتمام_المنة_بشرح_أصول_السنة_upload.txt`
- **Status:** File was written when DB had 3; it lists 8 entries to upload. Now DB has 4 → 1 uploaded. Remaining: ~4 clean lessons.
- **Entries:** Old channel (msg 2748–2878: lessons 3–6) + newer (msg 6380: lesson 3 re-post, msg 6651: lesson 4 duplicate, msg 6817: unidentified).
- **Action:** Upload lessons 3–6 from old channel (msg 2748–2878). Resolve the lesson 3 re-post and lesson 4 duplicate with manual check. Series is labeled complete — bring it to full count.

---

### 6. التعليقات الأثرية على العقيدة الواسطية (2026 series)
- **DB count:** [7]
- **Upload file:** `wasatiyya_new_2026_upload_data.txt`
- **Status:** Upload file was written when DB had 0 and prepared 5 lessons (AUDIO-2026-04-11 through AUDIO-2026-05-18). DB now has [7] → all 5 were uploaded + 2 more came from somewhere (possibly lessons from before Apr 2026).
- **Action:** Check Telegram HTML for any lessons after AUDIO-2026-05-18 (end of Ramadan 1447). The series is ongoing (جامع الورود location). Look for AUDIO-2026-06-* or AUDIO-2026-07-* files tagged to this series.

---

## PRIORITY 3 — ACTIVE ONGOING SERIES (check for new lessons post-May 2026)

The PDF is dated 2026-07-13. These series had recent activity up to May/June 2026. Since today is 2026-07-13 (6-8 weeks later), new lessons may exist in Telegram that aren't in the export.

| Series | DB Count | Last Known File | Notes |
|--------|----------|-----------------|-------|
| الأفنان الندية | 183 | AUDIO-2026-05-18 (كتاب النكاح, lesson 14) | Active weekly |
| تأسيس الأحكام عن بعد | 134 | AUDIO-2026-05-14 (lesson 134) | Active weekly |
| الملخص شرح كتاب التوحيد | 53 | almlkhs-shrh-ktab-atwhyd-adrs-53.m4a | Check for new |
| صحيح البخاري | 10 | shyh-albkhary-adrs-10.m4a | Active — new lessons likely |
| معارج القبول | 28 | AUDIO-2026-05-20 (lesson 28) | May be complete |
| مختصر السيرة النبوية | 16 | mkhtsr-asyra-anbwya-adrs-16.m4a | May be complete |
| التعليقات الأثرية (2026) | 7 | AUDIO-2026-05-18 | Active — check |
| تأسيس الأحكام (main) | 46 | tasys-alahkam-shrh-amda-alahkam-adrs-46.m4a | Active |
| الملخص الفقهي | 51 | almlkhs-alfqhy-adrs-51.m4a | Active |

**Action:** Run a fresh HTML parse or manually check the Telegram export for messages after May 2026 for these series. صحيح البخاري in particular started Jan 2026 and may have grown significantly.

---

## PRIORITY 4 — SERIES AT [0] (non-actionable / CMS issues)

These appear empty in the DB but are non-issues:

| Series | DB | Reason |
|--------|-----|--------|
| تأسيس الأحكام - الطهارة | 0 | 9 Tahara lessons already in main series (lessons 1–9) |
| تيسير العلي القدير (without رحمه الله) | 0 | Same content exists under "...رحمه الله" variant [14] |
| معارج القبول - عن بعد | 0 | No "عن بعد" variant exists in Telegram export |
| الملخص الفقهي - أرشيف رمضان | 0 | Check Telegram for Ramadan-specific recordings |

**الملخص الفقهي - أرشيف رمضان:** Only 1 lesson in Telegram export (msg 2122). Upload that single lesson to populate this [0] series. See `output/mulakhkhas_fiqhi_archive_index.txt`.

---

## PRIORITY 5 — KHUTAB (خطب الجمعة)

- **DB count:** 243 main khutbas + complete sub-series
- **PDF last khutba:** #243 = khtb-aljmaa-adrs-3.m4a (Feb/Mar 2026 based on sequence)
- **Today:** 2026-07-13 = ~20 Fridays have passed since the last khutba in DB
- **Estimated gap:** ~18–20 new Friday khutbas not yet in DB
- **Action:** Parse Telegram HTML for messages after the last khutba filename date. The `خطب_الجمعة_upload.txt` file may have some of these pre-identified.

**Khutba sub-series — all confirmed complete in PDF:**
- من نواقض الإسلام [10] ✓
- وقفات مع السيرة النبوية [10] ✓  
- قصة موسى عليه السلام [8] ✓
- قصة يوسف عليه السلام [12] ✓
- قصة عيسى عليه السلام [3] ✓
- قصة طالوت وجالوت [2] ✓
- قصة آدم عليه السلام [2] ✓
- سيرة محمد بن عبدالله [4] ✓
- في ظلال آية قرآنية كريمة [4] ✓
- فيى ظلال حديث نبوي شريف [5] ✓
- وقفات مع شهر رمضان [5] ✓
- الاستعداد لرمضان [3] ✓ and [2] ✓ (two versions)
- بعض أخطاء الصائمين [2] ✓
- أعمال صالحة في شهر شعبان [1] ✓
- مختصر السيرة النبوية (khutba sub-series) [9] ✓

---

## SERIES CONFIRMED COMPLETE IN DB (no action needed)

These match their full Telegram content:

| Series | DB Count |
|--------|----------|
| الأفنان الندية - أرشيف رمضان | 10 |
| التعليق على شرح كتاب التوحيد (النجمي) | 52 |
| التعليقات الأثرية على العقيدة الواسطية (1443) | 19 |
| التعليق على كتاب مجالس شهر رمضان | 30 |
| المورد العذب الزلال | 18 |
| إرشاد الساري شرح السنة للبربهاري | 14 |
| القراءة والتعليق على كتاب أعلام السنة المنشورة | 13 |
| القراءة والتعليق على كتاب الفضل المبين | 8 |
| القراءة في كتاب الرد الشرعي المعقول | 12 |
| التعليقات المختصرة على أبواب كتاب التوحيد | 13 |
| التعليقات البهية على الرسائل العقدية | 9 |
| تنبيه الأنام | 18 |
| تيسير العلي القدير (رحمه الله) | 14 |
| مختصر السيرة النبوية | 16 |
| معارج القبول شرح منظومة سلم الوصول | 28 |
| محاضرات الحج | 13 |
| شرح كتاب الفقه الميسر ١٤٤٠-١٤٤٤ | 115 |
| شرح كتاب الفقه الميسر - أرشيف رمضان | 6 |
| صحيح البخاري | 10 (check for new) |
| فتاوى أركان الإسلام | 151 |
| محاضرات متفرقة | 156 |
| فتح الرب الغفور | 4 |
| كتاب آداب المشي إلى الصلاة - أرشيف رمضان | 6 |
| دروس عشر ذي الحجة | 10 |
| صفة الحج | 2 |
| غنية السائل | 1 |

---

## ITEMS NEEDING MANUAL IDENTIFICATION / REVIEW

These files exist in upload files but could not be auto-identified:

### From الملخص الفقهي:
- msg 5697 — درس (رقم غير محدد)
- msg 6570 — درس (رقم غير محدد)
- msg 6689 — درس (رقم غير محدد)
- msg 6769 — درس (رقم غير محدد)
- msg 6784 — درس (رقم غير محدد)
- msg 6883 vs 6949 — DUPLICATE for lesson 43 (resolve which is real)
- lesson 47 — possibly missing from channel entirely

### From إتمام المنة:
- msg 6380 — lesson 3 re-post (same as msg 2748 — verify if different recording)
- msg 6651 — lesson 4 DUPLICATE (same msg referenced twice)
- msg 6817 — درس (رقم غير محدد)

### From التحفة النجمية:
- msg 6062 vs 7423 — DUPLICATE for lesson 1 (two versions — pick one or both)
- msg 766, 817, 870, 1024 — listed as lessons 1, 11, 21, 31 (same as existing DB entries — may be reformatted versions)

### New series (needs CMS series creation first):
- **تأسيس الأحكام — كتاب القصاص والحدود (عن بعد):** 5 lessons (msg 6375, 6423, 6707, 7139, 7268)
  - Lesson 1: AUDIO-2026-02-11 — كتاب القصاص
  - Lesson 2: AUDIO-2026-02-11 — كتاب القصاص (تابع)
  - Lesson 3: AUDIO-2026-04-08 — كتاب القصاص
  - Lesson 4: AUDIO-2026-05-06 — كتاب القصاص
  - Lesson 5: AUDIO-2026-05-14 — كتاب الحدود

---

## SUMMARY — RECOMMENDED UPLOAD ORDER

| Priority | Series | Lessons to Upload | Effort |
|----------|--------|-------------------|--------|
| 1 | التحفة النجمية | ~35 clean lessons (8–42) | High — many entries but sequential |
| 2 | دروس رمضان - وزارة الشؤون الإسلامية | 30 | Medium — all identified |
| 3 | تأسيس الأحكام (mosque) | ~5 remaining | Low |
| 4 | الملخص الفقهي | ~9 remaining (some unidentified) | Medium |
| 5 | إتمام المنة | ~4 remaining | Low |
| 6 | New series: القصاص والحدود عن بعد | 5 | Low (needs series creation) |
| 7 | Check active series for post-May lessons | TBD | High — needs HTML parse |
| 8 | خطب الجمعة (new weekly khutbas) | ~18–20 | High — needs parse |

---

*Report generated from PDF taqreer dated 2026-07-13. All upload files in `output/corrected/`. Detailed lesson lists are in the respective `*_upload_data.txt` / `*_upload.txt` files.*
