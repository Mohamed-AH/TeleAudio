#!/usr/bin/env python3
"""Generate corrected master analysis comparing MongoDB DB state to HTML index files."""

import re
import os
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"
CORRECTED_DIR = OUTPUT_DIR / "corrected"
DB_FILE = ROOT / "data-export.txt"

# ── DB → index file mapping ──────────────────────────────────────────────────
# Each entry: DB series title → (index_files, note)
# note = None if straightforward, otherwise explanation of any caveat
DB_TO_INDEX = {
    # ── Active / main series ──────────────────────────────────────────────────
    "خطب الجمعة": (
        ["khutba_index.txt"],
        # NOTE: khutba_index.txt also contains the seerah-themed khutbahs that are
        # catalogued separately in DB as "خطبة الجمعة - مختصر السيرة النبوية".
        # The total DB count across BOTH khutba series = 15 + 16 = 31.
        "NOTE: 9 additional seerah-khutbahs in DB as 'خطبة الجمعة - مختصر السيرة النبوية' "
        "are ALSO captured in this same index file. "
        "True combined DB count = 24 (15 general + 9 seerah). "
        "Actual gap to upload = 384 - 24 = 360 when treating both khutba series as one pool."
    ),
    "خطبة الجمعة - مختصر السيرة النبوية": (
        [],
        "NOTE: These 16 seerah-khutbahs are captured in khutba_index.txt but cannot be "
        "separated out automatically (khutba_seera match finds 0 posts). "
        "They are already counted under 'خطب الجمعة' above."
    ),
    "الملخص شرح كتاب التوحيد": (["mulakhkhas_tawheed_index.txt"], None),
    "الملخص الفقهي": (["mulakhkhas_fiqhi_index.txt"], None),
    "تأسيس الأحكام شرح عمدة الأحكام": (["ta2sees_index.txt"], None),
    "تأسيس الأحكام شرح عمدة الأحكام - الطهارة": (["ta2sees_tahara_index.txt"], None),
    "تأسيس الأحكام شرح عمدة الأحكام - عن بعد": (
        [],
        "NOTE: Series NOT found in any Telegram HTML export (0 matches). "
        "These lectures may have been uploaded from a non-Telegram source."
    ),
    "تأسيس الأحكام شرح عمدة الأحكام - أرشيف رمضان": (["ta2sees_archive_index.txt"], None),
    "الأفنان الندية - عن بعد": (
        [],
        "NOTE: Series NOT found in any Telegram HTML export (0 matches across all 6 HTML files). "
        "All 33 DB lectures appear to have been uploaded from a non-Telegram source."
    ),
    "الأفنان الندية - أرشيف رمضان": (["afnan_archive_index.txt"], None),
    "إرشاد الساري شرح السنة للبربهاري": (["irshad_sari_index.txt"], None),
    "المورد العذب الزلال": (["mawrid_index.txt"], None),
    "معارج القبول شرح منظومة سلم الوصول - عن بعد": (["ma3arij_index.txt"], None),
    "التحفة النجمية بشرح الأربعين النووية": (["tuhfa_najmiyya_index.txt"], None),
    "مختصر السيرة النبوية": (["mukhtasar_seera_index.txt"], None),
    "تنبيه الانام على ما في كتاب سبل السلام من الفوائد والأحكام": (
        ["tanbeeh_anam_index.txt"],
        "NOTE: tanbeeh_anam_index.txt also contains the Ramadan archive lessons "
        "('تنبيه الأنام - أرشيف رمضان', 12 lessons in DB). "
        "Combined DB total = 6 (main) + 12 (archive) = 18. HTML total = 26. Gap = 8."
    ),
    "تنبيه الأنام على ما في كتاب سبل السلام من الفوائد والأحكام - أرشيف رمضان": (
        [],
        "NOTE: Archive entries are in tanbeeh_anam_index.txt (same as main series above). "
        "Upload gap already accounted for under main tanbeeh entry."
    ),
    "تيسير العلي القدير لاختصار تفسير ابن كثير": (["tayseer_ali_index.txt"], None),
    "صحيح البخاري": (["bukhari_index.txt"], None),
    "محاضرات متفرقة": (["muhadarat_index.txt"], None),
    "إتمام المنة بشرح أصول السنة": (["itmam_minna_index.txt"], None),
    "الشرح الموجز الممهد لتوحيد الخالق الممجد الذي ألفه شيخ الإسلام محمد": (["sharh_mujaz_index.txt"], None),
    "التعليقات البهية على الرسائل العقدية": (["ta3leeqat_bahiyya_index.txt"], None),
    "التفسير الميسر": (["tafseer_muyassar_index.txt"], None),
    "دروس رمضان - وزارة الشؤون الإسلامية": (["durus_wazara_index.txt"], None),
    "التعليق على كتاب مجالس شهر رمضان": (
        ["majaalis_ramadan_index.txt"],
        "NOTE: Series exists in DB but has 0 lectures exported. All 66 HTML entries need upload."
    ),
    # Archive series
    "الملخص الفقهي - أرشيف رمضان": (["mulakhkhas_fiqhi_archive_index.txt"], None),
    "شرح كتاب الفقه الميسر - أرشيف رمضان": (["fiqh_archive_index.txt"], None),
    "كتاب آداب المشي إلى الصلاة- كتاب_الصيام - أرشيف رمضان": (["adab_mashi_index.txt"], None),
    "الممتع شرح زاد المستقنع - أرشيف رمضان": (
        [],
        "NOTE: No index file built for this series yet. "
        "Would need a new SeriesConfig for 'الممتع شرح زاد المستقنع'."
    ),
}

# HTML-only series (not in DB at all)
HTML_ONLY = [
    ("فتاوى أركان الإسلام", "fatawa_arkan_index.txt"),
    ("شرح الفقه الميسر", "fiqh_muyassar_index.txt"),
    ("الأعلام السنية", "a3lam_sunna_index.txt"),
    ("القول السديد", "qawl_sadeed_index.txt"),
    ("التعليقات المختصرة", "ta3leeqat_mukhtasara_index.txt"),
    ("فضل المبين في التعريف بشيخ الإسلام المبين", "fadl_mubeen_index.txt"),
    ("الرد الشرعي", "radd_shar3i_index.txt"),
    ("الأصول الثلاثة", "usul_thalatha_index.txt"),
    ("الأصول الستة", "usul_sitta_index.txt"),
    ("التعليق على كتاب التوحيد (المتن)", "ta3leeq_tawheed_index.txt"),
    ("التعليقات الأثرية على الرسائل العقدية", "ta3leeqat_athariyya_index.txt"),
    ("صحيح مسلم", "sahih_muslim_index.txt"),
    ("الإبهاج بشرح صحيح مسلم", "ibhaj_index.txt"),
    ("دروس عشر ذي الحجة", "ashr_dhilhijja_index.txt"),
    ("التحقيق والإيضاح", "tahqeeq_idah_index.txt"),
    ("فتح الرب الغفور", "fath_rabb_index.txt"),
]


def parse_db_series(db_file: Path) -> dict:
    """Parse data-export.txt → {series_title: {count, first_date, last_date, sort_orders}}"""
    series_data = {}
    current_series = None

    with open(db_file, encoding="utf-8") as f:
        lines = f.readlines()

    in_lectures_section = False
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if "LECTURES (grouped by series)" in line:
            in_lectures_section = True
            i += 1
            continue

        if not in_lectures_section:
            i += 1
            continue

        # New series block
        if line.startswith("SERIES:"):
            current_series = line[len("SERIES:"):].strip()
            if current_series not in series_data:
                series_data[current_series] = {
                    "count": 0,
                    "first_hijri": None,
                    "last_hijri": None,
                    "first_sort": None,
                    "last_sort": None,
                    "lectures": [],
                }
            i += 1
            continue

        if current_series is None:
            i += 1
            continue

        # Lecture fields
        if line.startswith("Date Recorded (Hijri):"):
            hijri = line.split(":", 1)[1].strip()
            sd = series_data[current_series]
            if sd["first_hijri"] is None:
                sd["first_hijri"] = hijri
            sd["last_hijri"] = hijri

        elif line.startswith("Sort Order:"):
            try:
                so = int(line.split(":", 1)[1].strip())
                sd = series_data[current_series]
                if sd["first_sort"] is None:
                    sd["first_sort"] = so
                sd["last_sort"] = so
            except ValueError:
                pass

        elif line.startswith("Lecture #"):
            series_data[current_series]["count"] += 1

        elif line.startswith("Audio:"):
            audio = line.split(":", 1)[1].strip()
            series_data[current_series]["lectures"].append(audio)

        i += 1

    return series_data


def parse_index_file(index_file: Path) -> list:
    """Parse an index file → list of entry lines (lines containing 📎)."""
    if not index_file.exists():
        return []
    entries = []
    with open(index_file, encoding="utf-8") as f:
        for line in f:
            if "📎" in line:
                entries.append(line.rstrip())
    return entries


def count_entries(entries: list) -> tuple[int, int]:
    """Count old vs new entries. Split on '٢٠٢٥' in section headers."""
    # We read the raw file to detect the section boundary
    return len(entries), 0  # caller handles split if needed


def parse_index_file_split(index_file: Path) -> tuple[list, list]:
    """Return (old_entries, new_entries) split by the 2025+ section header."""
    if not index_file.exists():
        return [], []
    old_entries = []
    new_entries = []
    in_new = False
    with open(index_file, encoding="utf-8") as f:
        for line in f:
            stripped = line.rstrip()
            if "٢٠٢٥" in stripped and "━" in stripped:
                in_new = True
            if "📎" in stripped:
                if in_new:
                    new_entries.append(stripped)
                else:
                    old_entries.append(stripped)
    return old_entries, new_entries


def extract_msg_id(entry: str) -> int:
    """Extract message ID from a Telegram URL in an entry line."""
    m = re.search(r"/(\d+)\]?$", entry)
    if m:
        return int(m.group(1))
    return 0


# When multiple DB series share one index file, override db_count to combined total
# Key = DB series title of the "main" entry (the one with the index file mapped)
COMBINED_DB_COUNT_OVERRIDE = {
    # tanbeeh main + archive both use tanbeeh_anam_index.txt
    "تنبيه الانام على ما في كتاب سبل السلام من الفوائد والأحكام": 18,  # 6 main + 12 archive
}


def format_summary_line(series_name: str, db_info: dict | None, all_entries: list,
                         old_entries: list, new_entries: list) -> str:
    """Format a single-line summary."""
    total_html = len(all_entries)

    if db_info is None or db_info["count"] == 0:
        return (f"{series_name}: Not in DB. "
                f"HTML has {total_html} entries "
                f"({len(old_entries)} old + {len(new_entries)} new). "
                f"All {total_html} need to be uploaded.")

    db_count = db_info["count"]
    first_h = db_info.get("first_hijri") or "?"
    last_h = db_info.get("last_hijri") or "?"
    gap = total_html - db_count

    if gap <= 0:
        return (f"{series_name}: DB has {db_count} lessons ({first_h} → {last_h}). "
                f"HTML has {total_html} entries. DB appears up to date.")
    else:
        return (f"{series_name}: DB has {db_count} lessons ({first_h} → {last_h}). "
                f"HTML has {total_html} entries total ({len(old_entries)} old + {len(new_entries)} new). "
                f"{gap} lessons need to be uploaded.")


def main():
    CORRECTED_DIR.mkdir(parents=True, exist_ok=True)

    print("Parsing DB export...")
    db_series = parse_db_series(DB_FILE)
    print(f"  Found {len(db_series)} series in DB with lectures")

    # Build the full report
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("CORRECTED MASTER ANALYSIS — TeleAudio vs MongoDB")
    report_lines.append("Generated: 2026-06-01")
    report_lines.append("=" * 80)
    report_lines.append("")
    report_lines.append("SUMMARY TABLE")
    report_lines.append("─" * 80)
    report_lines.append("")

    summaries = []
    detail_blocks = []

    # ── Process DB series ────────────────────────────────────────────────────
    processed_index_files = set()

    for db_title, (index_files, mapping_note) in DB_TO_INDEX.items():
        db_info = db_series.get(db_title)

        # Aggregate entries across all mapped index files
        all_entries = []
        old_entries = []
        new_entries = []
        for fname in index_files:
            fpath = OUTPUT_DIR / fname
            o, n = parse_index_file_split(fpath)
            old_entries.extend(o)
            new_entries.extend(n)
            all_entries.extend(o + n)
            processed_index_files.add(fname)

        # Deduplicate by URL within this series
        seen_urls = set()
        dedup_entries = []
        for e in all_entries:
            m = re.search(r"https?://[^\]]+", e)
            url = m.group(0) if m else e
            if url not in seen_urls:
                seen_urls.add(url)
                dedup_entries.append(e)
        all_entries = dedup_entries

        # Same dedup for old/new split (rebuild from dedup_entries order)
        dedup_set = {re.search(r"https?://[^\]]+", e).group(0) if re.search(r"https?://[^\]]+", e) else e
                     for e in all_entries}
        old_entries = [e for e in old_entries
                       if (re.search(r"https?://[^\]]+", e).group(0)
                           if re.search(r"https?://[^\]]+", e) else e) in dedup_set]
        new_entries = [e for e in new_entries
                       if (re.search(r"https?://[^\]]+", e).group(0)
                           if re.search(r"https?://[^\]]+", e) else e) in dedup_set]

        # Apply combined count override if this series shares an index with another
        raw_db_count = db_info["count"] if db_info else 0
        effective_db_count = COMBINED_DB_COUNT_OVERRIDE.get(db_title, raw_db_count)

        summary = format_summary_line(db_title, db_info, all_entries, old_entries, new_entries)
        if effective_db_count != raw_db_count:
            # Rebuild summary with the combined count for more accurate gap
            gap_adj = len(all_entries) - effective_db_count
            first_h = db_info.get("first_hijri", "?") if db_info else "?"
            last_h = db_info.get("last_hijri", "?") if db_info else "?"
            if gap_adj > 0:
                summary = (f"{db_title}: DB has {raw_db_count} lessons ({first_h} → {last_h}); "
                           f"combined with paired archive series = {effective_db_count} total in DB. "
                           f"HTML has {len(all_entries)} entries. {gap_adj} need to be uploaded.")
            else:
                summary = (f"{db_title}: DB (combined with archive) has {effective_db_count} lessons. "
                           f"HTML has {len(all_entries)} entries. DB appears up to date.")
        summaries.append(summary)

        # Detail block
        block = []
        block.append("")
        block.append("─" * 80)
        block.append(f"SERIES: {db_title}")
        if db_info and db_info["count"] > 0:
            block.append(f"  DB: {raw_db_count} lessons  |  "
                         f"Dates: {db_info.get('first_hijri','?')} → {db_info.get('last_hijri','?')}")
            if effective_db_count != raw_db_count:
                block.append(f"  DB (combined with shared-index archive): {effective_db_count} lessons total")
        else:
            block.append("  DB: 0 lessons (series exists but empty or not found in LECTURES section)")
        block.append(f"  HTML: {len(all_entries)} total entries  "
                     f"({len(old_entries)} old / {len(new_entries)} new)")
        block.append(f"  Index files: {', '.join(index_files) if index_files else 'none mapped'}")
        if mapping_note:
            block.append(f"  ⚠ {mapping_note}")
        block.append("")

        db_count = effective_db_count
        gap = len(all_entries) - db_count

        if not index_files:
            # No HTML source — series is DB-only or not separable
            if db_count > 0:
                block.append(f"  ✓ DB has {db_count} lessons. No HTML index — see note above.")
            else:
                block.append("  ⚠ No index file and no DB entries.")
        elif gap > 0:
            block.append(f"  ENTRIES TO UPLOAD ({gap} total):")
            entries_to_upload = (old_entries[db_count:] + new_entries
                                 if db_count <= len(old_entries)
                                 else new_entries[db_count - len(old_entries):])
            block.append(f"  (First {db_count} entries assumed already in DB; following still need upload:)")
            block.append("")
            for idx, entry in enumerate(entries_to_upload, 1):
                block.append(f"    {idx:3}. {entry.strip()}")
        elif gap == 0:
            block.append("  ✓ All entries appear to be in DB already.")
        else:
            block.append(f"  ⚠ DB has more records ({db_count}) than HTML entries ({len(all_entries)}) — "
                         "check for missing index entries.")

        detail_blocks.append("\n".join(block))

    # ── Process HTML-only series ─────────────────────────────────────────────
    report_lines.append("── DB SERIES ──────────────────────────────────────────────────────────────────")
    report_lines.append("")
    for s in summaries:
        report_lines.append(f"  • {s}")
    report_lines.append("")
    report_lines.append("── HTML-ONLY SERIES (not yet in DB) ──────────────────────────────────────────")
    report_lines.append("")

    html_only_summaries = []
    html_only_details = []

    for series_name, fname in HTML_ONLY:
        fpath = OUTPUT_DIR / fname
        old_e, new_e = parse_index_file_split(fpath)
        all_e = old_e + new_e
        processed_index_files.add(fname)

        summary = format_summary_line(series_name, None, all_e, old_e, new_e)
        html_only_summaries.append(summary)

        block = []
        block.append("")
        block.append("─" * 80)
        block.append(f"SERIES: {series_name}")
        block.append("  DB: Not present")
        block.append(f"  HTML: {len(all_e)} total entries ({len(old_e)} old / {len(new_e)} new)")
        block.append(f"  Index file: {fname}")
        block.append("")
        if all_e:
            block.append(f"  ALL ENTRIES NEED UPLOAD ({len(all_e)} total):")
            block.append("")
            for i, entry in enumerate(all_e, 1):
                block.append(f"    {i:3}. {entry.strip()}")
        html_only_details.append("\n".join(block))

    for s in html_only_summaries:
        report_lines.append(f"  • {s}")

    report_lines.append("")
    report_lines.append("─" * 80)

    # Check for unprocessed index files
    all_index_files = {f.name for f in OUTPUT_DIR.glob("*_index.txt")}
    unprocessed = all_index_files - processed_index_files
    if unprocessed:
        report_lines.append("")
        report_lines.append("── UNPROCESSED INDEX FILES (not mapped above) ─────────────────────────────────")
        for fname in sorted(unprocessed):
            old_e, new_e = parse_index_file_split(OUTPUT_DIR / fname)
            total = len(old_e) + len(new_e)
            report_lines.append(f"  • {fname}: {total} entries ({len(old_e)} old / {len(new_e)} new)")

    report_lines.append("")
    report_lines.append("=" * 80)
    report_lines.append("DETAILED BREAKDOWN — DB SERIES")
    report_lines.append("=" * 80)
    for block in detail_blocks:
        report_lines.append(block)

    report_lines.append("")
    report_lines.append("=" * 80)
    report_lines.append("DETAILED BREAKDOWN — HTML-ONLY SERIES")
    report_lines.append("=" * 80)
    for block in html_only_details:
        report_lines.append(block)

    # Write master analysis
    master_path = CORRECTED_DIR / "master_analysis.txt"
    with open(master_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    print(f"Written: {master_path}")

    # Write per-series upload files for DB series with gaps
    print("\nWriting per-series upload files...")
    for db_title, (index_files, _mapping_note) in DB_TO_INDEX.items():
        if not index_files:
            continue  # no HTML source; skip upload file
        db_info = db_series.get(db_title)
        raw_count = db_info["count"] if db_info else 0
        db_count = COMBINED_DB_COUNT_OVERRIDE.get(db_title, raw_count)

        old_entries = []
        new_entries = []
        for fname in index_files:
            fpath = OUTPUT_DIR / fname
            o, n = parse_index_file_split(fpath)
            old_entries.extend(o)
            new_entries.extend(n)

        entries_to_upload = (old_entries[db_count:] + new_entries
                             if db_count <= len(old_entries)
                             else new_entries[db_count - len(old_entries):])

        if not entries_to_upload:
            continue

        safe_name = re.sub(r'[^\w؀-ۿ]+', '_', db_title).strip('_')
        out_path = CORRECTED_DIR / f"{safe_name}_upload.txt"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"📤 UPLOAD LIST: {db_title}\n")
            f.write(f"DB already has: {db_count} lessons\n")
            f.write(f"Entries to upload: {len(entries_to_upload)}\n")
            f.write("─" * 60 + "\n\n")
            for i, entry in enumerate(entries_to_upload, db_count + 1):
                f.write(f"* {entry.strip()}\n")
        print(f"  {out_path.name}: {len(entries_to_upload)} entries")

    # Write per-series upload files for HTML-only series
    for series_name, fname in HTML_ONLY:
        fpath = OUTPUT_DIR / fname
        old_e, new_e = parse_index_file_split(fpath)
        all_e = old_e + new_e
        if not all_e:
            continue
        safe_name = re.sub(r'[^\w؀-ۿ]+', '_', series_name).strip('_')
        out_path = CORRECTED_DIR / f"{safe_name}_upload.txt"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"📤 UPLOAD LIST: {series_name}\n")
            f.write(f"DB: Not present\n")
            f.write(f"Entries to upload: {len(all_e)}\n")
            f.write("─" * 60 + "\n\n")
            for i, entry in enumerate(all_e, 1):
                f.write(f"* {entry.strip()}\n")
        print(f"  {out_path.name}: {len(all_e)} entries")

    print("\nDone. Output written to:", CORRECTED_DIR)


if __name__ == "__main__":
    main()
