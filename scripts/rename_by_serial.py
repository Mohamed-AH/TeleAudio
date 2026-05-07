"""
rename_by_serial.py
───────────────────
Reads full_archive.xlsx, looks up each audio file in the files/ folder,
and renames it to its S.No value while preserving the original extension.

Usage
─────
    python rename_by_serial.py                   # live run
    python rename_by_serial.py --dry-run         # preview only, no changes
    python rename_by_serial.py --xlsx "D:\\Copytele\\full_archive.xlsx"
    python rename_by_serial.py --files-dir "D:\\Copytele\\files"

Defaults
────────
    --xlsx      D:\\Copytele\\full_archive.xlsx
    --files-dir D:\\Copytele\\files
"""

import argparse
import sys
import unicodedata
from collections import Counter
from pathlib import Path

import openpyxl


# ── helpers ────────────────────────────────────────────────────────────────────

def load_rows(xlsx_path: Path) -> list[dict]:
    """
    Read the spreadsheet and return a list of dicts with keys:
        sno (str), telegram_filename (str), row_num (int)
    Raises ValueError if the expected column headers are missing.
    """
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    rows_iter = ws.iter_rows(values_only=True)
    header = next(rows_iter, None)
    if header is None:
        raise ValueError("Spreadsheet is empty.")

    # Locate columns by name (case-insensitive, whitespace-tolerant)
    header_clean = [str(h).strip().lower() if h is not None else "" for h in header]
    try:
        sno_idx = header_clean.index("s.no")
    except ValueError:
        raise ValueError(f"Column 'S.No' not found. Headers found: {list(header)}")
    try:
        fname_idx = header_clean.index("telegramfilename")
    except ValueError:
        raise ValueError(f"Column 'TelegramFileName' not found. Headers found: {list(header)}")

    records = []
    for row_num, row in enumerate(rows_iter, start=2):  # row 1 = header
        sno = row[sno_idx]
        fname = row[fname_idx]
        if sno is None and fname is None:
            continue  # blank row
        records.append({
            "sno": str(sno).strip() if sno is not None else "",
            # Normalise so NBSP / composed chars from Excel match real filesystem names
            "telegram_filename": normalise_filename(str(fname)) if fname is not None else "",
            "row_num": row_num,
        })

    wb.close()
    return records


def normalise_filename(name: str) -> str:
    """
    Normalise a filename for reliable matching:
    - NFC Unicode normalisation (handles composed/decomposed Arabic)
    - Collapse all Unicode space variants (NBSP \xa0, etc.) to regular space
    - Strip leading/trailing whitespace
    """
    name = unicodedata.normalize("NFC", name)
    # Replace any Unicode space (NBSP, thin space, zero-width space…) with plain space
    name = "".join(" " if unicodedata.category(c) in ("Zs", "Cc") and c != "\n" else c for c in name)
    return name.strip()


def build_target_name(sno: str, source_path: Path) -> str:
    """Return the new filename: '<sno><original_extension>'."""
    return sno + source_path.suffix  # suffix includes the dot, e.g. '.m4a'


# ── main logic ─────────────────────────────────────────────────────────────────

def run(xlsx_path: Path, files_dir: Path, dry_run: bool) -> int:
    mode_label = "[DRY RUN] " if dry_run else ""
    print(f"{mode_label}Spreadsheet : {xlsx_path}")
    print(f"{mode_label}Files folder: {files_dir}")
    print()

    # ── load spreadsheet ───────────────────────────────────────────────────────
    try:
        records = load_rows(xlsx_path)
    except Exception as exc:
        print(f"ERROR reading spreadsheet: {exc}")
        return 1

    print(f"Rows loaded: {len(records)}")

    # ── build normalised lookup of files on disk ───────────────────────────────
    # Maps normalised_name → actual Path, so we match despite encoding differences
    disk_files: dict[str, Path] = {}
    for p in files_dir.iterdir():
        if p.is_file():
            norm_key = normalise_filename(p.name)
            disk_files[norm_key] = p
    print(f"Files on disk: {len(disk_files)}\n")

    # ── detect duplicate S.No values in the spreadsheet ───────────────────────
    sno_counter = Counter(r["sno"] for r in records)
    duplicate_snos = {k for k, v in sno_counter.items() if v > 1}
    if duplicate_snos:
        print(f"WARNING: {len(duplicate_snos)} duplicate S.No value(s) found in spreadsheet:")
        for s in sorted(duplicate_snos):
            print(f"  S.No={s!r} appears {sno_counter[s]} times")
        print()

    # ── detect duplicate TelegramFileName values ───────────────────────────────
    fname_counter = Counter(r["telegram_filename"] for r in records)
    duplicate_fnames = {k for k, v in fname_counter.items() if v > 1}
    if duplicate_fnames:
        print(f"WARNING: {len(duplicate_fnames)} duplicate TelegramFileName value(s):")
        for f in sorted(duplicate_fnames):
            print(f"  {f!r} appears {fname_counter[f]} times")
        print()

    # ── process each row ───────────────────────────────────────────────────────
    renamed_ok   = []   # (sno, old_name, new_name)
    already_done = []   # target already exists with correct name
    missing      = []   # source file not found
    skipped_dup  = []   # skipped because of duplicate S.No (would overwrite)
    failed       = []   # rename raised an exception

    # Track target names we've already assigned in this run (duplicate S.No guard)
    assigned_targets: dict[str, str] = {}  # target_path_str → sno

    for rec in records:
        sno            = rec["sno"]
        telegram_fname = rec["telegram_filename"]
        row_num        = rec["row_num"]

        if not sno:
            print(f"  Row {row_num}: SKIP — empty S.No")
            continue
        if not telegram_fname:
            print(f"  Row {row_num}: SKIP — empty TelegramFileName")
            continue

        # Resolve source via normalised lookup (handles NBSP, NFC/NFD differences)
        norm_key = normalise_filename(telegram_fname)
        source_path = disk_files.get(norm_key)

        if source_path is None:
            missing.append((row_num, sno, telegram_fname))
            print(f"  Row {row_num}: MISSING  {telegram_fname!r}")
            continue

        new_name    = build_target_name(sno, source_path)
        target_path = source_path.parent / new_name

        # Already correctly named?
        if source_path.name == new_name:
            already_done.append((sno, telegram_fname))
            print(f"  Row {row_num}: OK       {telegram_fname!r} already named correctly")
            continue

        # Duplicate S.No guard — would two different rows claim the same target?
        target_key = str(target_path)
        if target_key in assigned_targets:
            skipped_dup.append((row_num, sno, telegram_fname, assigned_targets[target_key]))
            print(
                f"  Row {row_num}: SKIP-DUP S.No={sno!r} target {new_name!r} already claimed "
                f"by another row (S.No={assigned_targets[target_key]!r})"
            )
            continue

        # Target already exists (different source file trying to claim this name)
        if target_path.exists() and target_path != source_path:
            skipped_dup.append((row_num, sno, telegram_fname, f"file {new_name!r} exists"))
            print(
                f"  Row {row_num}: CONFLICT {new_name!r} already exists in folder "
                f"(source: {telegram_fname!r})"
            )
            continue

        assigned_targets[target_key] = sno

        if dry_run:
            print(f"  Row {row_num}: WOULD RENAME  {telegram_fname!r}  →  {new_name!r}")
            renamed_ok.append((sno, telegram_fname, new_name))
        else:
            try:
                source_path.rename(target_path)
                print(f"  Row {row_num}: RENAMED  {telegram_fname!r}  →  {new_name!r}")
                renamed_ok.append((sno, telegram_fname, new_name))
            except Exception as exc:
                failed.append((row_num, sno, telegram_fname, str(exc)))
                print(f"  Row {row_num}: FAILED   {telegram_fname!r} — {exc}")

    # ── summary ────────────────────────────────────────────────────────────────
    verb = "Would rename" if dry_run else "Renamed"
    print()
    print("=" * 60)
    print(f"{'DRY-RUN ' if dry_run else ''}SUMMARY")
    print("=" * 60)
    print(f"  Total rows processed : {len(records)}")
    print(f"  {verb:13s}       : {len(renamed_ok)}")
    print(f"  Already correct      : {len(already_done)}")
    print(f"  Missing files        : {len(missing)}")
    print(f"  Duplicates/conflicts : {len(skipped_dup)}")
    print(f"  Rename failures      : {len(failed)}")
    print("=" * 60)

    if missing:
        print(f"\nMISSING FILES ({len(missing)}):")
        for row_num, sno, fname in missing:
            print(f"  Row {row_num}  S.No={sno}  {fname!r}")

    if skipped_dup:
        print(f"\nDUPLICATES / CONFLICTS ({len(skipped_dup)}):")
        for item in skipped_dup:
            row_num, sno, fname = item[0], item[1], item[2]
            reason = item[3]
            print(f"  Row {row_num}  S.No={sno}  {fname!r}  reason: {reason}")

    if failed:
        print(f"\nFAILED RENAMES ({len(failed)}):")
        for row_num, sno, fname, err in failed:
            print(f"  Row {row_num}  S.No={sno}  {fname!r}  error: {err}")

    return 0 if not failed else 1


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Rename audio files to their S.No from full_archive.xlsx"
    )
    parser.add_argument(
        "--xlsx",
        default=r"D:\Copytele\full_archive.xlsx",
        help="Path to the Excel file (default: D:\\Copytele\\full_archive.xlsx)",
    )
    parser.add_argument(
        "--files-dir",
        default=r"D:\Copytele\files",
        help="Folder containing the audio files (default: D:\\Copytele\\files)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would happen without making any changes",
    )
    args = parser.parse_args()

    xlsx_path = Path(args.xlsx)
    files_dir = Path(args.files_dir)

    if not xlsx_path.exists():
        print(f"ERROR: Spreadsheet not found: {xlsx_path}")
        sys.exit(1)
    if not files_dir.is_dir():
        print(f"ERROR: Files folder not found: {files_dir}")
        sys.exit(1)

    sys.exit(run(xlsx_path, files_dir, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
