"""
download_audio_links.py
───────────────────────
Downloads audio files from non-YouTube links in output/links_archive.xlsx.
YouTube links are skipped (handle manually).

For every non-YouTube row the script:
  1. Sends a HEAD request to check Content-Type before downloading.
  2. If the server returns audio/* or a known audio extension, downloads the file.
  3. If the server returns something else (HTML, image, etc.) marks it "not_audio".
  4. Handles dead links, timeouts, SSL errors, and redirects gracefully.
  5. Skips rows whose file already exists in the download folder (resume-safe).

Usage
─────
    python download_audio_links.py                        # live run
    python download_audio_links.py --dry-run              # preview only
    python download_audio_links.py --xlsx "D:\\Copytele\\output\\links_archive.xlsx"
    python download_audio_links.py --out-dir "D:\\Copytele\\downloads"
    python download_audio_links.py --all-links            # include Other-type links too

Defaults
────────
    --xlsx      output/links_archive.xlsx   (relative to script location)
    --out-dir   downloads/                  (relative to script location)
    --timeout   30 (connect), 120 (read)
    --retries   3
    --workers   4   (parallel downloads)

Report
──────
Status values written to the JSON report (downloads/download_report.json):
  downloaded    – file saved successfully
  already_done  – file existed, skipped
  not_audio     – server returned non-audio content (Content-Type shown)
  broken        – HTTP error or DNS/connection error
  blocked_403   – server returned 403 (IP restriction; will work from your machine)
  timeout       – request timed out after retries
  skipped_yt    – YouTube link (always skipped)
  skipped_other – Other-type link skipped (without --all-links flag)
  dry_run       – dry-run mode, no download attempted
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
import openpyxl
from tqdm import tqdm

# ── constants ────────────────────────────────────────────────────────────────
AUDIO_MIME_PREFIXES = ("audio/",)
AUDIO_MIME_EXACT    = {
    "application/octet-stream",   # generic binary — rely on extension
    "binary/octet-stream",
    "video/mp4",                  # m4a is often served as video/mp4
}
AUDIO_EXTENSIONS = {".m4a", ".mp3", ".aac", ".ogg", ".opus", ".amr", ".flac", ".wav", ".3gp"}

YOUTUBE_RE   = re.compile(r"youtu\.be|youtube\.com", re.IGNORECASE)
SKIP_DOMAINS = {
    # Definitely not audio
    "maps.app.goo.gl", "maps.google.com",
    "chat.whatsapp.com", "wa.me",
    "twitter.com", "x.com",
    "t.me", "telegram.me",
    "instagram.com",
    "facebook.com",
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; TeleAudio-Downloader/1.0)"
})


# ── helpers ───────────────────────────────────────────────────────────────────

def is_youtube(url: str) -> bool:
    return bool(YOUTUBE_RE.search(url))


def domain_of(url: str) -> str:
    return urlparse(url).netloc.lower()


def should_skip_domain(url: str) -> bool:
    dom = domain_of(url)
    return any(dom == s or dom.endswith("." + s) for s in SKIP_DOMAINS)


def ext_from_url(url: str) -> str:
    path = urlparse(url).path
    ext = Path(unquote(path)).suffix.lower()
    return ext if ext in AUDIO_EXTENSIONS else ""


def ext_from_content_type(ct: str) -> str:
    ct = ct.lower().split(";")[0].strip()
    mapping = {
        "audio/mpeg":  ".mp3",
        "audio/mp4":   ".m4a",
        "audio/m4a":   ".m4a",
        "audio/aac":   ".aac",
        "audio/ogg":   ".ogg",
        "audio/opus":  ".opus",
        "audio/wav":   ".wav",
        "audio/webm":  ".webm",
        "audio/flac":  ".flac",
        "audio/amr":   ".amr",
        "video/mp4":   ".m4a",  # common for m4a files
    }
    return mapping.get(ct, "")


def ext_from_disposition(disposition: str) -> str:
    if not disposition:
        return ""
    m = re.search(r'filename[^;=\n]*=\s*["\']?([^"\'\n;]+)', disposition, re.IGNORECASE)
    if m:
        return Path(m.group(1).strip()).suffix.lower()
    return ""


def is_audio_content(content_type: str, url: str) -> bool:
    ct = content_type.lower().split(";")[0].strip()
    if any(ct.startswith(p) for p in AUDIO_MIME_PREFIXES):
        return True
    if ct in AUDIO_MIME_EXACT and ext_from_url(url) in AUDIO_EXTENSIONS:
        return True
    return False


def choose_extension(content_type: str, disposition: str, url: str) -> str:
    return (
        ext_from_disposition(disposition)
        or ext_from_content_type(content_type)
        or ext_from_url(url)
        or ".bin"
    )


def target_filename(sno: int | str, ext: str) -> str:
    return f"{sno}{ext}"


def load_excel(path: Path) -> list[dict]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    header = [str(h).strip() if h else "" for h in next(rows_iter)]
    records = []
    for row in rows_iter:
        if all(v is None for v in row):
            continue
        d = {header[i]: row[i] for i in range(len(header))}
        records.append(d)
    wb.close()
    return records


def head_check(url: str, timeout: tuple[int, int]) -> tuple[str, str, str, int]:
    """
    Returns (content_type, content_length, final_url, status_code).
    Uses GET with Range: bytes=0-4095 — more compatible than HEAD,
    which many audio hosts block.  Reads only the first 4 KB then closes.
    """
    try:
        r = SESSION.get(
            url,
            headers={"Range": "bytes=0-4095"},
            timeout=timeout,
            allow_redirects=True,
            stream=True,
        )
        # Consume just enough to confirm connection, then release
        for _ in r.iter_content(256):
            break
        r.close()
        ct = r.headers.get("Content-Type", "")
        cl = r.headers.get("Content-Length", r.headers.get("Content-Range", ""))
        return ct, cl, r.url, r.status_code
    except Exception:
        raise


def download_file(
    url: str,
    dest: Path,
    timeout: tuple[int, int],
    retries: int,
) -> tuple[str, str]:
    """
    Download url → dest. Returns (status, detail).
    status: 'downloaded' | 'not_audio' | 'broken' | 'timeout'
    """
    attempt = 0
    last_error = ""
    while attempt < retries:
        attempt += 1
        wait = 2 ** (attempt - 1)
        try:
            ct, cl, final_url, status_code = head_check(url, timeout)

            # 403 often means the server blocks cloud/bot IPs — report clearly
            if status_code == 403:
                return "blocked_403", "Server returned 403 (IP/allowlist restriction — try from your own machine)"
            if status_code == 401:
                return "broken", "HTTP 401: authentication required"
            if status_code == 404:
                return "broken", "HTTP 404: file not found"
            if status_code >= 400:
                return "broken", f"HTTP {status_code}"

            if not is_audio_content(ct, final_url):
                return "not_audio", f"Content-Type: {ct or '(empty)'}"

            ext = choose_extension(ct, "", final_url)
            # If dest has .bin suffix, fix it now we know the real extension
            if dest.suffix == ".bin" and ext != ".bin":
                dest = dest.with_suffix(ext)

            # Full streaming download
            with SESSION.get(final_url, timeout=timeout,
                             allow_redirects=True, stream=True) as resp:
                resp.raise_for_status()
                dest.parent.mkdir(parents=True, exist_ok=True)
                tmp = dest.with_suffix(dest.suffix + ".part")
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)
                tmp.rename(dest)

            size_kb = dest.stat().st_size // 1024
            return "downloaded", f"{dest.name}  ({size_kb} KB)"

        except requests.exceptions.Timeout:
            last_error = "timeout"
            if attempt < retries:
                time.sleep(wait)
        except requests.exceptions.SSLError as e:
            last_error = f"SSL error: {e}"
            break
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error: {e}"
            if attempt < retries:
                time.sleep(wait)
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code
            if code == 403:
                return "blocked_403", "Server returned 403 (IP/allowlist restriction)"
            last_error = f"HTTP {code}: {e.response.reason}"
            if code < 500:
                break
            if attempt < retries:
                time.sleep(wait)
        except Exception as e:
            last_error = str(e)
            break

    if "timeout" in last_error:
        return "timeout", last_error
    return "broken", last_error


# ── worker ────────────────────────────────────────────────────────────────────

def process_row(
    rec: dict,
    out_dir: Path,
    timeout: tuple[int, int],
    retries: int,
    dry_run: bool,
    all_links: bool,
) -> dict:
    sno  = rec.get("S.No", "?")
    url  = str(rec.get("TelegramFileName") or "").strip()
    ltype = str(rec.get("LinkType") or "").strip()

    result = {"sno": sno, "url": url, "link_type": ltype, "status": "", "detail": ""}

    if not url or not url.startswith("http"):
        result["status"] = "skipped"
        result["detail"] = "empty or non-HTTP URL"
        return result

    if is_youtube(url):
        result["status"] = "skipped_yt"
        result["detail"] = "YouTube — handle manually"
        return result

    if should_skip_domain(url):
        result["status"] = "skipped_other"
        result["detail"] = f"Non-audio domain: {domain_of(url)}"
        return result

    if ltype == "Other" and not all_links:
        result["status"] = "skipped_other"
        result["detail"] = "Other-type link — use --all-links to attempt"
        return result

    # Determine target path (best guess at extension, will be fixed after HEAD)
    ext = ext_from_url(url) or ".bin"
    dest = out_dir / target_filename(sno, ext)

    # Check if any file for this S.No already exists
    existing = list(out_dir.glob(f"{sno}.*"))
    existing = [p for p in existing if not p.name.endswith(".part")]
    if existing:
        result["status"] = "already_done"
        result["detail"] = existing[0].name
        return result

    if dry_run:
        result["status"] = "dry_run"
        result["detail"] = f"Would download → {dest.name}"
        return result

    status, detail = download_file(url, dest, timeout, retries)
    result["status"] = status
    result["detail"] = detail
    return result


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    here = Path(__file__).parent.parent  # repo root

    parser = argparse.ArgumentParser(
        description="Download audio from non-YouTube links in links_archive.xlsx"
    )
    parser.add_argument("--xlsx",     default=str(here / "output" / "links_archive.xlsx"))
    parser.add_argument("--out-dir",  default=str(here / "downloads"))
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--all-links",action="store_true",
                        help="Also attempt Other-type links (not just AudioURL)")
    parser.add_argument("--timeout",  type=int, default=30,
                        help="Connect+read timeout in seconds (default 30)")
    parser.add_argument("--retries",  type=int, default=3)
    parser.add_argument("--workers",  type=int, default=4,
                        help="Parallel download threads (default 4)")
    args = parser.parse_args()

    xlsx_path = Path(args.xlsx)
    out_dir   = Path(args.out_dir)
    timeout   = (args.timeout, args.timeout * 4)   # (connect, read)
    dry_run   = args.dry_run

    if not xlsx_path.exists():
        print(f"ERROR: {xlsx_path} not found.")
        sys.exit(1)

    print(f"{'[DRY RUN] ' if dry_run else ''}Reading {xlsx_path.name} …")
    records = load_excel(xlsx_path)
    print(f"Rows loaded: {len(records)}")
    print(f"Output dir : {out_dir}")
    print(f"Threads    : {args.workers}   Timeout: {args.timeout}s   Retries: {args.retries}")
    print(f"All-links  : {args.all_links}")
    print()

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "download_report.json"

    # Load previous report to honour already_done entries across runs
    previous: dict[str, dict] = {}
    if report_path.exists():
        with open(report_path, encoding="utf-8") as f:
            prev_list = json.load(f).get("results", [])
        previous = {str(r["sno"]): r for r in prev_list if r.get("status") == "downloaded"}

    results: list[dict] = []

    def worker(rec):
        # Mark previously downloaded rows immediately
        if str(rec.get("S.No")) in previous:
            return {**previous[str(rec["S.No"])], "status": "already_done",
                    "detail": previous[str(rec["S.No"])].get("detail", "")}
        return process_row(rec, out_dir, timeout, args.retries, dry_run, args.all_links)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(worker, rec): rec for rec in records}
        with tqdm(total=len(records), unit="row") as pbar:
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                pbar.update(1)
                status = result["status"]
                if status in ("downloaded", "broken", "not_audio", "timeout"):
                    sno = result["sno"]
                    detail = result["detail"][:70]
                    tqdm.write(f"  {status.upper():12s}  #{sno}  {detail}")

    # Sort by sno for deterministic report
    try:
        results.sort(key=lambda r: int(str(r["sno"])))
    except Exception:
        pass

    # Save JSON report
    from collections import Counter
    status_counter = Counter(r["status"] for r in results)
    report = {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "xlsx": str(xlsx_path),
        "out_dir": str(out_dir),
        "dry_run": dry_run,
        "summary": dict(status_counter),
        "results": results,
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # ── print summary ─────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"{'DRY-RUN ' if dry_run else ''}SUMMARY")
    print("=" * 60)
    labels = [
        ("downloaded",    "Downloaded"),
        ("already_done",  "Already done (skipped)"),
        ("not_audio",     "Not audio (HTML/image/etc.)"),
        ("broken",        "Broken / HTTP error"),
        ("blocked_403",   "Blocked 403 (try from your own machine)"),
        ("timeout",       "Timed out"),
        ("skipped_yt",    "Skipped (YouTube)"),
        ("skipped_other", "Skipped (non-audio domain / Other-type)"),
        ("dry_run",       "Would download (dry run)"),
        ("skipped",       "Skipped (other reason)"),
    ]
    for key, label in labels:
        if status_counter.get(key):
            print(f"  {label:<35s}: {status_counter[key]}")
    print("=" * 60)

    if status_counter.get("broken"):
        print(f"\nBROKEN LINKS ({status_counter['broken']}):")
        for r in results:
            if r["status"] == "broken":
                print(f"  #{r['sno']}  {r['url'][:70]}")
                print(f"        {r['detail']}")

    if status_counter.get("timeout"):
        print(f"\nTIMED OUT ({status_counter['timeout']}):")
        for r in results:
            if r["status"] == "timeout":
                print(f"  #{r['sno']}  {r['url'][:70]}")

    if status_counter.get("blocked_403"):
        print(f"\nBLOCKED 403 — run from your own machine ({status_counter['blocked_403']}):")
        for r in results:
            if r["status"] == "blocked_403":
                print(f"  #{r['sno']}  {r['url'][:70]}")

    if status_counter.get("not_audio"):
        print(f"\nNOT AUDIO ({status_counter['not_audio']}):")
        for r in results:
            if r["status"] == "not_audio":
                print(f"  #{r['sno']}  {r['url'][:60]}  →  {r['detail']}")

    print(f"\nFull report → {report_path}")


if __name__ == "__main__":
    main()
