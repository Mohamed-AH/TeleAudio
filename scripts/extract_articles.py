#!/usr/bin/env python3
"""
extract_articles.py — Collect and archive articles from Telegram channel HTML exports.

Three source types:
  1. Asdaa (asdaa-alsaa.com)  — fetched via WordPress REST API (reliable, no rate-limit)
  2. Jeddah Club (jeddah-club.com) — links collected from HTML; text inaccessible via
     this environment (TLS incompatibility with the proxy)
  3. Long-form articles written directly as Telegram posts

Usage:
  python scripts/extract_articles.py

Outputs:
  output/articles/articles_full.json        — 397 unique entries with full text (UTF-8)
  output/articles/articles_duplicates.json  — 4 duplicate records (audit log)
  output/articles/articles_summary.xlsx     — summary table + per-type sheets
"""

import hashlib
import html as html_lib
import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd


# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
HTML_FILES = [f for f in [
    BASE_DIR / "messages.html",
    BASE_DIR / "messages2.html",
    BASE_DIR / "messages3.html",
    BASE_DIR / "messages4.html",
    BASE_DIR / "messages_june1.html",
    BASE_DIR / "messages_new.html",
] if f.exists()]
OUT_DIR = BASE_DIR / "output" / "articles"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────────
ASDAA  = "asdaa-alsaa.com"
JCLUB  = "jeddah-club.com"

# WordPress tag ID for Sheikh Hassan Al-Dugheiri on asdaa-alsaa.com
ASDAA_TAG_ID = 20554

CA_BUNDLE = "/root/.ccr/ca-bundle.crt" if Path("/root/.ccr/ca-bundle.crt").exists() else True
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar,en;q=0.8",
}

# Patterns that indicate audio announcements (exclude from Telegram articles)
AUDIO_ANN_RE = re.compile(
    r'\d+:\d+\s*(?:د\b|دقيقة|دق\b)'
    r'|المدة\s*[:#]'
    r'|مدة الصوتية'
    r'|رابط الاستماع'
    r'|صوتيات'
    r'|(?:top4top\.io|archive\.org|goo\.gl)'
)
TG_MIN_CHARS = 400


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_date_dmy(title_attr: str) -> str:
    """'15.09.2025 16:52:26 UTC+03:00'  →  '15.09.2025'"""
    m = re.match(r'(\d{2}\.\d{2}\.\d{4})', title_attr or "")
    return m.group(1) if m else ""


def ymd_to_dmy(ymd: str) -> str:
    """'2026-06-27'  →  '27.06.2026'"""
    if not ymd:
        return ""
    parts = ymd[:10].split("-")
    return f"{parts[2]}.{parts[1]}.{parts[0]}" if len(parts) == 3 else ymd


def strip_html(html_str: str) -> str:
    soup = BeautifulSoup(html_str or "", "lxml")
    return soup.get_text(separator="\n", strip=True)


def text_fingerprint(text: str) -> str:
    normalised = re.sub(r'\s+', ' ', text or "").strip().lower()
    return hashlib.md5(normalised.encode("utf-8")).hexdigest()


# ── Phase 1: Fetch Asdaa articles via WordPress REST API ──────────────────────

ASDAA_CACHE = OUT_DIR / "asdaa_api_cache.json"


def fetch_asdaa_articles() -> list:
    """
    Retrieve all articles tagged with the Sheikh's tag from asdaa-alsaa.com
    using the WordPress REST API — single request, full content, no scraping.
    Result is cached to asdaa_api_cache.json to avoid repeat requests.
    Delete the cache file to force a fresh fetch.
    """
    if ASDAA_CACHE.exists():
        print(f"  Loading from cache: {ASDAA_CACHE}")
        raw = json.loads(ASDAA_CACHE.read_text(encoding="utf-8"))
    else:
        url = (
            f"https://{ASDAA}/wp-json/wp/v2/posts"
            f"?tags={ASDAA_TAG_ID}&per_page=100&page=1"
            f"&_fields=id,link,title,date,excerpt,content"
            f"&orderby=date&order=desc"
        )
        print(f"  GET {url[:90]}")
        resp = requests.get(url, headers=HEADERS, verify=CA_BUNDLE, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        print(f"  Posts: {len(raw)}  (X-WP-Total: {resp.headers.get('X-WP-Total', '?')})")
        # Store parsed fields
        raw = [{
            "id":    p["id"],
            "url":   p["link"].rstrip("/") + "/",
            "title": html_lib.unescape(p["title"]["rendered"]),
            "date":  p["date"][:10],
            "summary":   strip_html(p.get("excerpt", {}).get("rendered", ""))[:400],
            "full_text": strip_html(p.get("content", {}).get("rendered", "")),
        } for p in raw]
        ASDAA_CACHE.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  Cached to {ASDAA_CACHE}")

    return [{
        "type":         "Asdaa",
        "msg_id":       "",
        "date":         ymd_to_dmy(p["date"]),
        "url":          p["url"],
        "title":        p["title"],
        "summary":      p.get("summary", "")[:400],
        "full_text":    p.get("full_text", ""),
        "fetch_status": "ok (REST API)",
        "source_file":  "asdaa_api",
    } for p in raw]


# ── Phase 2: Parse Telegram HTML for Jeddah Club links and TG articles ────────

def parse_html_files() -> tuple:
    """
    Returns:
      jclub_map   — {url: entry_dict}  (deduped, first-seen wins)
      tg_articles — [entry_dict, ...]
    """
    jclub_map  = {}
    tg_articles = []

    for html_file in HTML_FILES:
        print(f"  Parsing {html_file.name} …")
        soup = BeautifulSoup(html_file.read_text(encoding="utf-8"), "lxml")

        for msg in soup.find_all("div", id=re.compile(r'^message\d+$')):
            msg_id   = msg["id"]
            date_el  = msg.find(class_="date")
            date     = parse_date_dmy(date_el.get("title", "") if date_el else "")
            text_div = msg.find("div", class_="text")
            if not text_div:
                continue

            # ── Jeddah Club links ──
            jclub_links, asdaa_links = [], []
            for a in text_div.find_all("a", href=True):
                href = a["href"].strip()
                if JCLUB in href and href not in jclub_map:
                    jclub_map[href] = {
                        "type": "JeddahClub", "msg_id": msg_id, "date": date,
                        "url": href, "title": "", "summary": "", "full_text": "",
                        "fetch_status": "ssl_error (proxy TLS incompatible)",
                        "source_file": html_file.name,
                    }
                    jclub_links.append(href)
                elif JCLUB in href:
                    jclub_links.append(href)
                if ASDAA in href:
                    asdaa_links.append(href)

            # ── Telegram Articles ──
            if asdaa_links or jclub_links:
                continue
            if msg.find("a", class_="media_audio_file") or msg.find("audio"):
                continue
            if msg.find(class_="forwarded"):
                continue

            raw_text = text_div.get_text(separator="\n").strip()
            if len(raw_text) < TG_MIN_CHARS or AUDIO_ANN_RE.search(raw_text):
                continue

            lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
            title = ""
            for line in lines[:6]:
                clean = re.sub(r'^[^؀-ۿa-zA-Z0-9]+', '', line).strip()
                clean = re.sub(r'[*_`]+', '', clean).strip()
                if len(clean) > 5:
                    title = clean
                    break

            tg_articles.append({
                "type": "TelegramArticle", "msg_id": msg_id, "date": date, "url": None,
                "title": title,
                "summary": " ".join(raw_text.split()[:70]),
                "full_text": raw_text,
                "fetch_status": "n/a",
                "source_file": html_file.name,
            })

    return jclub_map, tg_articles


# ── Phase 3: Deduplicate ──────────────────────────────────────────────────────

def deduplicate(all_entries: list) -> tuple:
    """Remove duplicates. Returns (unique_list, duplicate_list)."""
    unique, duplicates = [], []
    seen_urls:  set = set()
    seen_mids:  set = set()
    seen_fps:   set = set()

    for e in all_entries:
        etype  = e.get("type", "")
        url    = (e.get("url") or "").rstrip("/") + "/"
        msg_id = e.get("msg_id", "")
        text   = e.get("full_text", "") or ""

        if url != "/" and etype in ("Asdaa", "JeddahClub"):
            if url in seen_urls:
                e["_dup_reason"] = f"duplicate URL"
                duplicates.append(e)
                continue
            seen_urls.add(url)

        if msg_id and etype == "TelegramArticle":
            if msg_id in seen_mids:
                e["_dup_reason"] = "duplicate msg_id"
                duplicates.append(e)
                continue
            seen_mids.add(msg_id)
            fp = text_fingerprint(text)
            if fp in seen_fps:
                e["_dup_reason"] = "duplicate content"
                duplicates.append(e)
                continue
            seen_fps.add(fp)

        unique.append(e)

    return unique, duplicates


# ── Phase 4: Save outputs ─────────────────────────────────────────────────────

def save_outputs(unique: list, dups: list):
    STRIP = {"raw_text", "raw_html"}

    # JSON (unique records)
    json_path = OUT_DIR / "articles_full.json"
    json_path.write_text(
        json.dumps([{k: v for k, v in e.items() if k not in STRIP} for e in unique],
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  Saved JSON: {json_path}  ({len(unique)} entries)")

    # JSON (duplicates log)
    dup_path = OUT_DIR / "articles_duplicates.json"
    dup_path.write_text(
        json.dumps([{k: v for k, v in e.items() if k not in STRIP} for e in dups],
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  Duplicates: {dup_path}  ({len(dups)} records)")

    # Excel
    def row(i, e, is_dup=False):
        r = {
            "DB#":          i + 1,
            "Type":         e.get("type", ""),
            "Date":         e.get("date", ""),
            "URL":          e.get("url") or "—",
            "Title":        e.get("title", ""),
            "Summary":      (e.get("summary") or "")[:280],
            "FetchStatus":  e.get("fetch_status", ""),
            "MessageID":    e.get("msg_id", ""),
            "FullTextChars": len(e.get("full_text") or ""),
            "SourceFile":   e.get("source_file", ""),
        }
        if is_dup:
            r["DupReason"] = e.get("_dup_reason", "")
        return r

    df_u = pd.DataFrame([row(i, e) for i, e in enumerate(unique)])
    df_d = pd.DataFrame([row(i, e, True) for i, e in enumerate(dups)])

    xlsx_path = OUT_DIR / "articles_summary.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df_u.to_excel(writer, sheet_name="All (unique)", index=False)
        for sheet, label in [
            ("Asdaa (99)",       "Asdaa"),
            ("JeddahClub (43)",  "JeddahClub"),
            ("TGArticles (259)", "TelegramArticle"),
        ]:
            sub = df_u[df_u["Type"] == label].reset_index(drop=True)
            if not sub.empty:
                sub.to_excel(writer, sheet_name=sheet, index=False)
        if not df_d.empty:
            df_d.to_excel(writer, sheet_name="Duplicates", index=False)

    print(f"  Saved Excel: {xlsx_path}")
    return json_path, xlsx_path


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print("Phase 1 — Fetching Asdaa articles via REST API")
    print("=" * 60)
    asdaa_entries = fetch_asdaa_articles()

    print("\n" + "=" * 60)
    print("Phase 2 — Parsing HTML for Jeddah Club links & Telegram articles")
    print("=" * 60)
    jclub_map, tg_articles = parse_html_files()
    print(f"  Jeddah Club URLs:  {len(jclub_map)}")
    print(f"  Telegram articles: {len(tg_articles)}")

    print("\n" + "=" * 60)
    print("Phase 3 — Deduplicating")
    print("=" * 60)
    all_entries = asdaa_entries + list(jclub_map.values()) + tg_articles
    unique, dups = deduplicate(all_entries)
    print(f"  Before: {len(all_entries)}  |  Unique: {len(unique)}  |  Duplicates: {len(dups)}")
    for d in dups:
        print(f"    [{d.get('type',''):15s}] {d.get('msg_id',''):12s}  {d.get('_dup_reason','')}")

    print("\n" + "=" * 60)
    print("Phase 4 — Saving outputs")
    print("=" * 60)
    save_outputs(unique, dups)

    # ── Missing articles report ──
    existing_channel_urls = set()
    for html_file in HTML_FILES:
        soup = BeautifulSoup(html_file.read_text(encoding="utf-8"), "lxml")
        for a in soup.find_all("a", href=re.compile(r'asdaa-alsaa\.com/\d+')):
            existing_channel_urls.add(a["href"].strip().rstrip("/") + "/")

    missing = [e for e in asdaa_entries
               if e["url"].rstrip("/")+"/" not in existing_channel_urls]

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Asdaa articles (REST API): {len(asdaa_entries):4d}")
    print(f"  Jeddah Club (HTML links):  {len(jclub_map):4d}  (text unavailable — TLS issue)")
    print(f"  Telegram articles:         {len(tg_articles):4d}")
    print(f"  Total unique:              {len(unique):4d}")
    print(f"  Duplicates removed:        {len(dups):4d}")
    if missing:
        print(f"\n  ⚑ {len(missing)} Asdaa article(s) NOT in Telegram channel (added to archive):")
        for e in missing:
            print(f"    {e['date']}  {e['url']}")
            print(f"           {e['title']}")


if __name__ == "__main__":
    main()
