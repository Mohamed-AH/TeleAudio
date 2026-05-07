"""
Phase 1b – Parse non-audio link messages from all HTML files
Reads all messages*.html files, finds messages that have no audio attachment
but contain at least one HTTP link (YouTube or other), and writes:
  checkpoints/raw_link_messages.json
"""

import json
import re
from pathlib import Path

from bs4 import BeautifulSoup

ROOT = Path(__file__).parent.parent
HTML_FILES = [
    ROOT / "messages.html",
    ROOT / "messages2.html",
    ROOT / "messages3.html",
    ROOT / "messages4.html",
    ROOT / "messages_new.html",
]
OUTPUT = ROOT / "checkpoints" / "raw_link_messages.json"

# Links to skip — Telegram channel links, emoji anchors, etc.
_SKIP_PATTERNS = re.compile(
    r"t\.me/|telegram\.me/|tg://|javascript:|#$",
    re.IGNORECASE,
)
_YOUTUBE_RE = re.compile(r"youtu\.be|youtube\.com", re.IGNORECASE)
_AUDIO_EXT_RE = re.compile(r"\.(m4a|mp3|aac|amr|ogg|opus)([\?#]|$)", re.IGNORECASE)


def is_audio_file_link(href: str) -> bool:
    return bool(_AUDIO_EXT_RE.search(href))


def classify_link(href: str) -> str:
    if _YOUTUBE_RE.search(href):
        return "YouTube"
    if is_audio_file_link(href):
        return "AudioURL"
    return "Other"


def pick_primary_link(hrefs: list[str]) -> str:
    """Return the most meaningful link: YouTube > AudioURL > first other."""
    for href in hrefs:
        if _YOUTUBE_RE.search(href):
            return href
    for href in hrefs:
        if is_audio_file_link(href):
            return href
    return hrefs[0]


def parse_file(html_path: Path) -> list[dict]:
    print(f"  Parsing {html_path.name} …", flush=True)
    with open(html_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "lxml")

    records = []
    skipped_audio = 0

    for message_div in soup.select("div.message.default"):
        msg_id = message_div.get("id", "")

        # Skip messages that have an audio attachment
        if message_div.find("a", class_=re.compile(r"media_audio_file")):
            skipped_audio += 1
            continue
        audio_media = message_div.find("a", class_=re.compile(r"media_file"))
        if audio_media:
            href_check = audio_media.get("href", "").lower()
            if any(href_check.endswith(ext) for ext in (".m4a", ".mp3", ".aac", ".amr", ".ogg", ".opus")):
                skipped_audio += 1
                continue

        # Collect all HTTP links from the message text
        text_el = message_div.find("div", class_="text")
        if not text_el:
            continue

        all_links = text_el.find_all("a", href=True)
        hrefs = [
            a["href"] for a in all_links
            if a["href"].startswith("http") and not _SKIP_PATTERNS.search(a["href"])
        ]
        if not hrefs:
            continue

        date_el = message_div.find("div", class_="date")
        date_raw = date_el["title"].split(" ")[0] if date_el and date_el.get("title") else ""
        message_text = text_el.get_text(separator="\n", strip=True)

        primary_link = pick_primary_link(hrefs)
        link_type = classify_link(primary_link)

        # Collect all YouTube links separately for reference
        youtube_links = [h for h in hrefs if _YOUTUBE_RE.search(h)]
        other_links = [h for h in hrefs if not _YOUTUBE_RE.search(h)]

        records.append({
            "id": f"{html_path.name}::{msg_id}",
            "source_file": html_path.name,
            "message_id": msg_id,
            "primary_link": primary_link,
            "link_type": link_type,
            "all_links": hrefs,
            "youtube_links": youtube_links,
            "other_links": other_links,
            "date_raw": date_raw,
            "message_text": message_text,
        })

    print(f"    → {len(records)} link messages ({skipped_audio} audio messages skipped)")
    return records


def main():
    print("=== Phase 1b: Parsing link-only messages ===\n")
    all_records = []
    for html_file in HTML_FILES:
        if not html_file.exists():
            print(f"  WARNING: {html_file.name} not found — skipping")
            continue
        all_records.extend(parse_file(html_file))

    print(f"\nTotal link messages extracted: {len(all_records)}")
    OUTPUT.parent.mkdir(exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
    print(f"Saved → {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
