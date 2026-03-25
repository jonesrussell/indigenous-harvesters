"""
Scrape CivicInfo BC First Nations directory for contact info (email, phone, website).

Uses Playwright (non-headless) to bypass Cloudflare, then decodes obfuscated emails
server-side from data-cfemail attributes — no need to wait for Cloudflare JS.

Run: .venv/bin/python notebooks/scrape_civicinfo.py

Requires: pip install playwright beautifulsoup4 lxml && playwright install chromium
"""

import json
import re
import time
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

DATA_DIR = Path(__file__).parent / "data"


def decode_cfemail(encoded: str) -> str:
    """Decode Cloudflare's data-cfemail obfuscated email addresses."""
    key = int(encoded[:2], 16)
    return "".join(
        chr(int(encoded[i : i + 2], 16) ^ key) for i in range(2, len(encoded), 2)
    )


def extract_entries(html: str) -> list[dict]:
    """Extract First Nation entries from a CivicInfo directory page."""
    soup = BeautifulSoup(html, "lxml")
    entries = []

    for li in soup.select("li"):
        text = li.get_text(" ", strip=True)
        if "Ph:" not in text:
            continue

        # Name: first link pointing to a detail page with meaningful text
        name = ""
        for a in li.select('a[href*="firstnations?id="]'):
            t = a.get_text(strip=True)
            if len(t) > 3:
                name = t
                break
        if not name:
            continue

        # Email: decode from data-cfemail if present, else check mailto
        email = ""
        cfe = li.select_one("[data-cfemail]")
        if cfe:
            email = decode_cfemail(cfe["data-cfemail"])
        else:
            # Check href with /cdn-cgi/l/email-protection#<encoded>
            for a in li.select("a[href]"):
                href = a.get("href", "")
                if "/cdn-cgi/l/email-protection" in href and "#" in href:
                    encoded = href.split("#")[-1]
                    if len(encoded) >= 4:
                        email = decode_cfemail(encoded)
                        break
            if not email:
                mailto = li.select_one('a[href^="mailto:"]')
                if mailto:
                    email = mailto["href"].replace("mailto:", "").split("?")[0]

        # Website: first external link not pointing to civicinfo or s3.ca
        website = ""
        for a in li.select('a[href^="http"]'):
            href = a.get("href", "")
            if "civicinfo" not in href and "s3.ca" not in href:
                website = href
                break

        # Phone
        ph_match = re.search(r"Ph:\s*([\d\-(). ]+)", text)
        phone = ph_match.group(1).strip() if ph_match else ""

        entries.append(
            {"name": name, "email": email, "phone": phone, "website": website}
        )

    return entries


def main():
    all_entries = []
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = ctx.new_page()

        for pn in range(1, 22):
            url = (
                f"https://www.civicinfo.bc.ca/firstnations?pn={pn}"
                if pn > 1
                else "https://www.civicinfo.bc.ca/firstnations"
            )
            try:
                page.goto(url, timeout=60000)
                page.wait_for_selector('a[href*="firstnations?id="]', timeout=15000)

                # Get raw HTML and decode emails server-side
                html = page.content()
                entries = extract_entries(html)

                if not entries:
                    print(f"Page {pn}: no entries found — stopping")
                    break

                all_entries.extend(entries)
                with_email = sum(1 for e in entries if e["email"])
                print(f"Page {pn}: {len(entries)} entries ({with_email} with email)")
                time.sleep(1.5)
            except Exception as e:
                print(f"Page {pn}: error {e}")
                break

        browser.close()

    total_email = sum(1 for e in all_entries if e["email"])
    print(f"\nTotal: {len(all_entries)}, With email: {total_email}")

    out = DATA_DIR / "civicinfo_bc_all.json"
    out.write_text(json.dumps(all_entries, indent=2))
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
