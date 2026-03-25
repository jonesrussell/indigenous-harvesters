"""
Scrape FN websites for email addresses, filtered by province.

Usage:
  .venv/bin/python notebooks/scrape_province_emails.py AB SK MB
  .venv/bin/python notebooks/scrape_province_emails.py --all-gaps

Requires SSH tunnel for gap analysis:
  ssh -f -N -L 18050:localhost:8050 deployer@147.182.150.145
"""

import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from playwright.sync_api import sync_playwright

DATA_DIR = Path(__file__).parent / "data"
BASE = "http://localhost:18050"

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
SKIP_DOMAINS = {
    "example.com", "sentry.io", "wordpress.org", "w3.org", "schema.org",
    "gravatar.com", "googleusercontent.com", "gstatic.com", "google.com",
    "facebook.com", "twitter.com", "instagram.com", "youtube.com",
    "godaddy.com", "wixsite.com", "squarespace.com",
}

CONTACT_PATHS = [
    "/contact", "/contact-us", "/contactus", "/about/contact",
    "/about", "/about-us", "/administration", "/band-office",
    "/governance", "/leadership", "/council",
]


def is_valid_email(email: str) -> bool:
    email = email.lower().strip()
    domain = email.split("@")[-1]
    if domain in SKIP_DOMAINS:
        return False
    if any(x in email for x in ["noreply", "no-reply", "donotreply", "test@", "admin@wordpress"]):
        return False
    return len(email) <= 80


def decode_cfemail(encoded: str) -> str:
    key = int(encoded[:2], 16)
    return "".join(
        chr(int(encoded[i : i + 2], 16) ^ key) for i in range(2, len(encoded), 2)
    )


def extract_emails_from_page(page) -> set[str]:
    emails = set()
    html = page.content()
    for match in re.finditer(r'data-cfemail="([a-f0-9]+)"', html):
        try:
            email = decode_cfemail(match.group(1))
            if is_valid_email(email):
                emails.add(email.lower())
        except (ValueError, IndexError):
            pass
    for match in re.finditer(r'mailto:([^"\'?\s]+)', html):
        if is_valid_email(match.group(1)):
            emails.add(match.group(1).lower().strip())
    text = page.inner_text("body")
    for match in EMAIL_RE.finditer(text):
        if is_valid_email(match.group(0)):
            emails.add(match.group(0).lower())
    return emails


def scrape_community(page, base_url: str) -> dict:
    all_emails = set()
    parsed = urlparse(base_url)
    if not parsed.scheme:
        base_url = "http://" + base_url
    try:
        page.goto(base_url, timeout=15000, wait_until="domcontentloaded")
        time.sleep(1)
        all_emails.update(extract_emails_from_page(page))
    except Exception:
        return {"emails": [], "error": "homepage_failed"}
    for path in CONTACT_PATHS:
        if len(all_emails) >= 3:
            break
        try:
            resp = page.goto(urljoin(base_url, path), timeout=10000, wait_until="domcontentloaded")
            if resp and resp.status < 400:
                time.sleep(0.5)
                all_emails.update(extract_emails_from_page(page))
        except Exception:
            continue
    return {"emails": sorted(all_emails)}


def pick_best_email(emails: list[str]) -> str:
    if not emails:
        return ""
    for prefix in ["info@", "contact@", "reception@", "admin@", "office@", "general@", "band@"]:
        for e in emails:
            if e.startswith(prefix):
                return e
    return emails[0]


def get_targets(provinces: list[str]) -> list[dict]:
    """Fetch communities with websites but no email for given provinces."""
    all_nc = []
    offset = 0
    with httpx.Client(timeout=30) as client:
        while True:
            resp = client.get(f"{BASE}/api/v1/communities?limit=100&offset={offset}")
            batch = resp.json()["communities"]
            all_nc.extend(batch)
            if len(batch) < 100:
                break
            offset += 100

        targets = []
        for c in all_nc:
            if c.get("province") not in provinces:
                continue
            if not c.get("website"):
                continue
            cid = c["id"]
            bo = client.get(f"{BASE}/api/v1/communities/{cid}/band-office")
            office = {}
            if bo.status_code == 200:
                office = bo.json().get("band_office") or {}
            if not office.get("email"):
                targets.append({"name": c["name"], "id": cid, "website": c["website"], "province": c["province"]})

    return targets


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]

    # Accept pre-generated targets file OR province codes
    targets_file = None
    provinces = []
    for a in args:
        if a.endswith(".json"):
            targets_file = DATA_DIR / a if not Path(a).is_absolute() else Path(a)
        else:
            provinces.append(a.upper())

    if targets_file and targets_file.exists():
        targets = json.load(open(targets_file))
        provs = sorted(set(t.get("province", "??") for t in targets))
        print(f"Loaded {len(targets)} targets from {targets_file.name} ({', '.join(provs)})")
    elif provinces:
        if "--all-gaps" in sys.argv:
            provinces = ["AB", "SK", "MB", "QC", "NB", "NS", "NL", "NT", "YT"]
        print(f"Provinces: {', '.join(provinces)}")
        print("Fetching targets from NC...")
        targets = get_targets(provinces)
    else:
        print("Usage: scrape_province_emails.py targets_ab_sk_mb.json")
        print("   OR: scrape_province_emails.py AB SK MB")
        sys.exit(1)

    print(f"Found {len(targets)} communities with websites but no email\n")

    if not targets:
        print("No targets found.")
        return

    by_prov = {}
    for t in targets:
        by_prov.setdefault(t["province"], []).append(t)
    for prov, ts in sorted(by_prov.items()):
        print(f"  {prov}: {len(ts)} targets")
    print()

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = ctx.new_page()

        for i, target in enumerate(targets):
            result = scrape_community(page, target["website"])
            best = pick_best_email(result["emails"])
            results.append({
                "name": target["name"],
                "id": target["id"],
                "province": target["province"],
                "website": target["website"],
                "best_email": best,
                "all_emails": result["emails"],
                "error": result.get("error", ""),
            })
            status = best if best else ("ERROR" if result.get("error") else "none")
            print(f"  [{i+1}/{len(targets)}] [{target['province']}] {target['name']}: {status}")
            time.sleep(0.5)

        browser.close()

    found = [r for r in results if r["best_email"]]
    print(f"\nFound emails: {len(found)}/{len(targets)}")
    if found:
        print("\nEmails found:")
        for r in found:
            print(f"  [{r['province']}] {r['name']}: {r['best_email']}")

    suffix = "_".join(provinces).lower()
    out = DATA_DIR / f"province_{suffix}_emails.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
