"""
Search Google for FN community emails using Playwright.

Usage:
  .venv/bin/python notebooks/google_email_search.py targets_file.json
  .venv/bin/python notebooks/google_email_search.py --generate AB SK MB ON

--generate fetches targets from NC (requires SSH tunnel).
Otherwise reads a pre-generated targets JSON file.
"""

import json
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

DATA_DIR = Path(__file__).parent / "data"

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
SKIP_DOMAINS = {
    "example.com", "sentry.io", "wordpress.org", "w3.org", "schema.org",
    "gravatar.com", "googleusercontent.com", "gstatic.com", "google.com",
    "facebook.com", "twitter.com", "instagram.com", "youtube.com",
    "godaddy.com", "wixsite.com", "squarespace.com", "sentry-next.wixpress.com",
    "wix.com", "outlook.com", "hotmail.com", "gmail.com", "yahoo.com",
}


def is_valid_email(email: str) -> bool:
    email = email.lower().strip()
    domain = email.split("@")[-1]
    if domain in SKIP_DOMAINS:
        return False
    if any(x in email for x in ["noreply", "no-reply", "donotreply", "test@", "admin@wordpress", "webmaster@"]):
        return False
    return 4 < len(email) <= 80


def pick_best_email(emails: list[str], community_name: str) -> str:
    if not emails:
        return ""
    # Prefer generic contact prefixes
    for prefix in ["info@", "contact@", "reception@", "admin@", "office@", "general@", "band@"]:
        for e in emails:
            if e.startswith(prefix):
                return e
    # Prefer emails whose domain looks related to community name
    name_words = set(re.findall(r"[a-z]{3,}", community_name.lower()))
    for e in emails:
        domain = e.split("@")[1].split(".")[0]
        if any(w in domain for w in name_words if len(w) > 3):
            return e
    return emails[0]


def search_community_email(page, name: str) -> dict:
    """Google search for a community's email."""
    query = f'"{name}" first nation band office email contact'
    emails = set()

    try:
        page.goto(
            f"https://www.google.com/search?q={query.replace(' ', '+')}&num=10",
            timeout=15000,
            wait_until="domcontentloaded",
        )
        time.sleep(2)

        # Check for CAPTCHA
        if "unusual traffic" in page.content().lower():
            return {"emails": [], "error": "captcha"}

        # Extract emails from search results page
        text = page.inner_text("body")
        for match in EMAIL_RE.finditer(text):
            if is_valid_email(match.group(0)):
                emails.add(match.group(0).lower())

        # Also check snippets HTML for mailto links
        html = page.content()
        for match in re.finditer(r'mailto:([^"\'?\s]+)', html):
            if is_valid_email(match.group(1)):
                emails.add(match.group(1).lower().strip())

    except Exception as e:
        return {"emails": [], "error": str(e)[:100]}

    return {"emails": sorted(emails)}


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]

    if "--generate" in sys.argv:
        # Generate targets from NC
        import httpx
        provinces = [a.upper() for a in args]
        if not provinces:
            provinces = ["ON", "AB", "SK", "MB", "QC", "BC", "NS", "NB", "NL", "NT", "YT"]
        BASE = "http://localhost:18050"

        all_nc = []
        offset = 0
        with httpx.Client(timeout=60) as client:
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
                cid = c["id"]
                bo = client.get(f"{BASE}/api/v1/communities/{cid}/band-office")
                office = {}
                if bo.status_code == 200:
                    office = bo.json().get("band_office") or {}
                if not office.get("email"):
                    targets.append({"name": c["name"], "id": cid, "province": c["province"]})

        suffix = "_".join(sorted(provinces)).lower()
        out = DATA_DIR / f"google_targets_{suffix}.json"
        out.write_text(json.dumps(targets, indent=2))
        print(f"Generated {len(targets)} targets: {out}")
        return

    # Load targets
    if not args:
        print("Usage: google_email_search.py <targets.json>")
        print("   OR: google_email_search.py --generate ON AB SK MB")
        sys.exit(1)

    targets_file = DATA_DIR / args[0] if not Path(args[0]).is_absolute() else Path(args[0])
    targets = json.load(open(targets_file))
    print(f"Loaded {len(targets)} targets from {targets_file.name}\n")

    results = []
    captcha_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = ctx.new_page()

        for i, target in enumerate(targets):
            if captcha_count >= 3:
                print(f"\n  Too many CAPTCHAs — stopping at {i}/{len(targets)}")
                # Save remaining as-is
                for remaining in targets[i:]:
                    results.append({
                        "name": remaining["name"],
                        "id": remaining["id"],
                        "province": remaining.get("province", ""),
                        "best_email": "",
                        "all_emails": [],
                        "error": "skipped_captcha",
                    })
                break

            result = search_community_email(page, target["name"])
            best = pick_best_email(result["emails"], target["name"])

            if result.get("error") == "captcha":
                captcha_count += 1
                print(f"  [{i+1}/{len(targets)}] CAPTCHA — waiting 30s...")
                time.sleep(30)

            results.append({
                "name": target["name"],
                "id": target["id"],
                "province": target.get("province", ""),
                "best_email": best,
                "all_emails": result["emails"],
                "error": result.get("error", ""),
            })

            status = best if best else ("CAPTCHA" if result.get("error") == "captcha" else "none")
            print(f"  [{i+1}/{len(targets)}] [{target.get('province','')}] {target['name']}: {status}")

            # Polite delay to avoid CAPTCHA
            time.sleep(3 + (i % 3))  # 3-5 seconds between searches

        browser.close()

    found = [r for r in results if r["best_email"]]
    print(f"\nFound emails: {len(found)}/{len(targets)}")
    if found:
        print("\nEmails found:")
        for r in found:
            print(f"  [{r['province']}] {r['name']}: {r['best_email']}")

    out = DATA_DIR / f"google_emails_{targets_file.stem}.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
