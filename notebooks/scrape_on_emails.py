"""
Scrape Ontario First Nation websites for email addresses.

Visits each community website, finds contact/about pages, and extracts emails.
Uses Playwright for JavaScript-rendered sites.

Run: .venv/bin/python notebooks/scrape_on_emails.py
"""

import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright

DATA_DIR = Path(__file__).parent / "data"

# Email regex — skip common false positives
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
SKIP_EMAILS = {
    "example.com", "sentry.io", "wordpress.org", "w3.org", "schema.org",
    "gravatar.com", "googleusercontent.com", "gstatic.com", "google.com",
    "facebook.com", "twitter.com", "instagram.com", "youtube.com",
    "godaddy.com", "wixsite.com", "squarespace.com",
}

# Contact page URL patterns to try
CONTACT_PATHS = [
    "/contact", "/contact-us", "/contactus", "/about/contact",
    "/about", "/about-us", "/aboutus",
    "/administration", "/admin", "/band-office", "/band-council",
    "/governance", "/leadership", "/council",
]


def is_valid_email(email: str) -> bool:
    """Filter out junk emails."""
    email = email.lower().strip()
    domain = email.split("@")[-1]
    if domain in SKIP_EMAILS:
        return False
    if any(x in email for x in ["noreply", "no-reply", "donotreply", "test@", "admin@wordpress"]):
        return False
    if len(email) > 80:
        return False
    return True


def decode_cfemail(encoded: str) -> str:
    """Decode Cloudflare data-cfemail obfuscated emails."""
    key = int(encoded[:2], 16)
    return "".join(
        chr(int(encoded[i : i + 2], 16) ^ key) for i in range(2, len(encoded), 2)
    )


def extract_emails_from_page(page) -> set[str]:
    """Extract emails from the current page."""
    emails = set()

    html = page.content()

    # 1. Cloudflare data-cfemail
    for match in re.finditer(r'data-cfemail="([a-f0-9]+)"', html):
        try:
            email = decode_cfemail(match.group(1))
            if is_valid_email(email):
                emails.add(email.lower())
        except (ValueError, IndexError):
            pass

    # 2. mailto: links
    for match in re.finditer(r'mailto:([^"\'?\s]+)', html):
        email = match.group(1).strip()
        if is_valid_email(email):
            emails.add(email.lower())

    # 3. Visible email patterns in text
    text = page.inner_text("body")
    for match in EMAIL_RE.finditer(text):
        email = match.group(0)
        if is_valid_email(email):
            emails.add(email.lower())

    return emails


def scrape_community(page, base_url: str) -> dict:
    """Scrape a community website for emails."""
    all_emails = set()
    pages_checked = []

    # Parse base URL
    parsed = urlparse(base_url)
    if not parsed.scheme:
        base_url = "http://" + base_url

    # Check homepage first
    try:
        page.goto(base_url, timeout=15000, wait_until="domcontentloaded")
        time.sleep(1)
        emails = extract_emails_from_page(page)
        all_emails.update(emails)
        pages_checked.append(base_url)
    except Exception:
        return {"emails": [], "pages_checked": [], "error": "homepage_failed"}

    # Try contact pages
    for path in CONTACT_PATHS:
        if len(all_emails) >= 3:
            break  # Got enough
        contact_url = urljoin(base_url, path)
        try:
            resp = page.goto(contact_url, timeout=10000, wait_until="domcontentloaded")
            if resp and resp.status < 400:
                time.sleep(0.5)
                emails = extract_emails_from_page(page)
                all_emails.update(emails)
                pages_checked.append(contact_url)
        except Exception:
            continue

    return {
        "emails": sorted(all_emails),
        "pages_checked": pages_checked,
    }


def pick_best_email(emails: list[str]) -> str:
    """Pick the most likely general contact email."""
    if not emails:
        return ""
    # Prefer generic prefixes
    for prefix in ["info@", "contact@", "reception@", "admin@", "office@", "general@", "band@"]:
        for e in emails:
            if e.startswith(prefix):
                return e
    return emails[0]


def main():
    # Load target list
    targets = json.load(open(DATA_DIR / "on_website_no_email.json"))
    print(f"Ontario targets: {len(targets)} communities with websites, no email\n")

    results = []
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

        for i, target in enumerate(targets):
            name = target["name"]
            website = target["website"]
            community_id = target["id"]

            result = scrape_community(page, website)
            best = pick_best_email(result["emails"])

            results.append({
                "name": name,
                "id": community_id,
                "website": website,
                "best_email": best,
                "all_emails": result["emails"],
                "pages_checked": len(result.get("pages_checked", [])),
                "error": result.get("error", ""),
            })

            status = best if best else ("ERROR" if result.get("error") else "none")
            print(f"  [{i+1}/{len(targets)}] {name}: {status}")
            time.sleep(0.5)

        browser.close()

    # Summary
    found = [r for r in results if r["best_email"]]
    errors = [r for r in results if r.get("error")]
    print(f"\nFound emails: {len(found)}/{len(targets)}")
    print(f"Errors: {len(errors)}")

    if found:
        print("\nEmails found:")
        for r in found:
            print(f"  {r['name']}: {r['best_email']}")

    # Save
    out = DATA_DIR / "on_website_emails.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
