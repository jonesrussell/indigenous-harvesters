"""
Import scraped province emails to NC band offices.

Usage:
  .venv/bin/python notebooks/import_province_emails.py province_ab_sk_mb_emails.json [--dry-run]

Reads a JSON file produced by scrape_province_emails.py, filters shared/generic
emails, and upserts community-specific emails to NC band offices.

Requires SSH tunnel: ssh -f -N -L 18050:localhost:8050 deployer@147.182.150.145
"""

import json
import sys
import time
from pathlib import Path

import httpx
import jwt

DATA_DIR = Path(__file__).parent / "data"
DRY_RUN = "--dry-run" in sys.argv

NC_WRITE = "http://localhost:18050"
JWT_SECRET = "CpgfEP9nKSzLJTzfSJKW8ynwsOXrlQJ1h8ZQRbYSBJ4="

# Shared/tribal council emails and bad scrapes to skip
SHARED_EMAILS = {
    "reception@ifna.ca",
    "info@fourarrowsrha.org",
    "helpdesk@pagc.net",
    "cfs.reception@mltc.net",
    "contact@stoney-nation.com",
    "/tel:",                          # bad parse
    "ryan@archerdesigns.ca",          # web designer, not community
    "dylanloblaw@gmail.com",          # personal gmail, not community
}


def make_jwt() -> str:
    now = int(time.time())
    return jwt.encode(
        {"sub": "enrichment-script", "iat": now, "exp": now + 3600},
        JWT_SECRET,
        algorithm="HS256",
    )


def fetch_band_office(client: httpx.Client, community_id: str) -> dict | None:
    resp = client.get(f"{NC_WRITE}/api/v1/communities/{community_id}/band-office")
    if resp.status_code == 200:
        return resp.json().get("band_office") or {}
    return None


def upsert_email(client: httpx.Client, community_id: str, email: str, existing: dict | None, token: str) -> bool:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"email": email, "data_source": "website_scrape"}
    if existing:
        for field in ["address_line1", "city", "province", "postal_code", "phone", "fax", "toll_free", "office_hours"]:
            if existing.get(field):
                body[field] = existing[field]
        if existing.get("data_source") in ("isc_profile", "civicinfo_bc"):
            body["data_source"] = existing["data_source"]

    resp = client.post(
        f"{NC_WRITE}/api/v1/communities/{community_id}/band-office",
        json=body,
        headers=headers,
    )
    return resp.status_code == 200


def main():
    # Find the input file
    files = [a for a in sys.argv[1:] if not a.startswith("-")]
    if not files:
        print("Usage: import_province_emails.py <results.json> [--dry-run]")
        sys.exit(1)

    input_path = DATA_DIR / files[0] if not Path(files[0]).is_absolute() else Path(files[0])
    if not input_path.exists():
        print(f"File not found: {input_path}")
        sys.exit(1)

    results = json.load(open(input_path))
    with_email = [r for r in results if r.get("best_email") and r["best_email"] not in SHARED_EMAILS]
    filtered = sum(1 for r in results if r.get("best_email") and r["best_email"] in SHARED_EMAILS)

    print(f"Input: {input_path.name}")
    print(f"Total entries: {len(results)}")
    print(f"With community-specific email: {len(with_email)}")
    print(f"Filtered shared emails: {filtered}")

    token = make_jwt()
    new_count = 0
    update_count = 0
    skip_count = 0
    error_count = 0

    with httpx.Client(timeout=15) as client:
        for r in with_email:
            name = r["name"]
            email = r["best_email"]
            community_id = r["id"]
            prov = r.get("province", "??")

            office = fetch_band_office(client, community_id)
            if office and office.get("email") and office["email"].lower() == email.lower():
                skip_count += 1
                continue

            action = "UPDATE" if (office and office.get("email")) else "NEW"

            if DRY_RUN:
                print(f"  [{action}] [{prov}] {name}: {email}")
                new_count += 1 if action == "NEW" else 0
                update_count += 1 if action == "UPDATE" else 0
            else:
                ok = upsert_email(client, community_id, email, office, token)
                if ok:
                    print(f"  [{action}] [{prov}] {name}: {email}")
                    new_count += 1 if action == "NEW" else 0
                    update_count += 1 if action == "UPDATE" else 0
                else:
                    print(f"  [ERROR] [{prov}] {name}")
                    error_count += 1
                time.sleep(0.1)

    print(f"\nDone{'  [DRY RUN]' if DRY_RUN else ''}:")
    print(f"  New: {new_count}")
    print(f"  Updated: {update_count}")
    print(f"  Skipped (same): {skip_count}")
    print(f"  Errors: {error_count}")


if __name__ == "__main__":
    main()
