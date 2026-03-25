"""
Import Ontario scraped emails to NC band offices.

Run: .venv/bin/python notebooks/import_on_emails.py [--dry-run]

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

NC_PUBLIC = "https://api.northcloud.one"
NC_WRITE = "http://localhost:18050"
JWT_SECRET = "CpgfEP9nKSzLJTzfSJKW8ynwsOXrlQJ1h8ZQRbYSBJ4="

# Shared tribal council emails — skip these as they're not community-specific
SHARED_EMAILS = {
    "reception@ifna.ca",
    "info@fourarrowsrha.org",
}


def make_jwt() -> str:
    now = int(time.time())
    return jwt.encode(
        {"sub": "enrichment-script", "iat": now, "exp": now + 3600},
        JWT_SECRET,
        algorithm="HS256",
    )


def fetch_band_office(community_id: str) -> dict | None:
    resp = httpx.get(
        f"{NC_WRITE}/api/v1/communities/{community_id}/band-office", timeout=10
    )
    if resp.status_code == 200:
        return resp.json().get("band_office") or {}
    return None


def upsert_email(community_id: str, email: str, existing: dict | None, token: str) -> bool:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"email": email, "data_source": "website_scrape"}
    if existing:
        for field in ["address_line1", "city", "province", "postal_code", "phone", "fax", "toll_free", "office_hours"]:
            if existing.get(field):
                body[field] = existing[field]
        if existing.get("data_source") in ("isc_profile", "civicinfo_bc"):
            body["data_source"] = existing["data_source"]

    resp = httpx.post(
        f"{NC_WRITE}/api/v1/communities/{community_id}/band-office",
        json=body,
        headers=headers,
        timeout=10,
    )
    return resp.status_code == 200


def main():
    results = json.load(open(DATA_DIR / "on_website_emails.json"))
    with_email = [r for r in results if r["best_email"] and r["best_email"] not in SHARED_EMAILS]
    print(f"Ontario results: {len(results)} total, {len(with_email)} with community-specific email")

    token = make_jwt()
    new_count = 0
    update_count = 0
    skip_count = 0
    error_count = 0

    for r in with_email:
        name = r["name"]
        email = r["best_email"]
        community_id = r["id"]

        office = fetch_band_office(community_id)
        if office and office.get("email") and office["email"].lower() == email.lower():
            skip_count += 1
            continue

        action = "UPDATE" if (office and office.get("email")) else "NEW"

        if DRY_RUN:
            print(f"  [{action}] {name}: {email}")
            if action == "NEW":
                new_count += 1
            else:
                update_count += 1
        else:
            ok = upsert_email(community_id, email, office, token)
            if ok:
                print(f"  [{action}] {name}: {email}")
                if action == "NEW":
                    new_count += 1
                else:
                    update_count += 1
            else:
                print(f"  [ERROR] {name}")
                error_count += 1
            time.sleep(0.1)

    print(f"\nDone{'  [DRY RUN]' if DRY_RUN else ''}:")
    print(f"  New: {new_count}")
    print(f"  Updated: {update_count}")
    print(f"  Skipped (same): {skip_count}")
    print(f"  Errors: {error_count}")
    print(f"  Filtered shared emails: {sum(1 for r in results if r['best_email'] in SHARED_EMAILS)}")


if __name__ == "__main__":
    main()
