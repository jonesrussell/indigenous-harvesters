"""Analyze Ontario email gap: which communities have websites but no email."""

import json
from pathlib import Path

import httpx

DATA_DIR = Path(__file__).parent / "data"
BASE = "http://localhost:18050"


def main():
    # Fetch all communities
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

        on_communities = [c for c in all_nc if c.get("province") == "ON"]

        with_website_no_email = []
        no_website_no_email = []
        with_email = []

        for c in on_communities:
            cid = c["id"]
            bo = client.get(f"{BASE}/api/v1/communities/{cid}/band-office")
            office = {}
            if bo.status_code == 200:
                office = bo.json().get("band_office") or {}

            has_email = bool(office.get("email"))
            has_website = bool(c.get("website"))

            if has_email:
                with_email.append(c["name"])
            elif has_website:
                with_website_no_email.append(
                    {"name": c["name"], "id": c["id"], "website": c["website"]}
                )
            else:
                no_website_no_email.append(c["name"])

    print(f"Ontario: {len(on_communities)} total")
    print(f"  With email: {len(with_email)}")
    print(f"  Has website, no email: {len(with_website_no_email)}")
    print(f"  No website, no email: {len(no_website_no_email)}")

    print(f"\nWebsite but no email ({len(with_website_no_email)}):")
    for w in with_website_no_email:
        print(f"  {w['name']}: {w['website']}")

    print(f"\nNo website ({len(no_website_no_email)}):")
    for n in no_website_no_email:
        print(f"  {n}")

    # Save for scraping
    out = DATA_DIR / "on_website_no_email.json"
    out.write_text(json.dumps(with_website_no_email, indent=2))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
