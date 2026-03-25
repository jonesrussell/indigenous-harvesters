"""
Pre-generate scraping target lists by province.
Saves JSON files so the Playwright scraper doesn't need the SSH tunnel.

Run: .venv/bin/python notebooks/generate_targets.py AB SK MB
     .venv/bin/python notebooks/generate_targets.py --all

Requires SSH tunnel: ssh -f -N -L 18050:localhost:8050 deployer@147.182.150.145
"""

import json
import sys
from pathlib import Path

import httpx

DATA_DIR = Path(__file__).parent / "data"
BASE = "http://localhost:18050"


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    if "--all" in sys.argv:
        provinces = ["AB", "SK", "MB", "QC", "NB", "NS", "NL", "NT", "YT", "ON", "BC"]
    elif args:
        provinces = [a.upper() for a in args]
    else:
        print("Usage: generate_targets.py AB SK MB  OR  --all")
        sys.exit(1)

    print("Fetching all communities...")
    all_nc = []
    offset = 0
    with httpx.Client(timeout=60) as client:
        while True:
            resp = client.get(f"{BASE}/api/v1/communities?limit=100&offset={offset}")
            resp.raise_for_status()
            batch = resp.json()["communities"]
            all_nc.extend(batch)
            if len(batch) < 100:
                break
            offset += 100
        print(f"Total: {len(all_nc)}")

        # Filter to requested provinces
        filtered = [c for c in all_nc if c.get("province") in provinces]
        print(f"In {', '.join(provinces)}: {filtered}")

        # Check band offices
        targets = []
        has_email = 0
        no_website = 0
        print("Checking band offices...")
        for i, c in enumerate(filtered):
            cid = c["id"]
            bo = client.get(f"{BASE}/api/v1/communities/{cid}/band-office")
            office = {}
            if bo.status_code == 200:
                office = bo.json().get("band_office") or {}

            if office.get("email"):
                has_email += 1
            elif c.get("website"):
                targets.append({
                    "name": c["name"],
                    "id": cid,
                    "province": c["province"],
                    "website": c["website"],
                })
            else:
                no_website += 1

            if (i + 1) % 50 == 0:
                print(f"  Checked {i + 1}/{len(filtered)}...")

    print(f"\nResults for {', '.join(provinces)}:")
    print(f"  Already has email: {has_email}")
    print(f"  Has website, no email (TARGETS): {len(targets)}")
    print(f"  No website: {no_website}")

    by_prov = {}
    for t in targets:
        by_prov.setdefault(t["province"], []).append(t)
    for prov in sorted(by_prov):
        print(f"    {prov}: {len(by_prov[prov])} targets")

    suffix = "_".join(sorted(provinces)).lower()
    out = DATA_DIR / f"targets_{suffix}.json"
    out.write_text(json.dumps(targets, indent=2))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
