"""
Match CivicInfo BC scraped data to NC communities and import emails.

Run: .venv/bin/python notebooks/match_civicinfo_to_nc.py [--dry-run]
"""

import json
import re
import sys
from pathlib import Path

import httpx

DATA_DIR = Path(__file__).parent / "data"
DRY_RUN = "--dry-run" in sys.argv


def fetch_all_nc_communities() -> list[dict]:
    """Fetch all communities from NC public API."""
    all_communities = []
    offset = 0
    while True:
        resp = httpx.get(
            f"https://api.northcloud.one/api/v1/communities?limit=100&offset={offset}",
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()["communities"]
        all_communities.extend(batch)
        if len(batch) < 100:
            break
        offset += 100
    return all_communities


def fetch_band_office(community_id: str) -> dict | None:
    """Fetch existing band office for a community."""
    resp = httpx.get(
        f"https://api.northcloud.one/api/v1/communities/{community_id}/band-office",
        timeout=10,
    )
    if resp.status_code == 200:
        return resp.json().get("band_office")
    return None


def normalize(name: str) -> str:
    """Normalize a community name for matching."""
    # Remove parenthetical alternate names
    name = re.sub(r"\s*\(.*?\)", "", name)
    # Remove leading ? (ISC data artifact)
    name = name.lstrip("?").strip()
    # Normalize whitespace
    name = " ".join(name.split())
    return name.lower()


def match_communities(civic_entries: list[dict], nc_communities: list[dict]):
    """Match CivicInfo entries to NC communities."""
    # Build lookup maps for NC BC communities
    nc_bc = [c for c in nc_communities if c.get("province") == "BC"]
    nc_by_exact = {}
    nc_by_normalized = {}
    for c in nc_bc:
        nc_by_exact[c["name"].lower().strip()] = c
        nc_by_normalized[normalize(c["name"])] = c

    matched = []
    unmatched = []

    for entry in civic_entries:
        if not entry.get("email"):
            continue

        civic_name = entry["name"].strip()
        civic_norm = normalize(civic_name)

        found = None

        # 1. Exact match
        if civic_name.lower() in nc_by_exact:
            found = nc_by_exact[civic_name.lower()]
        # 2. Normalized match
        elif civic_norm in nc_by_normalized:
            found = nc_by_normalized[civic_norm]
        else:
            # 3. Substring match (civic name in NC name or vice versa)
            for nc_norm, nc in nc_by_normalized.items():
                if civic_norm in nc_norm or nc_norm in civic_norm:
                    found = nc
                    break
            # 4. Try matching the parenthetical alternate name
            if not found:
                alt_match = re.search(r"\(([^)]+)\)", civic_name)
                if alt_match:
                    alt = alt_match.group(1).lower().strip()
                    for nc_norm, nc in nc_by_normalized.items():
                        if alt in nc_norm:
                            found = nc
                            break

        if found:
            matched.append({"civic": entry, "nc": found})
        else:
            unmatched.append(entry)

    return matched, unmatched


def main():
    # Load CivicInfo data
    civic_path = DATA_DIR / "civicinfo_bc_all.json"
    civic = json.load(open(civic_path))
    print(f"CivicInfo entries: {len(civic)}")
    print(f"CivicInfo with email: {sum(1 for e in civic if e.get('email'))}")

    # Fetch NC communities
    print("\nFetching NC communities...")
    nc_communities = fetch_all_nc_communities()
    nc_bc = [c for c in nc_communities if c.get("province") == "BC"]
    print(f"NC total: {len(nc_communities)}, NC BC: {len(nc_bc)}")

    # Match
    matched, unmatched = match_communities(civic, nc_communities)
    print(f"\nMatched: {len(matched)}")
    print(f"Unmatched: {len(unmatched)}")

    if unmatched:
        print("\nUnmatched CivicInfo entries:")
        for u in unmatched:
            print(f"  - {u['name']} ({u['email']})")

    # Check how many matched entries have NEW emails (band office has no email)
    new_emails = 0
    updated_emails = 0
    already_has = 0

    print("\n--- Checking existing band offices ---")
    for m in matched:
        nc = m["nc"]
        civic_email = m["civic"]["email"]
        office = fetch_band_office(nc["id"])

        if office and office.get("email") and office["email"] == civic_email:
            already_has += 1
        elif office and office.get("email"):
            updated_emails += 1
            print(f"  UPDATE: {nc['name']}: {office['email']} -> {civic_email}")
        else:
            new_emails += 1
            print(f"  NEW: {nc['name']}: {civic_email}")

    print(f"\nSummary:")
    print(f"  Already has same email: {already_has}")
    print(f"  Would update email: {updated_emails}")
    print(f"  Would add NEW email: {new_emails}")

    if DRY_RUN:
        print("\n[DRY RUN] No changes made.")
        return

    # Save match results for import
    results = {
        "matched": [
            {
                "nc_id": m["nc"]["id"],
                "nc_name": m["nc"]["name"],
                "civic_name": m["civic"]["name"],
                "email": m["civic"]["email"],
                "phone": m["civic"].get("phone", ""),
            }
            for m in matched
        ],
        "unmatched": unmatched,
        "stats": {
            "total_civic": len(civic),
            "with_email": sum(1 for e in civic if e.get("email")),
            "matched": len(matched),
            "unmatched": len(unmatched),
            "new_emails": new_emails,
            "updated_emails": updated_emails,
            "already_has": already_has,
        },
    }
    out = DATA_DIR / "civicinfo_bc_matched.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nSaved match results: {out}")


if __name__ == "__main__":
    main()
