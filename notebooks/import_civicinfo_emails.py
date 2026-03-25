"""
Match CivicInfo BC scraped data to NC communities and import emails via band-office upsert.

Run: .venv/bin/python notebooks/import_civicinfo_emails.py [--dry-run]

Requires SSH tunnel: ssh -f -N -L 18050:localhost:8050 deployer@147.182.150.145
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import jwt

DATA_DIR = Path(__file__).parent / "data"
DRY_RUN = "--dry-run" in sys.argv

NC_PUBLIC = "https://api.northcloud.one"
NC_WRITE = "http://localhost:18050"
JWT_SECRET = "CpgfEP9nKSzLJTzfSJKW8ynwsOXrlQJ1h8ZQRbYSBJ4="


def make_jwt() -> str:
    now = int(time.time())
    return jwt.encode(
        {"sub": "enrichment-script", "iat": now, "exp": now + 3600},
        JWT_SECRET,
        algorithm="HS256",
    )


def fetch_all_nc_communities() -> list[dict]:
    all_communities = []
    offset = 0
    while True:
        resp = httpx.get(
            f"{NC_PUBLIC}/api/v1/communities?limit=100&offset={offset}", timeout=30
        )
        resp.raise_for_status()
        batch = resp.json()["communities"]
        all_communities.extend(batch)
        if len(batch) < 100:
            break
        offset += 100
    return all_communities


def fetch_band_office(community_id: str) -> dict | None:
    resp = httpx.get(
        f"{NC_PUBLIC}/api/v1/communities/{community_id}/band-office", timeout=10
    )
    if resp.status_code == 200:
        return resp.json().get("band_office")
    return None


def normalize(name: str) -> str:
    """Normalize for matching: strip parens, leading ?, extra whitespace, lowercase."""
    name = re.sub(r"\s*\(.*?\)", "", name)
    name = name.lstrip("?").strip()
    name = " ".join(name.split())
    return name.lower()


def tokenize(name: str) -> set[str]:
    """Extract meaningful words, dropping common suffixes."""
    stop = {"first", "nation", "nations", "indian", "band", "of", "the", "village", "government", "tribe"}
    words = re.findall(r"[a-z']+", normalize(name))
    return {w for w in words if w not in stop and len(w) > 2}


def match_communities(civic_entries: list[dict], nc_communities: list[dict]):
    nc_bc = [c for c in nc_communities if c.get("province") == "BC"]

    # Build lookups
    nc_by_normalized = {}
    nc_by_tokens = []
    for c in nc_bc:
        norm = normalize(c["name"])
        nc_by_normalized[norm] = c
        nc_by_tokens.append((tokenize(c["name"]), c))

    matched = []
    unmatched = []
    used_nc_ids = set()  # Prevent duplicate matches

    for entry in civic_entries:
        if not entry.get("email"):
            continue

        civic_norm = normalize(entry["name"])
        civic_tokens = tokenize(entry["name"])

        found = None

        # 1. Exact normalized match
        if civic_norm in nc_by_normalized:
            found = nc_by_normalized[civic_norm]

        # 2. Try parenthetical alternate name
        if not found:
            alt_match = re.search(r"\(([^)]+)\)", entry["name"])
            if alt_match:
                for alt_part in alt_match.group(1).split(","):
                    alt_norm = normalize(alt_part)
                    if alt_norm in nc_by_normalized:
                        found = nc_by_normalized[alt_norm]
                        break

        # 3. Token overlap scoring (require >50% overlap)
        if not found:
            best_score = 0
            best_match = None
            for nc_tokens, nc in nc_by_tokens:
                if nc["id"] in used_nc_ids:
                    continue
                if not civic_tokens or not nc_tokens:
                    continue
                overlap = len(civic_tokens & nc_tokens)
                union = len(civic_tokens | nc_tokens)
                score = overlap / union if union > 0 else 0
                if score > best_score and score > 0.5:
                    best_score = score
                    best_match = nc
            if best_match:
                found = best_match

        if found and found["id"] not in used_nc_ids:
            matched.append({"civic": entry, "nc": found})
            used_nc_ids.add(found["id"])
        else:
            unmatched.append(entry)

    return matched, unmatched


def upsert_email(community_id: str, email: str, existing_office: dict | None, token: str) -> bool:
    """Upsert band office email via NC write API."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    body = {"email": email, "data_source": "civicinfo_bc"}
    # Preserve existing fields if office exists
    if existing_office:
        for field in ["address_line1", "city", "province", "postal_code", "phone", "fax", "toll_free", "office_hours"]:
            if existing_office.get(field):
                body[field] = existing_office[field]
        # Keep existing data_source if it was ISC (more authoritative for address)
        if existing_office.get("data_source") == "isc_profile":
            body["data_source"] = "isc_profile"

    resp = httpx.post(
        f"{NC_WRITE}/api/v1/communities/{community_id}/band-office",
        json=body,
        headers=headers,
        timeout=10,
    )
    return resp.status_code == 200


def main():
    civic = json.load(open(DATA_DIR / "civicinfo_bc_all.json"))
    print(f"CivicInfo entries: {len(civic)}, with email: {sum(1 for e in civic if e.get('email'))}")

    print("Fetching NC communities...")
    nc_communities = fetch_all_nc_communities()
    nc_bc = [c for c in nc_communities if c.get("province") == "BC"]
    print(f"NC total: {len(nc_communities)}, NC BC: {len(nc_bc)}")

    matched, unmatched = match_communities(civic, nc_communities)
    print(f"\nMatched: {len(matched)}, Unmatched: {len(unmatched)}")

    if unmatched:
        print("\nUnmatched:")
        for u in unmatched:
            print(f"  - {u['name']}")

    # Categorize matches
    token = make_jwt()
    new_count = 0
    update_count = 0
    skip_count = 0
    error_count = 0

    print(f"\n{'[DRY RUN] ' if DRY_RUN else ''}Importing emails...")
    for m in matched:
        nc = m["nc"]
        civic_email = m["civic"]["email"]
        office = fetch_band_office(nc["id"])

        if office and office.get("email") and office["email"].lower() == civic_email.lower():
            skip_count += 1
            continue

        action = "UPDATE" if (office and office.get("email")) else "NEW"

        if DRY_RUN:
            if action == "NEW":
                print(f"  [NEW] {nc['name']}: {civic_email}")
                new_count += 1
            else:
                print(f"  [UPDATE] {nc['name']}: {office['email']} -> {civic_email}")
                update_count += 1
        else:
            ok = upsert_email(nc["id"], civic_email, office, token)
            if ok:
                if action == "NEW":
                    new_count += 1
                else:
                    update_count += 1
                print(f"  [{action}] {nc['name']}: {civic_email}")
            else:
                error_count += 1
                print(f"  [ERROR] {nc['name']}: failed to upsert")
            time.sleep(0.1)  # gentle rate limit

    print(f"\nDone{'  [DRY RUN]' if DRY_RUN else ''}:")
    print(f"  New emails added: {new_count}")
    print(f"  Emails updated: {update_count}")
    print(f"  Already correct: {skip_count}")
    print(f"  Errors: {error_count}")
    print(f"  Unmatched: {len(unmatched)}")

    # Save results
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": DRY_RUN,
        "matched": len(matched),
        "unmatched_names": [u["name"] for u in unmatched],
        "new_emails": new_count,
        "updated_emails": update_count,
        "skipped": skip_count,
        "errors": error_count,
    }
    out = DATA_DIR / "civicinfo_bc_import_results.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"Results saved: {out}")


if __name__ == "__main__":
    main()
