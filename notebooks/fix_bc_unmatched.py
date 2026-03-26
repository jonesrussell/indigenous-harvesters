"""
Fix unmatched CivicInfo BC entries with manual mapping + aggressive fuzzy matching.

Run: .venv/bin/python notebooks/fix_bc_unmatched.py [--dry-run]

Requires SSH tunnel for import: ssh -f -N -L 18050:localhost:8050 deployer@...
"""

import json
import re
import sys
import time
from pathlib import Path

import httpx
import jwt

DATA_DIR = Path(__file__).parent / "data"
DRY_RUN = "--dry-run" in sys.argv
NC_WRITE = "http://localhost:18050"
NC_PUBLIC = "https://api.northcloud.one"
JWT_SECRET = "CpgfEP9nKSzLJTzfSJKW8ynwsOXrlQJ1h8ZQRbYSBJ4="

# Manual mapping: CivicInfo name fragment -> NC name fragment
MANUAL_MAP = {
    "adams lake": "Adams Lake",
    "akisq'nuk": "?Akisq'nuk",
    "alexis creek": "Tl'esqox",  # Alexis Creek = Tsideldel = Tl'esqox
    "cowichan tribes": "Cowichan",
    "dzawada": "Da'naxda'xw",  # Actually different nation — skip
    "gitwangak": "Gitwangak",
    "haisla nation": "Haisla Nation",
    "iskut band": "Iskut",
    "k'omoks": "K'omoks",  # May need exact match
    "kitasoo": "Kitasoo",
    "ktunaxa nation": None,  # Tribal gov, not a single band
    "kwakiutl band": "Kwakiutl",
    "leq' á:mel": "Leq'a:mel",
    "little shuswap": "Skwlax te Secwepemculecw",
    "lower kootenay": "Lower Kootenay",
    "nadleh whut'en": "Nadleh Whut'en",
    "nak'azdli": "Nak'azdli Whut'en",
    "naut'sa mawt": None,  # Tribal council, not a band
    "nisga'a village of gitlaxt": "Nisga'a Village of New Aiyansh",
    "nisga'a village of laxgalts": "Nisga'a Village of Gitwinksihlkw",
    "office of the wet'suwet'en": None,  # Tribal gov
    "okanagan indian band": "Okanagan",
    "osoyoos indian band": "Osoyoos",
    "scowlitz": "Sq'ewlets",  # alternate name
    "shísháll": "Sechelt",
    "shishalh": "Sechelt",
    "shuswap band": "Shuswap",
    "shxwh'a:y": "Shxw'ow'hamel",  # Maybe different — check
    "skidegate band": "Skidegate",
    "skwah": "Skwah",
    "soda creek": "Soda Creek",
    "splatsin first nation": "Splatsin",
    "stswecem'c": "Stswecem'c Xgat'tem",
    "temexw": None,  # Treaty association, not a band
    "tk'emlúps": "Tk'emlups te Secwepemc",
    "tk'emlups": "Tk'emlups te Secwepemc",
    "tobacco plains": "Tobacco Plains",
    "toosey": "Toosey",
    "tsal'alh": "Tsal'alh",
    "tsilhqot'in national": None,  # Tribal gov
    "tsq?éscen?": "Canim Lake",
    "canim lake": "Canim Lake",
    "wei wai kum": "Wei Wai Kum",
    "westbank": "Westbank",
    "xwisten": "Xwisten",
    # Round 2 — entries that failed on special chars or naming
    "alexis creek": "Alexis Creek",  # Tsideldel = Alexis Creek in NC
    "sechelt": "Sechelt",
    "skwah": "Skwah",
    "soda creek": "Soda Creek",
    "stswecem": "Canoe Creek",  # Stswecem'c Xgat'tem = Canoe Creek
    "kamloops": "Tk'emlups te Secwepemc",
    "tobacco plains": "Tobacco Plains",
    "toosey": "Toosey",
    "tsal'alh": "Seton Lake",  # Tsal'alh = Seton Lake
    "canim lake": "Canim Lake",
    "nadleh": "Nadleh Whut'en",
    "k'omoks": "K'omoks",
    "adams lake": "Adams Lake",
    "akisq'nuk": "?Akisq'nuk",
    "k'omoks": "K'omoks",
    "leq'": "Leq'a:mel",
    "little shuswap": "Skwlax te Secwepemculecw",
    "lower kootenay": "Lower Kootenay",
    "nadleh whut'en": "Nadleh Whut'en",
    "okanagan indian": "Okanagan",
    "osoyoos indian": "Osoyoos",
    "ktunaxa": None,  # Tribal gov
    "naut'sa mawt": None,  # Tribal council
    "office of the wet'suwet'en": None,  # Tribal gov
}


def make_jwt() -> str:
    now = int(time.time())
    return jwt.encode(
        {"sub": "enrichment-script", "iat": now, "exp": now + 3600},
        JWT_SECRET, algorithm="HS256",
    )


def normalize(name: str) -> str:
    name = re.sub(r"\s*\(.*?\)", "", name)
    name = name.lstrip("?").strip()
    return " ".join(name.split()).lower()


def main():
    # Load CivicInfo data
    civic_all = json.load(open(DATA_DIR / "civicinfo_bc_all.json"))
    import_results = json.load(open(DATA_DIR / "civicinfo_bc_import_results.json"))
    unmatched_names = set(import_results.get("unmatched_names", []))

    # Get unmatched entries with their emails
    unmatched = []
    for e in civic_all:
        if e["name"] in unmatched_names and e.get("email"):
            unmatched.append(e)
    print(f"Unmatched with email: {len(unmatched)}")

    # Fetch NC BC communities
    all_nc = []
    offset = 0
    while True:
        resp = httpx.get(f"{NC_PUBLIC}/api/v1/communities?limit=100&offset={offset}", timeout=30)
        batch = resp.json()["communities"]
        all_nc.extend(batch)
        if len(batch) < 100:
            break
        offset += 100

    # Search ALL communities — some BC communities have wrong province in NC
    nc_bc = {c["name"]: c for c in all_nc}
    nc_by_norm = {normalize(name): c for name, c in nc_bc.items()}
    print(f"NC communities (all): {len(nc_bc)}")

    # Match using manual map
    matched = []
    still_unmatched = []

    for entry in unmatched:
        civic_name = entry["name"]
        civic_lower = civic_name.lower().strip()

        found = None
        # Check manual map
        for key, nc_target in MANUAL_MAP.items():
            if key in civic_lower:
                if nc_target is None:
                    # Explicitly skip (tribal council/gov, not a band)
                    break
                # Find NC community matching target
                for nc_name, nc in nc_bc.items():
                    if nc_target.lower() in nc_name.lower():
                        found = nc
                        break
                break

        # Fallback: try matching parenthetical alternate names more aggressively
        if not found:
            alt_match = re.search(r"\(([^)]+)\)", civic_name)
            if alt_match:
                for alt_part in alt_match.group(1).split(","):
                    alt_clean = alt_part.strip().lower()
                    if len(alt_clean) < 4:
                        continue
                    for nc_norm, nc in nc_by_norm.items():
                        if alt_clean in nc_norm or nc_norm in alt_clean:
                            found = nc
                            break
                    if found:
                        break

        if found:
            matched.append({"civic": entry, "nc": found})
        else:
            still_unmatched.append(entry)

    print(f"\nNewly matched: {len(matched)}")
    print(f"Still unmatched: {len(still_unmatched)}")

    if still_unmatched:
        print("\nStill unmatched:")
        for u in still_unmatched:
            print(f"  {u['name']}: {u['email']}")

    if not matched:
        print("\nNo new matches to import.")
        return

    # Import
    token = make_jwt()
    new_count = 0
    update_count = 0
    skip_count = 0

    print(f"\n{'[DRY RUN] ' if DRY_RUN else ''}Importing...")
    for m in matched:
        nc = m["nc"]
        email = m["civic"]["email"]

        # Check existing band office
        bo = httpx.get(f"{NC_PUBLIC}/api/v1/communities/{nc['id']}/band-office", timeout=10)
        office = {}
        if bo.status_code == 200:
            office = bo.json().get("band_office") or {}

        if office.get("email") and office["email"].lower() == email.lower():
            skip_count += 1
            continue

        action = "UPDATE" if office.get("email") else "NEW"

        if DRY_RUN:
            print(f"  [{action}] {nc['name']} <- {m['civic']['name']}: {email}")
        else:
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            body = {"email": email, "data_source": "civicinfo_bc"}
            if office:
                for f in ["address_line1", "city", "province", "postal_code", "phone", "fax", "toll_free", "office_hours"]:
                    if office.get(f):
                        body[f] = office[f]
                if office.get("data_source") == "isc_profile":
                    body["data_source"] = "isc_profile"

            resp = httpx.post(
                f"{NC_WRITE}/api/v1/communities/{nc['id']}/band-office",
                json=body, headers=headers, timeout=10,
            )
            if resp.status_code == 200:
                print(f"  [{action}] {nc['name']} <- {m['civic']['name']}: {email}")
            else:
                print(f"  [ERROR] {nc['name']}: {resp.status_code}")
            time.sleep(0.1)

        if action == "NEW":
            new_count += 1
        else:
            update_count += 1

    print(f"\nDone{'  [DRY RUN]' if DRY_RUN else ''}:")
    print(f"  New: {new_count}, Updated: {update_count}, Skipped: {skip_count}")


if __name__ == "__main__":
    main()
