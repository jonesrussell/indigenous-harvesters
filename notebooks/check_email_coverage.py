"""Check email coverage across all NC communities.

Uses SSH tunnel (localhost:18050) to avoid public API rate limits.
Requires: ssh -f -N -L 18050:localhost:8050 deployer@147.182.150.145
"""

import httpx

BASE = "http://localhost:18050"


def main():
    # Fetch all communities via local tunnel
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

        total = len(all_nc)
        with_email = 0
        by_province: dict[str, dict] = {}

        for i, c in enumerate(all_nc):
            prov = c.get("province", "??")
            if prov not in by_province:
                by_province[prov] = {"total": 0, "email": 0}
            by_province[prov]["total"] += 1

            cid = c["id"]
            try:
                bo = client.get(f"{BASE}/api/v1/communities/{cid}/band-office")
                if bo.status_code == 200:
                    office = bo.json().get("band_office") or {}
                    if office.get("email"):
                        with_email += 1
                        by_province[prov]["email"] += 1
            except httpx.ReadTimeout:
                pass

            if (i + 1) % 100 == 0:
                print(f"  Checked {i + 1}/{total}...")

    print(f"\nTotal communities: {total}")
    print(f"With email: {with_email} ({100 * with_email / total:.0f}%)")
    print(f"Missing email: {total - with_email}")
    print(f"\nBy province:")
    for p in sorted(by_province.keys()):
        d = by_province[p]
        pct = 100 * d["email"] / d["total"] if d["total"] else 0
        gap = d["total"] - d["email"]
        print(f"  {p}: {d['email']}/{d['total']} ({pct:.0f}%) — {gap} missing")


if __name__ == "__main__":
    main()
