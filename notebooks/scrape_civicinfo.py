"""
Scrape CivicInfo BC using Playwright for Cloudflare bypass.
Run: .venv/bin/python notebooks/scrape_civicinfo.py

Requires: pip install playwright && playwright install chromium
"""
import json, time
from pathlib import Path
from playwright.sync_api import sync_playwright

DATA_DIR = Path(__file__).parent / "data"

JS_EXTRACT = """() => {
    var results = [];
    var items = document.querySelectorAll('li');
    for (var i = 0; i < items.length; i++) {
        var li = items[i];
        var text = li.textContent || '';
        if (text.indexOf('Ph:') === -1) continue;
        var nameLinks = li.querySelectorAll('a[href*="firstnations?id="]');
        var name = '';
        for (var k = 0; k < nameLinks.length; k++) {
            var t = nameLinks[k].textContent.trim();
            if (t.length > 3) { name = t; break; }
        }
        if (!name) continue;
        var email = '';
        var mailto = li.querySelector('a[href^="mailto:"]');
        if (mailto) email = mailto.href.replace('mailto:', '').split('?')[0];
        var website = '';
        var allLinks = li.querySelectorAll('a[href^="http"]');
        for (var j = 0; j < allLinks.length; j++) {
            if (allLinks[j].href.indexOf('civicinfo') === -1 && allLinks[j].href.indexOf('s3.ca') === -1) {
                website = allLinks[j].href;
                break;
            }
        }
        var phMatch = text.match(/Ph:\\s*([\\d\\-(). ]+)/);
        var phone = phMatch ? phMatch[1].trim() : '';
        results.push({name: name, email: email, phone: phone, website: website});
    }
    return results;
}"""

def main():
    all_entries = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # non-headless to bypass Cloudflare
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = ctx.new_page()

        for pn in range(1, 22):
            url = f"https://www.civicinfo.bc.ca/firstnations?pn={pn}" if pn > 1 else "https://www.civicinfo.bc.ca/firstnations"
            try:
                page.goto(url, timeout=60000)
                page.wait_for_selector('a[href*="firstnations?id="]', timeout=15000)
                time.sleep(2)  # let Cloudflare JS decode emails

                entries = page.evaluate(JS_EXTRACT)
                if not entries:
                    break

                all_entries.extend(entries)
                with_email = sum(1 for e in entries if e["email"])
                print(f"Page {pn}: {len(entries)} entries ({with_email} with email)")
                time.sleep(1)
            except Exception as e:
                print(f"Page {pn}: error {e}")
                break

        browser.close()

    print(f"\nTotal: {len(all_entries)}, With email: {sum(1 for e in all_entries if e['email'])}")

    out = DATA_DIR / "civicinfo_bc_all.json"
    out.write_text(json.dumps(all_entries, indent=2))
    print(f"Saved: {out}")

if __name__ == "__main__":
    main()
