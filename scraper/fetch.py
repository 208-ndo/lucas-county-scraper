import asyncio
import json
import csv
import logging
import re
import requests
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import List, Optional

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# ==========================================
# CONFIGURATION
# ==========================================
COUNTY_NAME = "Lucas"
STATE = "OH"
SOURCE_NAME = f"{COUNTY_NAME} County, {STATE}"
DATA_DIR = Path("data")
DASHBOARD_DIR = Path("dashboard")

TLN_BASE = "https://www.toledolegalnews.com"
TLN_PROBATE = f"{TLN_BASE}/courts/probate/"
TLN_FORECLOSURE = f"{TLN_BASE}/legal_notices/foreclosures/"
TAX_URL = "https://co.lucas.oh.us/2949/Forfeited-Land-Sale"
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

# THE "NUCLEAR" BLACKLIST - If these appear, DELETE the lead immediately.
FORBIDDEN_WORDS = [
    "noneman", "department", "office", "public record", "government", "bureau", 
    "commission", "court", "municipal", "administrator", "executor", "fiduciary", 
    "estates", "payment", "registration", "rental", "exemption", "valuation", 
    "board", "hours", "meeting", "archive", "link", "menu", "share", "facebook", "twitter"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

@dataclass
class LeadRecord:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    owner: str = ""
    amount: float = 0.0
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = STATE
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = STATE
    mail_zip: str = ""
    clerk_url: str = ""
    parcel_id: str = ""
    flags: List[str] = field(default_factory=list)
    score: int = 0

# ==========================================
# SURGICAL TOOLS
# ==========================================
def surgical_name_clean(name: str) -> str:
    """Cuts off everything after the name (e.g., 'John Doe, Defendants To' -> 'John Doe')."""
    if not name: return ""
    # 1. Split by common legal delimiters and take the first part
    name = re.split(r"(?i),| Defendants| Defendant| et al| Estate of| whose last place", name)[0]
    # 2. Remove non-alphanumeric characters except spaces
    name = re.sub(r"[^\w\s]", "", name).strip()
    return name

def is_valid_person(name: str) -> bool:
    if not name or len(name) < 3: return False
    name_low = name.lower()
    if any(word in name_low for word in FORBIDDEN_WORDS): return False
    if len(name.split()) > 5: return False 
    return True

def api_lookup_by_address(address_string: str) -> Optional[dict]:
    """The Gold Standard: Find owner and parcel data using the address."""
    if not address_string: return None
    clean_addr = address_string.upper().replace(".", "").replace(",", "").strip()
    params = {
        "where": f"SITEADDRESS LIKE '%{clean_addr}%' AND COUNTY='{COUNTY_NAME.upper()}'",
        "outFields": "OWNER1,SITEADDRESS,SITECITY,SITEZIP,PARCELID,MAILADDRESS,MAILCITY,MAILZIP",
        "returnGeometry": "false", "f": "json", "resultRecordCount": 1
    }
    try:
        r = requests.get(ARCGIS_API_URL, params=params, timeout=10).json()
        if r.get('features'):
            attr = r['features'][0]['attributes']
            return {
                "prop_address": attr.get("SITEADDRESS", ""), "prop_city": attr.get("SITECITY", ""),
                "prop_zip": attr.get("SITEZIP", ""), "owner": attr.get("OWNER1", ""),
                "mail_address": attr.get("MAILADDRESS", ""), "mail_city": attr.get("MAILCITY", ""),
                "mail_zip": attr.get("MAILZIP", ""), "parcel_id": attr.get("PARCELID", ""),
            }
    except: pass
    return None

def api_lookup_by_name(raw_name: str) -> Optional[dict]:
    """Fallback: Search by Name if no address was found in text."""
    clean_name = surgical_name_clean(raw_name)
    if not is_valid_person(clean_name): return None
    params = {
        "where": f"COUNTY='{COUNTY_NAME.upper()}' AND OWNER1 LIKE '%{clean_name.upper()}%'",
        "outFields": "OWNER1,SITEADDRESS,SITECITY,SITEZIP,PARCELID,MAILADDRESS,MAILCITY,MAILZIP",
        "returnGeometry": "false", "f": "json", "resultRecordCount": 1
    }
    try:
        r = requests.get(ARCGIS_API_URL, params=params, timeout=10).json()
        if r.get('features'):
            attr = r['features'][0]['attributes']
            return {
                "prop_address": attr.get("SITEADDRESS", ""), "prop_city": attr.get("SITECITY", ""),
                "prop_zip": attr.get("SITEZIP", ""), "owner": attr.get("OWNER1", ""),
                "mail_address": attr.get("MAILADDRESS", ""), "mail_city": attr.get("MAILCITY", ""),
                "mail_zip": attr.get("MAILZIP", ""), "parcel_id": attr.get("PARCELID", ""),
            }
    except: pass
    return None

# ==========================================
# SCRAPERS
# ==========================================
async def scrape_tln_deep(page):
    leads = []
    sources = [(TLN_FORECLOSURE, "Foreclosure", ["Pre-foreclosure"]), (TLN_PROBATE, "Probate", ["Inherited"])]
    for url, dtype, flags in sources:
        logger.info(f"Deep Scanning {dtype}...")
        try:
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            soup = BeautifulSoup(await page.content(), "html.parser")
            links = [a['href'] for a in soup.find_all("a", href=True) if "article" in a['href'].lower() and "facebook" not in a['href'].lower()]
            
            for link in links[:150]:
                try:
                    full_url = link if link.startswith("http") else TLN_BASE + link
                    await page.goto(full_url, timeout=30000, wait_until="domcontentloaded")
                    text = await page.inner_text("body")
                    
                    # 1. FIND ADDRESS IN TEXT FIRST
                    addr_match = re.search(r"(\d+\s+[A-Z][a-zA-Z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way)[^,\n]*,\s*[A-Za-z\s]+,?\s*[A-Z]{0,2}\s*\d{0,5})", text)
                    found_addr = addr_match.group(1) if addr_match else ""

                    # 2. FIND NAME
                    owner = ""
                    for pat in [r"vs\.?\s+([A-Z][a-zA-Z\s\.,]{3,40})", r"Defendant\s*[:\s]+([A-Z][a-zA-Z\s\.,]{3,40})", r"Estate\s+of\s+([A-Z][a-zA-Z\s\.,]{3,40})"]:
                        m = re.search(pat, text, re.I)
                        if m: owner = m.group(1).strip(); break
                    
                    # 3. RESOLVE via API
                    final_owner = owner
                    final_data = {}

                    if found_addr:
                        res = api_lookup_by_address(found_addr)
                        if res:
                            final_data = res
                            final_owner = res['owner'] # Use Official Name
                    
                    if not final_data and owner:
                        res = api_lookup_by_name(owner)
                        if res: final_data = res

                    if final_owner and is_valid_person(surgical_name_clean(final_owner)):
                        lead = LeadRecord(doc_type=dtype, owner=surgical_name_clean(final_owner), clerk_url=full_url, flags=flags)
                        if final_data:
                            lead.prop_address = final_data['prop_address']
                            lead.prop_city = final_data['prop_city']
                            lead.prop_zip = final_data['prop_zip']
                            lead.mail_address = final_data['mail_address']
                            lead.mail_city = final_data['mail_city']
                            lead.mail_zip = final_data['mail_zip']
                            lead.parcel_id = final_data['parcel_id']
                        leads.append(lead)
                except: pass
        except Exception as e: logger.error(f"TLN Error: {e}")
    return leads

def scrape_tax_delinquent():
    leads = []
    logger.info("Scanning Tax Delinquent List...")
    try:
        r = requests.get(TAX_URL, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        for table in soup.find_all("table"):
            for row in table.find_all("tr")[1:]:
                cells = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cells) >= 2:
                    owner = cells[0]
                    if is_valid_person(owner):
                        lead = LeadRecord(doc_type="Tax Delinquent", owner=owner, flags=["Tax Lien", "Hot Stack"])
                        res = api_lookup_by_name(owner)
                        if res:
                            lead.prop_address, lead.prop_city, lead.prop_zip = res['prop_address'], res['prop_city'], res['prop_zip']
                        leads.append(lead)
    except: pass
    return leads

# ==========================================
# SAVE & MERGE
# ==========================================
def merge_and_save(new_leads: List[LeadRecord]):
    DASHBOARD_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    existing = []
    if (DATA_DIR / "records.json").exists():
        try:
            with open(DATA_DIR / "records.json", "r") as f:
                existing = json.load(f).get("records", [])
        except: pass

    merged = {r['clerk_url'] if r.get('clerk_url') else f"static_{i}": r for i, r in enumerate(existing)}
    for nl in new_leads:
        merged[nl.clerk_url if nl.clerk_url else f"static_{datetime.now().timestamp()}"] = asdict(nl)
    
    final_list = list(merged.values())
    output = {"fetched_at": datetime.now(timezone.utc).isoformat(), "source": SOURCE_NAME, "total": len(final_list), 
              "with_address": len([r for r in final_list if r.get('prop_address')]), "records": final_list}
    with open(DASHBOARD_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    with open(DATA_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    logger.info(f"✅ Total records preserved: {len(final_list)}")

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        logger.info("🚀 Starting DETECTIVE scrape...")
        all_leads = await scrape_tln_deep(page)
        all_leads += scrape_tax_delinquent()
        
        await browser.close()
        merge_and_save(all_leads)

if __name__ == "__main__":
    asyncio.run(main())
