import asyncio
import json
import csv
import logging
import re
import requests
from datetime import datetime, timezone, timedelta
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

# URLs - Updated for better stability
TLN_BASE = "https://www.toledolegalnews.com"
TLN_PROBATE = f"{TLN_BASE}/courts/probate/"
TLN_FORECLOSURE = f"{TLN_BASE}/legal_notices/foreclosures/"
# Using .gov or .co.lucas.oh.us is more stable than .org
SHERIFF_URL = "https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=USER&zmethod=CALENDAR"
TAX_URL = "https://co.lucas.oh.us/2949/Forfeited-Land-Sale"
CODE_URL = "https://toledo.oh.gov/residents/neighborhoods/revitalization/vacant-lots-buildings"
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

# Trash Filter
BLACKLIST = ["agreed", "required", "filed as", "notice", "state of", "bureau", "answer within", "claims to have", "unknown heirs", "devisees", "civil order", "protection"]

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
# HELPERS
# ==========================================
def is_trash_name(name: str) -> bool:
    if not name: return True
    name_low = name.lower()
    if any(word in name_low for word in BLACKLIST): return True
    if len(name.split()) > 6: return True 
    return False

def enrich_address_via_arcgis(owner_name: str) -> Optional[dict]:
    if not owner_name or is_trash_name(owner_name): return None
    search_name = owner_name.upper().replace("ESTATE OF", "").replace(",", "").strip()
    params = {
        "where": f"COUNTY='{COUNTY_NAME.upper()}' AND OWNER1 LIKE '%{search_name}%'",
        "outFields": "OWNER1,SITEADDRESS,SITECITY,SITEZIP,PARCELID,MAILADDRESS,MAILCITY,MAILZIP",
        "returnGeometry": "false", "f": "json", "resultRecordCount": 1
    }
    try:
        response = requests.get(ARCGIS_API_URL, params=params, timeout=10)
        data = response.json()
        features = data.get("features", [])
        if features:
            attr = features[0]["attributes"]
            return {
                "prop_address": attr.get("SITEADDRESS", ""),
                "prop_city": attr.get("SITECITY", ""),
                "prop_zip": attr.get("SITEZIP", ""),
                "mail_address": attr.get("MAILADDRESS", ""),
                "mail_city": attr.get("MAILCITY", ""),
                "mail_zip": attr.get("MAILZIP", ""),
                "parcel_id": attr.get("PARCELID", ""),
            }
    except: pass
    return None

# ==========================================
# SCRAPERS
# ==========================================

# 1. DEEP TLN SCRAPER (Using Playwright)
async def scrape_tln_deep(page):
    leads = []
    sources = [(TLN_FORECLOSURE, "Foreclosure", ["Pre-foreclosure"]), (TLN_PROBATE, "Probate", ["Inherited"])]
    for url, dtype, flags in sources:
        logger.info(f"Diving deep into {dtype}...")
        try:
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            links = [a['href'] for a in soup.find_all("a", href=True) if "article" in a['href'].lower() and "facebook" not in a['href'].lower()]
            for link in links[:40]:
                try:
                    full_url = link if link.startswith("http") else TLN_BASE + link
                    await page.goto(full_url, timeout=30000, wait_until="domcontentloaded")
                    text = await page.inner_text("body")
                    
                    # Extract Name
                    owner = ""
                    for pat in [r"vs\.?\s+([A-Z][a-zA-Z\s\.,]{3,40})", r"Defendant\s*[:\s]+([A-Z][a-zA-Z\s\.,]{3,40})"]:
                        m = re.search(pat, text, re.I)
                        if m: owner = m.group(1).strip(); break
                    
                    if owner and not is_trash_name(owner):
                        lead = LeadRecord(doc_type=dtype, owner=owner, clerk_url=full_url, flags=flags)
                        addr = enrich_address_via_arcgis(owner)
                        if addr:
                            lead.prop_address, lead.prop_city, lead.prop_zip = addr['prop_address'], addr['prop_city'], addr['prop_zip']
                        leads.append(lead)
                except: pass
        except Exception as e: logger.error(f"TLN Error: {e}")
    return leads

# 2. STATIC SCRAPER (Using Requests - Much more stable for Gov sites)
def scrape_static_source(url, dtype, flags):
    leads = []
    logger.info(f"Scanning {dtype} via static request...")
    try:
        response = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(" ")
        
        # Simple Regex to find Names in lists
        names = re.findall(r"([A-Z][a-z]+ [A-Z][a-z]+(?: [A-Z][a-z]+)?)", text)
        for name in list(set(names))[:30]:
            if not is_trash_name(name):
                lead = LeadRecord(doc_type=dtype, owner=name, flags=flags)
                addr = enrich_address_via_arcgis(name)
                if addr:
                    lead.prop_address, lead.prop_city, lead.prop_zip = addr['prop_address'], addr['prop_city'], addr['prop_zip']
                leads.append(lead)
    except Exception as e: logger.error(f"Static Error {dtype}: {e}")
    return leads

# ==========================================
# MAIN & OUTPUT
# ==========================================
def safe_save(leads: List[LeadRecord]):
    if not leads:
        logger.warning("🚨 No leads found. Keeping old data.")
        return
    DASHBOARD_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    output = {"fetched_at": datetime.now(timezone.utc).isoformat(), "source": SOURCE_NAME, "total": len(leads), 
              "with_address": len([l for l in leads if l.prop_address]), "records": [asdict(l) for l in leads]}
    with open(DASHBOARD_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    with open(DATA_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        all_leads = []
        # HYBRID APPROACH
        all_leads += await scrape_tln_deep(page) # Deep Dive (Playwright)
        all_leads += scrape_static_source(SHERIFF_URL, "Sheriff Sale", ["Sheriff Sale"]) # Static (Requests)
        all_leads += scrape_static_source(TAX_URL, "Tax Delinquent", ["Tax Lien"]) # Static (Requests)
        all_leads += scrape_static_source(CODE_URL, "Code Violation", ["Nuisance"]) # Static (Requests)
        
        await browser.close()
        safe_save(all_leads)
        logger.info(f"DONE: Processed {len(all_leads)} clean leads.")

if __name__ == "__main__":
    asyncio.run(main())
