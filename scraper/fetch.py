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
LOOKBACK_DAYS = 90
SOURCE_NAME = f"{COUNTY_NAME} County, {STATE}"
DATA_DIR = Path("data")
DASHBOARD_DIR = Path("dashboard")

# URLs - Restoring all working sources
TLN_BASE = "https://www.toledolegalnews.com"
TLN_CP = f"{TLN_BASE}/courts/common_pleas/"
TLN_PROBATE = f"{TLN_BASE}/courts/probate/"
TLN_FORECLOSURE = f"{TLN_BASE}/legal_notices/foreclosures/"
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

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
# THE "MAGIC BUTTON" - ArcGIS Address Lookup
# ==========================================
def enrich_address_via_arcgis(owner_name: str) -> Optional[dict]:
    if not owner_name or len(owner_name) < 3: return None
    search_name = owner_name.upper().replace(",", "").strip()
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
# RESTORED SCRAPERS (TLN & Others)
# ==========================================
async def fetch_url(page, url):
    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        return await page.content()
    except Exception as e:
        logger.error(f"Failed to load {url}: {e}")
        return None

async def scrape_tln_sources(page):
    leads = []
    # 1. Probate
    html = await fetch_url(page, TLN_PROBATE)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        for text in soup.stripped_strings:
            if "Estate of" in text:
                name = text.replace("Estate of", "").strip()
                leads.append(LeadRecord(owner=name, doc_type="Probate", flags=["Inherited"], clerk_url=TLN_PROBATE))

    # 2. Foreclosures
    html = await fetch_url(page, TLN_FORECLOSURE)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            if "ci" in a['href'].lower():
                leads.append(LeadRecord(doc_type="Foreclosure", flags=["Pre-foreclosure"], clerk_url=a['href']))
    
    return leads

# ==========================================
# SAFE OUTPUT LOGIC (The Safety Switch)
# ==========================================
def safe_save_json(leads: List[LeadRecord]):
    DASHBOARD_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    
    # SAFETY SWITCH: If no leads found, DO NOT OVERWRITE EXISTING DATA
    if not leads:
        logger.warning("🚨 NO LEADS FOUND! To prevent data loss, I am NOT overwriting your records.json.")
        return

    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "total": len(leads),
        "with_address": len([l for l in leads if l.prop_address]),
        "records": [asdict(l) for l in leads]
    }
    
    with open(DASHBOARD_DIR / "records.json", "w") as f:
        json.dump(output, f, indent=2)
    with open(DATA_DIR / "records.json", "w") as f:
        json.dump(output, f, indent=2)
    logger.info(f"✅ Successfully saved {len(leads)} leads.")

def export_to_ghl_csv(leads: List[LeadRecord]):
    if not leads: return
    DATA_DIR.mkdir(exist_ok=True)
    file_path = DATA_DIR / "ghl_export.csv"
    fields = ["First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
              "Property Address", "Property City", "Property State", "Property Zip", "Lead Type", "Document Number", "Seller Score", "Motivated Seller Flags"]
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for l in leads:
            name_parts = l.owner.split(" ", 1)
            writer.writerow({
                "First Name": name_parts[0] if name_parts else "",
                "Last Name": name_parts[1] if len(name_parts)>1 else "",
                "Mailing Address": l.mail_address, "Mailing City": l.mail_city, "Mailing State": l.mail_state, "Mailing Zip": l.mail_zip,
                "Property Address": l.prop_address, "Property City": l.prop_city, "Property State": l.prop_state, "Property Zip": l.prop_zip,
                "Lead Type": l.doc_type, "Document Number": l.doc_num, "Seller Score": l.score, "Motivated Seller Flags": ", ".join(l.flags)
            })

# ==========================================
# MAIN EXECUTION
# ==========================================
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        logger.info("Starting multi-source scrape...")
        raw_leads = await scrape_tln_sources(page)
        
        final_leads = []
        for lead in raw_leads:
            addr = enrich_address_via_arcgis(lead.owner)
            if addr:
                lead.prop_address, lead.prop_city, lead.prop_zip = addr['prop_address'], addr['prop_city'], addr['prop_zip']
                lead.mail_address, lead.mail_city, lead.mail_zip = addr['mail_address'], addr['mail_city'], addr['mail_zip']
                lead.parcel_id = addr['parcel_id']
            
            lead.score = 30 + (len(lead.flags)*10) # Basic scoring
            final_leads.append(lead)
        
        await browser.close()
        
        # USE THE SAFE SAVE
        safe_save_json(final_leads)
        export_to_ghl_csv(final_leads)

if __name__ == "__main__":
    asyncio.run(main())
