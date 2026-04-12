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

TLN_BASE = "https://www.toledolegalnews.com"
TLN_PROBATE = f"{TLN_BASE}/courts/probate/"
TLN_FORECLOSURE = f"{TLN_BASE}/legal_notices/foreclosures/"
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

# EXTREME BLACKLIST - If these words appear, it's NOT a person.
FORBIDDEN_WORDS = [
    "department", "office", "public record", "record request", "government", 
    "bureau", "commission", "court", "municipal", "administrator", "executor", 
    "fiduciary", "estates", "payment", "registration", "rental", "exemption",
    "valuation", "board", "hours", "meeting", "archive", "link", "menu", "share"
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
# SURGICAL CLEANING
# ==========================================
def clean_owner_name(name: str) -> str:
    """Removes garbage from names so the API can actually find them."""
    if not name: return ""
    # Remove common legal suffixes
    name = re.sub(r"(?i),?\s*(Defendants|Defendant|et al|Estate of|whose last place|as Administrator|executor of)", "", name)
    # Remove punctuation and extra whitespace
    name = re.sub(r"[^\w\s]", "", name).strip()
    return name

def is_valid_person(name: str) -> bool:
    """Filters out the 'Ghost Leads' (menu items, office names)."""
    if not name or len(name) < 3: return False
    name_low = name.lower()
    if any(word in name_low for word in FORBIDDEN_WORDS): return False
    if len(name.split()) > 5: return False # Too long to be a human name
    return True

def enrich_address_via_arcgis(raw_name: str) -> Optional[dict]:
    clean_name = clean_owner_name(raw_name)
    if not is_valid_person(clean_name): return None
    
    params = {
        "where": f"COUNTY='{COUNTY_NAME.upper()}' AND OWNER1 LIKE '%{clean_name.upper()}%'",
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
# DEEP TLN SCRAPER
# ==========================================
async def scrape_tln_deep(page):
    leads = []
    sources = [(TLN_FORECLOSURE, "Foreclosure", ["Pre-foreclosure"]), (TLN_PROBATE, "Probate", ["Inherited"])]
    
    for url, dtype, flags in sources:
        logger.info(f"Scanning {dtype} source...")
        try:
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            
            # Find case links, ignore social media
            links = []
            for a in soup.find_all("a", href=True):
                href = a['href']
                if any(x in href.lower() for x in ["article", "case-no", "ci20"]) and \
                   not any(x in href.lower() for x in ["facebook", "twitter", "whatsapp", "email", "share"]):
                    full_url = href if href.startswith("http") else TLN_BASE + href
                    if full_url not in links: links.append(full_url)
            
            for link in links[:100]: # Be thorough
                try:
                    await page.goto(link, timeout=30000, wait_until="domcontentloaded")
                    text = await page.inner_text("body")
                    
                    # 1. Try to find the name
                    owner = ""
                    for pat in [r"vs\.?\s+([A-Z][a-zA-Z\s\.,]{3,40})", r"Defendant\s*[:\s]+([A-Z][a-zA-Z\s\.,]{3,40})", r"Estate\s+of\s+([A-Z][a-zA-Z\s\.,]{3,40})"]:
                        m = re.search(pat, text, re.I)
                        if m: owner = m.group(1).strip(); break
                    
                    if owner and is_valid_person(owner):
                        # 2. Try to find address in text first
                        addr_match = re.search(r"(\d+\s+[A-Z][a-zA-Z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way)[^,\n]*,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5})", text)
                        
                        lead = LeadRecord(doc_type=dtype, owner=owner, clerk_url=link, flags=flags)
                        if addr_match:
                            lead.prop_address = addr_match.group(1)
                        else:
                            # 3. Fallback to ArcGIS API
                            addr_data = enrich_address_via_arcgis(owner)
                            if addr_data:
                                lead.prop_address = addr_data['prop_address']
                                lead.prop_city = addr_data['prop_city']
                                lead.prop_zip = addr_data['prop_zip']
                                lead.mail_address = addr_data['mail_address']
                                lead.mail_city = addr_data['mail_city']
                                lead.mail_zip = addr_data['mail_zip']
                                lead.parcel_id = addr_data['parcel_id']
                        
                        leads.append(lead)
                except: pass
        except Exception as e: logger.error(f"TLN Error: {e}")
    return leads

# ==========================================
# DATA MERGER (STOPS THE "0 LEADS" DROP)
# ==========================================
def merge_and_save(new_leads: List[LeadRecord]):
    DASHBOARD_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    
    # Load existing data so we don't lose anything
    existing_records = []
    records_file = DATA_DIR / "records.json"
    if records_file.exists():
        try:
            with open(records_file, "r") as f:
                old_data = json.load(f)
                existing_records = old_data.get("records", [])
        except: pass

    # Merge new leads into existing ones based on Clerk URL
    merged = {r['clerk_url']: r for r in existing_records}
    for nl in new_leads:
        merged[nl.clerk_url] = asdict(nl)
    
    final_list = list(merged.values())
    
    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "total": len(final_list),
        "with_address": len([r for r in final_list if r.get('prop_address')]),
        "records": final_list
    }
    
    with open(DASHBOARD_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    with open(DATA_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    logger.info(f"✅ Merged data. Total records now: {len(final_list)}")

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        logger.info("🚀 Starting SURGICAL scrape...")
        leads = await scrape_tln_deep(page)
        
        await browser.close()
        merge_and_save(leads)

if __name__ == "__main__":
    asyncio.run(main())
