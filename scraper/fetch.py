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
PARCEL_INDEX_PATH = DATA_DIR / "parcel_index.csv"

TLN_BASE = "https://www.toledolegalnews.com"
TLN_PROBATE = f"{TLN_BASE}/courts/probate/"
TLN_FORECLOSURE = f"{TLN_BASE}/legal_notices/foreclosures/"
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

# EXTREME BLACKLIST - If these words appear, it's NOT a human owner.
FORBIDDEN_WORDS = [
    "department", "office", "public record", "record request", "government", 
    "bureau", "commission", "court", "municipal", "administrator", "executor", 
    "fiduciary", "estates", "payment", "registration", "rental", "exemption",
    "valuation", "board", "hours", "meeting", "archive", "link", "menu", "share",
    "defendant", "et al", "estate of"
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
# SURGICAL CLEANING & ADDRESS LOOKUP
# ==========================================
def clean_owner_name(name: str) -> str:
    """Strips legal jargon so the API can actually find the person."""
    if not name: return ""
    # Remove "Defendants", "et al", "Estate of" etc.
    name = re.sub(r"(?i),?\s*(Defendants|Defendant|et al|Estate of|whose last place|as Administrator|executor of)", "", name)
    # Remove punctuation and weird characters
    name = re.sub(r"[^\w\s]", "", name).strip()
    return name

def is_valid_person(name: str) -> bool:
    """Returns False if the name is actually a menu item or gov office."""
    if not name or len(name) < 3: return False
    name_low = name.lower()
    if any(word in name_low for word in FORBIDDEN_WORDS): return False
    if len(name.split()) > 6: return False 
    return True

def lookup_address(raw_name: str) -> Optional[dict]:
    """Tries Local CSV first, then ArcGIS API. NO PANDAS REQUIRED."""
    clean_name = clean_owner_name(raw_name)
    if not is_valid_person(clean_name): return None
    
    # 1. LOCAL CSV LOOKUP (Fastest)
    if PARCEL_INDEX_PATH.exists():
        try:
            with open(PARCEL_INDEX_PATH, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if clean_name.upper() in row.get('OWNER', '').upper():
                        return {
                            "prop_address": row.get('SITE_ADDR', ''),
                            "prop_city": row.get('SITECITY', ''),
                            "prop_zip": row.get('SITEZIP', ''),
                            "mail_address": row.get('MAIL_ADDR', ''),
                            "mail_city": row.get('MAILCITY', ''),
                            "mail_zip": row.get('MAILZIP', ''),
                            "parcel_id": row.get('PARCEL_ID', ''),
                        }
        except Exception as e:
            logger.debug(f"CSV Error: {e}")

    # 2. ARCGIS API BACKUP
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
            
            links = []
            for a in soup.find_all("a", href=True):
                href = a['href']
                if any(x in href.lower() for x in ["article", "case-no", "ci20"]) and \
                   not any(x in href.lower() for x in ["facebook", "twitter", "whatsapp", "email", "share"]):
                    full_url = href if href.startswith("http") else TLN_BASE + href
                    if full_url not in links: links.append(full_url)
            
            for link in links[:150]:
                try:
                    await page.goto(link, timeout=30000, wait_until="domcontentloaded")
                    text = await page.inner_text("body")
                    
                    owner = ""
                    for pat in [r"vs\.?\s+([A-Z][a-zA-Z\s\.,]{3,40})", r"Defendant\s*[:\s]+([A-Z][a-zA-Z\s\.,]{3,40})", r"Estate\s+of\s+([A-Z][a-zA-Z\s\.,]{3,40})"]:
                        m = re.search(pat, text, re.I)
                        if m: owner = m.group(1).strip(); break
                    
                    if owner and is_valid_person(owner):
                        # Try to extract address from text
                        addr_match = re.search(r"(\d+\s+[A-Z][a-zA-Z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way)[^,\n]*,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5})", text)
                        
                        lead = LeadRecord(doc_type=dtype, owner=owner, clerk_url=link, flags=flags)
                        if addr_match:
                            lead.prop_address = addr_match.group(1)
                        else:
                            # API Fallback
                            addr_data = lookup_address(owner)
                            if addr_data:
                                lead.prop_address, lead.prop_city, lead.prop_zip = addr_data['prop_address'], addr_data['prop_city'], addr_data['prop_zip']
                                lead.mail_address, lead.mail_city, lead.mail_zip = addr_data['mail_address'], addr_data['mail_city'], addr_data['mail_zip']
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
    
    existing_records = []
    records_file = DATA_DIR / "records.json"
    if records_file.exists():
        try:
            with open(records_file, "r") as f:
                old_data = json.load(f)
                existing_records = old_data.get("records", [])
        except: pass

    merged = {r['clerk_url']: r for r in existing_records if r.get('clerk_url')}
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
