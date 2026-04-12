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
PARCEL_INDEX_PATH = DATA_DIR / "parcel_index.csv"

TLN_BASE = "https://www.toledolegalnews.com"
TLN_PROBATE = f"{TLN_BASE}/courts/probate/"
TLN_FORECLOSURE = f"{TLN_BASE}/legal_notices/foreclosures/"
TAX_URL = "https://co.lucas.oh.us/2949/Forfeited-Land-Sale"
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

# EXTREME BLACKLIST
FORBIDDEN_WORDS = [
    "noneman", "department", "office", "public record", "government", "bureau", 
    "commission", "court", "municipal", "administrator", "executor", "fiduciary", 
    "estates", "payment", "registration", "rental", "exemption", "valuation", 
    "board", "hours", "meeting", "archive", "link", "menu", "share"
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
# SURGICAL CLEANING & LOOKUP
# ==========================================
def surgical_clean(text: str) -> str:
    if not text: return ""
    text = text.replace("\u00a0", " ").replace("\n", " ").replace("\r", " ")
    text = re.sub(r"(?i),?\s*(Defendants|Defendant|et al|Estate of|whose last place|as Administrator|executor of)", "", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s]", "", text).strip()
    return text

def is_valid_person(name: str) -> bool:
    if not name or len(name) < 3: return False
    name_low = name.lower()
    if any(word in name_low for word in FORBIDDEN_WORDS): return False
    if len(name.split()) > 6: return False 
    return True

def lookup_address_by_parcel(parcel_id: str) -> Optional[dict]:
    """100% Accurate lookup using Parcel ID."""
    if not parcel_id: return None
    params = {
        "where": f"PARCELID = '{parcel_id}'",
        "outFields": "OWNER1,SITEADDRESS,SITECITY,SITEZIP,PARCELID,MAILADDRESS,MAILCITY,MAILZIP",
        "returnGeometry": "false", "f": "json", "resultRecordCount": 1
    }
    try:
        r = requests.get(ARCGIS_API_URL, params=params, timeout=10).json()
        if r.get('features'):
            attr = r['features'][0]['attributes']
            return {
                "prop_address": attr.get("SITEADDRESS", ""), "prop_city": attr.get("SITECITY", ""),
                "prop_zip": attr.get("SITEZIP", ""), "mail_address": attr.get("MAILADDRESS", ""),
                "mail_city": attr.get("MAILCITY", ""), "mail_zip": attr.get("MAILZIP", ""),
                "parcel_id": attr.get("PARCELID", ""),
            }
    except: pass
    return None

def lookup_address_by_name(raw_name: str) -> Optional[dict]:
    """Fuzzy lookup using Owner Name."""
    clean_name = surgical_clean(raw_name)
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
                "prop_zip": attr.get("SITEZIP", ""), "mail_address": attr.get("MAILADDRESS", ""),
                "mail_city": attr.get("MAILCITY", ""), "mail_zip": attr.get("MAILZIP", ""),
                "parcel_id": attr.get("PARCELID", ""),
            }
    except: pass
    return None

# ==========================================
# TAX DELINQUENT SCRAPER (Workaround)
# ==========================================
def scrape_tax_delinquent():
    leads = []
    logger.info("Scanning Tax Delinquent Forfeited Land list...")
    try:
        # Use a common browser header to avoid blocks
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(TAX_URL, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Search for tables on the page
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]: # Skip header
                cells = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cells) >= 2:
                    owner = cells[0]
                    # Try to find address in remaining cells
                    address = " ".join(cells[1:])
                    
                    if is_valid_person(owner):
                        lead = LeadRecord(doc_type="Tax Delinquent", owner=owner, prop_address=address, flags=["Tax Lien", "Hot Stack"])
                        # Try to refine address via API
                        addr_data = lookup_address_by_name(owner)
                        if addr_data:
                            lead.prop_address = addr_data['prop_address']
                            lead.prop_city = addr_data['prop_city']
                            lead.prop_zip = addr_data['prop_zip']
                        leads.append(lead)
    except Exception as e: logger.error(f"Tax Scraping Error: {e}")
    return leads

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
            soup = BeautifulSoup(await page.content(), "html.parser")
            links = [a['href'] for a in soup.find_all("a", href=True) if "article" in a['href'].lower() and "facebook" not in a['href'].lower()]
            
            for link in links[:150]:
                try:
                    full_url = link if link.startswith("http") else TLN_BASE + link
                    await page.goto(full_url, timeout=30000, wait_until="domcontentloaded")
                    text = await page.inner_text("body")
                    
                    # 1. Look for Parcel ID first (The Gold Standard)
                    parcel_match = re.search(r"(\d{2}-\d{5,6}-\d{1}-\d{3})", text)
                    parcel_id = parcel_match.group(1) if parcel_match else ""
                    
                    # 2. Extract Owner
                    owner = ""
                    for pat in [r"vs\.?\s+([A-Z][a-zA-Z\s\.,]{3,40})", r"Defendant\s*[:\s]+([A-Z][a-zA-Z\s\.,]{3,40})", r"Estate\s+of\s+([A-Z][a-zA-Z\s\.,]{3,40})"]:
                        m = re.search(pat, text, re.I)
                        if m: owner = m.group(1).strip(); break
                    
                    if owner or parcel_id:
                        lead = LeadRecord(doc_type=dtype, owner=owner or "Unknown", clerk_url=link, flags=flags, parcel_id=parcel_id)
                        
                        # Address Hierarchy: Parcel ID -> Text Extract -> Name API
                        addr_match = re.search(r"(\d+\s+[A-Z][a-zA-Z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way)[^,\n]*,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5})", text)
                        
                        if parcel_id:
                            addr_data = lookup_address_by_parcel(parcel_id)
                            if addr_data:
                                lead.prop_address, lead.prop_city, lead.prop_zip = addr_data['prop_address'], addr_data['prop_city'], addr_data['prop_zip']
                        
                        if not lead.prop_address and addr_match:
                            lead.prop_address = addr_match.group(1)
                        
                        if not lead.prop_address and owner:
                            addr_data = lookup_address_by_name(owner)
                            if addr_data:
                                lead.prop_address, lead.prop_city, lead.prop_zip = addr_data['prop_address'], addr_data['prop_city'], addr_data['prop_zip']
                        
                        leads.append(lead)
                except: pass
        except Exception as e: logger.error(f"TLN Error: {e}")
    return leads

# ==========================================
# SAVE & MERGE
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

    merged = {r['clerk_url'] if r.get('clerk_url') else f"static_{i}": r for i, r in enumerate(existing_records)}
    for nl in new_leads:
        key = nl.clerk_url if nl.clerk_url else f"static_{datetime.now().timestamp()}"
        merged[key] = asdict(nl)
    
    final_list = list(merged.values())
    output = {"fetched_at": datetime.now(timezone.utc).isoformat(), "source": SOURCE_NAME, "total": len(final_list), 
              "with_address": len([r for r in final_list if r.get('prop_address')]), "records": final_list}
    with open(DASHBOARD_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    with open(DATA_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    logger.info(f"✅ Merged data. Total records: {len(final_list)}")

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        logger.info("🚀 Starting ADVANCED scrape...")
        
        # 1. TLN Deep Dive
        all_leads = await scrape_tln_deep(page)
        
        # 2. Tax Delinquent static scan
        tax_leads = scrape_tax_delinquent()
        all_leads.extend(tax_leads)
        
        await browser.close()
        merge_and_save(all_leads)

if __name__ == "__main__":
    asyncio.run(main())
