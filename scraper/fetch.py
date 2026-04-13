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
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

# Extreme Blacklist for Names
FORBIDDEN_WORDS = ["department", "office", "public record", "government", "bureau", "commission", "court", "municipal", "administrator", "executor", "fiduciary", "estates", "payment", "registration", "rental", "exemption", "valuation", "board", "hours", "meeting", "archive", "link", "menu", "share", "noneman"]

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
# THE "DETECTIVE" ENGINE
# ==========================================

def clean_text(text: str) -> str:
    """Deep clean for HTML artifacts."""
    if not text: return ""
    text = text.replace("\u00a0", " ").replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def lookup_address_by_string(address_string: str) -> Optional[dict]:
    """
    THE OUT-OF-THE-BOX MOVE: 
    Search the API by the address found in the text, NOT the name.
    """
    if not address_string or len(address_string) < 5: return None
    
    # Clean the address for the API query
    clean_addr = address_string.upper().replace(".", "").replace(",", "")
    
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
                "prop_address": attr.get("SITEADDRESS", ""),
                "prop_city": attr.get("SITECITY", ""),
                "prop_zip": attr.get("SITEZIP", ""),
                "owner": attr.get("OWNER1", ""), # API tells us the REAL owner
                "mail_address": attr.get("MAILADDRESS", ""),
                "mail_city": attr.get("MAILCITY", ""),
                "mail_zip": attr.get("MAILZIP", ""),
                "parcel_id": attr.get("PARCELID", ""),
            }
    except: pass
    return None

def lookup_address_by_name(raw_name: str) -> Optional[dict]:
    """Fallback: Search by Name if no address was found in text."""
    name = re.sub(r"(?i),?\s*(Defendants|Defendant|et al|Estate of)", "", raw_name).strip()
    params = {
        "where": f"COUNTY='{COUNTY_NAME.upper()}' AND OWNER1 LIKE '%{name.upper()}%'",
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
# DEEP SCRAPER
# ==========================================
async def scrape_tln_detective(page):
    leads = []
    sources = [(TLN_FORECLOSURE, "Foreclosure", ["Pre-foreclosure"]), (TLN_PROBATE, "Probate", ["Inherited"])]
    
    for url, dtype, flags in sources:
        logger.info(f"Detective Mode: Scanning {dtype}...")
        try:
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            soup = BeautifulSoup(await page.content(), "html.parser")
            links = [a['href'] for a in soup.find_all("a", href=True) if "article" in a['href'].lower() and "facebook" not in a['href'].lower()]
            
            for link in links[:150]:
                try:
                    full_url = link if link.startswith("http") else TLN_BASE + link
                    await page.goto(full_url, timeout=30000, wait_until="domcontentloaded")
                    text = clean_text(await page.inner_text("body"))
                    
                    # 1. HARVEST: Find any string that looks like an address
                    # Matches "123 Main St, Toledo, OH 43601" or "123 Main St, Toledo"
                    addr_match = re.search(r"(\d+\s+[A-Z][a-zA-Z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way)[^,\n]*,?\s*[A-Za-z\s]+,?\s*[A-Z]{0,2}\s*\d{0,5})", text)
                    found_addr_string = addr_match.group(1) if addr_match else ""

                    # 2. HARVEST: Find the Name
                    owner = ""
                    for pat in [r"vs\.?\s+([A-Z][a-zA-Z\s\.,]{3,40})", r"Defendant\s*[:\s]+([A-Z][a-zA-Z\s\.,]{3,40})", r"Estate\s+of\s+([A-Z][a-zA-Z\s\.,]{3,40})"]:
                        m = re.search(pat, text, re.I)
                        if m: owner = m.group(1).strip(); break
                    
                    # 3. VALIDATE: Use the Address-First Logic
                    final_owner = owner
                    final_addr = {}

                    if found_addr_string:
                        # If we found an address, get the OFFICIAL data from API
                        api_data = lookup_address_by_string(found_addr_string)
                        if api_data:
                            final_addr = api_data
                            # Use the API's owner name if it's a better match than the "Unknown Heirs" text
                            if not owner or "unknown" in owner.lower():
                                final_owner = api_data['owner']
                    
                    # Fallback: If no address in text, try searching by name
                    if not final_addr and owner:
                        api_data = lookup_address_by_name(owner)
                        if api_data:
                            final_addr = api_data

                    if final_owner or final_addr:
                        # Filter out garbage owners
                        if final_owner and any(w in final_owner.lower() for w in FORBIDDEN_WORDS):
                            continue
                            
                        lead = LeadRecord(
                            doc_type=dtype, 
                            owner=final_owner or "Unknown", 
                            clerk_url=full_url, 
                            flags=flags
                        )
                        if final_addr:
                            lead.prop_address = final_addr['prop_address']
                            lead.prop_city = final_addr['prop_city']
                            lead.prop_zip = final_addr['prop_zip']
                            lead.mail_address = final_addr['mail_address']
                            lead.mail_city = final_addr['mail_city']
                            lead.mail_zip = final_addr['mail_zip']
                            lead.parcel_id = final_addr['parcel_id']
                        
                        leads.append(lead)
                        if final_addr:
                            logger.info(f"🎯 MATCH: {final_owner} -> {lead.prop_address}")
                except: pass
        except Exception as e: logger.error(f"Error: {e}")
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

    merged = {r['clerk_url']: r for r in existing_records if r.get('clerk_url')}
    for nl in new_leads:
        merged[nl.clerk_url] = asdict(nl)
    
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
        leads = await scrape_tln_detective(page)
        await browser.close()
        merge_and_save(leads)

if __name__ == "__main__":
    asyncio.run(main())
