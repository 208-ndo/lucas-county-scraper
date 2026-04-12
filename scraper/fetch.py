import asyncio
import json
import csv
import logging
import re
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import List, Optional

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# ==========================================
# CONFIGURATION & GOALS
# ==========================================
COUNTY_NAME = "Lucas"
STATE = "OH"
SOURCE_NAME = f"{COUNTY_NAME} County, {STATE}"
DATA_DIR = Path("data")
DASHBOARD_DIR = Path("dashboard")
PARCEL_INDEX_PATH = DATA_DIR / "parcel_index.csv"

TLN_BASE = "https://www.toledolegalnews.com"
SOURCES = {
    "Foreclosure": (f"{TLN_BASE}/legal_notices/foreclosures/", ["Pre-foreclosure", "Lis pendens"]),
    "Probate": (f"{TLN_BASE}/courts/probate/", ["Probate / estate"]),
    "Judgment": (f"{TLN_BASE}/courts/common_pleas/", ["Judgment lien"]),
    "Lien": (f"{TLN_BASE}/courts/common_pleas/", ["Lien"]),
}
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

BLACKLIST = ["department", "office", "public record", "government", "bureau", "commission", "court", "municipal"]

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
# THE ADDRESS ENGINE (The "End Goal" Logic)
# ==========================================
def lookup_address(owner_name: str) -> Optional[dict]:
    """Tries Local Bulk Index first, then ArcGIS API."""
    if not owner_name or len(owner_name) < 3: return None
    
    # Clean name for matching
    clean_name = re.sub(r"(?i),?\s*(Defendants|Defendant|et al|Estate of)", "", owner_name).strip().upper()

    # LAYER 1: LOCAL BULK INDEX (The DBF conversion)
    if PARCEL_INDEX_PATH.exists():
        try:
            df = pd.read_csv(PARCEL_INDEX_PATH)
            # Search for the name in the OWNER column
            match = df[df['OWNER'].str.contains(clean_name, na=False, case=False)].iloc[0]
            return {
                "prop_address": match.get('SITE_ADDR', ''),
                "prop_city": match.get('SITECITY', ''),
                "prop_zip": match.get('SITEZIP', ''),
                "mail_address": match.get('MAIL_ADDR', ''),
                "mail_city": match.get('MAILCITY', ''),
                "mail_zip": match.get('MAILZIP', ''),
                "parcel_id": match.get('PARCEL_ID', ''),
            }
        except Exception as e:
            logger.debug(f"Local index miss for {clean_name}: {e}")

    # LAYER 2: ARCGIS API BACKUP
    params = {
        "where": f"COUNTY='{COUNTY_NAME.upper()}' AND OWNER1 LIKE '%{clean_name}%'",
        "outFields": "OWNER1,SITEADDRESS,SITECITY,SITEZIP,PARCELID,MAILADDRESS,MAILCITY,MAILZIP",
        "returnGeometry": "false", "f": "json", "resultRecordCount": 1
    }
    try:
        r = requests.get(ARCGIS_API_URL, params=params, timeout=10).json()
        attr = r['features'][0]['attributes']
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
# THE SCORING SYSTEM (From your Prompt)
# ==========================================
def calculate_score(lead: LeadRecord):
    score = 30 # Base
    score += (len(lead.flags) * 10)
    
    # Combo Bonus
    if "Pre-foreclosure" in lead.flags and "Lis pendens" in lead.flags:
        score += 20
    
    # Amount Bonus
    if lead.amount > 100000: score += 15
    elif lead.amount > 50000: score += 10
    
    # Address Bonus
    if lead.prop_address: score += 5
    
    return min(score, 100)

# ==========================================
# SCRAPER ENGINE
# ==========================================
async def scrape_deep_tln(page):
    leads = []
    for dtype, (url, flags) in SOURCES.items():
        logger.info(f"Processing {dtype}...")
        try:
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            soup = BeautifulSoup(await page.content(), "html.parser")
            links = [a['href'] for a in soup.find_all("a", href=True) if "article" in a['href'].lower() and "facebook" not in a['href'].lower()]
            
            for link in links[:150]: # High volume
                try:
                    full_url = link if link.startswith("http") else TLN_BASE + link
                    await page.goto(full_url, timeout=30000, wait_until="domcontentloaded")
                    text = await page.inner_text("body")
                    
                    # Extract Name
                    owner = ""
                    for pat in [r"vs\.?\s+([A-Z][a-zA-Z\s\.,]{3,40})", r"Defendant\s*[:\s]+([A-Z][a-zA-Z\s\.,]{3,40})"]:
                        m = re.search(pat, text, re.I)
                        if m: owner = m.group(1).strip(); break
                    
                    if owner and not any(w in owner.lower() for w in BLACKLIST):
                        lead = LeadRecord(doc_type=dtype, owner=owner, clerk_url=full_url, flags=flags)
                        addr = lookup_address(owner)
                        if addr:
                            lead.prop_address, lead.prop_city, lead.prop_zip = addr['prop_address'], addr['prop_city'], addr['prop_zip']
                            lead.mail_address, lead.mail_city, lead.mail_zip = addr['mail_address'], addr['mail_city'], addr['mail_zip']
                            lead.parcel_id = addr['parcel_id']
                        
                        lead.score = calculate_score(lead)
                        leads.append(lead)
                except: pass
        except Exception as e: logger.error(f"Error {dtype}: {e}")
    return leads

# ==========================================
# OUTPUTS (GHL & JSON)
# ==========================================
def save_outputs(leads: List[LeadRecord]):
    DASHBOARD_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    
    # JSON
    output = {"fetched_at": datetime.now(timezone.utc).isoformat(), "source": SOURCE_NAME, 
              "total": len(leads), "with_address": len([l for l in leads if l.prop_address]), "records": [asdict(l) for l in leads]}
    with open(DASHBOARD_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    with open(DATA_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    
    # GHL CSV Export
    csv_path = DATA_DIR / "ghl_export.csv"
    fields = ["First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip", 
              "Property Address", "Property City", "Property State", "Property Zip", "Lead Type", "Document Number", "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for l in leads:
            name_parts = l.owner.split(" ", 1)
            writer.writerow({
                "First Name": name_parts[0], "Last Name": name_parts[1] if len(name_parts)>1 else "",
                "Mailing Address": l.mail_address, "Mailing City": l.mail_city, "Mailing State": l.mail_state, "Mailing Zip": l.mail_zip,
                "Property Address": l.prop_address, "Property City": l.prop_city, "Property State": l.prop_state, "Property Zip": l.prop_zip,
                "Lead Type": l.doc_type, "Document Number": l.doc_num, "Seller Score": l.score, "Motivated Seller Flags": ", ".join(l.flags),
                "Source": SOURCE_NAME, "Public Records URL": l.clerk_url
            })

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        leads = await scrape_deep_tln(page)
        await browser.close()
        save_outputs(leads)
        logger.info(f"FINAL SUCCESS: Processed {len(leads)} high-quality leads.")

if __name__ == "__main__":
    asyncio.run(main())
