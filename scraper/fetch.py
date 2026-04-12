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
SHERIFF_URL = "https://lucassheriff.org/resources/sheriffs-sales"
CODE_VIOL_URL = "https://toledo.oh.gov/residents/neighborhoods/revitalization/vacant-lots-buildings"
TAX_DELINQ_URL = "https://co.lucas.oh.us/2949/Forfeited-Land-Sale"
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

# WORDS TO DELETE (Trash Filter)
BLACKLIST = [
    "agreed", "required", "filed as", "notice", "state of", "bureau", 
    "answer within", "claims to have", "unknown heirs", "devisees",
    "civil order", "protection", "defendant", "Defendants", "et al"
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
# HELPERS: Cleaning & ArcGIS
# ==========================================
def is_trash_name(name: str) -> bool:
    """Returns True if the name is actually a sentence fragment or institution."""
    if not name: return True
    name_low = name.lower()
    if any(word in name_low for word in BLACKLIST): return True
    if len(name.split()) > 6: return True # Too long to be a name
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
# DEEP SCRAPING LOGIC
# ==========================================
def extract_info_from_text(text: str):
    case_match = re.search(r"(CI\d{4}[-\s]?\d{4,6})", text, re.I)
    doc_num = case_match.group(1) if case_match else ""

    owner = ""
    owner_patterns = [
        r"vs\.?\s+([A-Z][a-zA-Z\s\.,]{3,40})(?=\s+[\n\r]|$)", 
        r"Defendant\s*[:\s]+([A-Z][a-zA-Z\s\.,]{3,40})",
        r"Estate\s+of\s+([A-Z][a-zA-Z\s\.,]{3,40})"
    ]
    for pat in owner_patterns:
        m = re.search(pat, text, re.I)
        if m:
            owner = m.group(1).strip()
            break

    addr_match = re.search(r"(\d+\s+[A-Z][a-zA-Z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way)[^,\n]*,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5})", text)
    prop_address = addr_match.group(1) if addr_match else ""

    return doc_num, owner, prop_address

async def scrape_source(page, url, dtype, flags):
    leads = []
    logger.info(f"Scraping {dtype} source...")
    try:
        await page.goto(url, timeout=60000)
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        
        links = []
        for a in soup.find_all("a", href=True):
            href = a['href']
            if any(x in href.lower() for x in ["article", "case-no", "ci20"]) and \
               not any(x in href.lower() for x in ["facebook", "twitter", "whatsapp", "email", "share"]):
                full_url = href if href.startswith("http") else TLN_BASE + href
                if full_url not in links: links.append(full_url)
        
        for link in links[:50]: # Limit to top 50 per category for speed
            try:
                await page.goto(link, timeout=30000)
                text = await page.inner_text("body")
                doc_num, owner, address = extract_info_from_text(text)
                
                if owner and not is_trash_name(owner):
                    lead = LeadRecord(doc_num=doc_num, doc_type=dtype, owner=owner, prop_address=address, clerk_url=link, flags=flags)
                    # ArcGIS Address lookup
                    if not address:
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
    except Exception as e:
        logger.error(f"Error scraping {dtype}: {e}")
    return leads

# ==========================================
# OUTPUTS
# ==========================================
def safe_save(leads: List[LeadRecord]):
    if not leads:
        logger.warning("🚨 No leads found. Keeping old data.")
        return
    DASHBOARD_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "total": len(leads),
        "with_address": len([l for l in leads if l.prop_address]),
        "records": [asdict(l) for l in leads]
    }
    with open(DASHBOARD_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)
    with open(DATA_DIR / "records.json", "w") as f: json.dump(output, f, indent=2)

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # STARTING ALL CATEGORIES TO FILL THE RED CIRCLES
        all_leads = []
        
        # 1. Foreclosures
        all_leads += await scrape_source(page, TLN_FORECLOSURE, "Foreclosure", ["Pre-foreclosure"])
        # 2. Probate
        all_leads += await scrape_source(page, TLN_PROBATE, "Probate", ["Inherited"])
        # 3. Sheriff Sales
        all_leads += await scrape_source(page, SHERIFF_URL, "Sheriff Sale", ["Sheriff Sale", "Hot Stack"])
        # 4. Code Violations
        all_leads += await scrape_source(page, CODE_VIOL_URL, "Code Violation", ["Nuisance", "Vacant"])
        # 5. Tax Delinquent
        all_leads += await scrape_source(page, TAX_DELINQ_URL, "Tax Delinquent", ["Tax Lien", "Hot Stack"])
        
        await browser.close()
        safe_save(all_leads)
        logger.info(f"DONE: Processed {len(all_leads)} clean leads across all categories.")

if __name__ == "__main__":
    asyncio.run(main())
