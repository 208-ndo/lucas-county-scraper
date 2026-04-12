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
# THE "MAGIC BUTTON" - ArcGIS API
# ==========================================
def enrich_address_via_arcgis(owner_name: str) -> Optional[dict]:
    if not owner_name or len(owner_name) < 3: return None
    # Clean name for search
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
# DEEP EXTRACTION LOGIC
# ==========================================
def extract_info_from_text(text: str, url: str):
    """Deeply parses a page to find the Owner Name and Address."""
    # 1. Try to find the Case Number (e.g., CI2025-01234)
    case_match = re.search(r"(CI\d{4}[-\s]?\d{4,6})", text, re.I)
    doc_num = case_match.group(1) if case_match else ""

    # 2. Try to find the Owner/Defendant
    # Looks for "vs. Name", "Defendant: Name", "Estate of Name"
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

    # 3. Try to find a physical address in the text
    # Matches "123 Main St, Toledo, OH 43601"
    addr_match = re.search(r"(\d+\s+[A-Z][a-zA-Z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way)[^,\n]*,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5})", text)
    prop_address = addr_match.group(1) if addr_match else ""

    return doc_num, owner, prop_address

# ==========================================
# MAIN SCRAPER
# ==========================================
async def scrape_deep_tln(page):
    all_leads = []
    
    # Define the sources we want to dive into
    sources = [
        (TLN_FORECLOSURE, "Foreclosure", ["Pre-foreclosure"]),
        (TLN_PROBATE, "Probate", ["Inherited"])
    ]

    for url, dtype, flags in sources:
        logger.info(f"Scraping {dtype} list...")
        await page.goto(url, timeout=60000)
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        
        # Find all links, but FILTER OUT social media and nav links
        links = []
        for a in soup.find_all("a", href=True):
            href = a['href']
            # ONLY keep links that look like actual case articles
            if any(x in href.lower() for x in ["article", "case-no", "ci20"]) and \
               not any(x in href.lower() for x in ["facebook", "twitter", "whatsapp", "email", "share"]):
                full_url = href if href.startswith("http") else TLN_BASE + href
                if full_url not in links: links.append(full_url)
        
        logger.info(f"Found {len(links)} potential case pages in {dtype}. Diving deep...")

        for link in links:
            try:
                # Visit the actual case page to get the details
                await page.goto(link, timeout=30000)
                text = await page.inner_text("body")
                
                doc_num, owner, address = extract_info_from_text(text, link)
                
                if owner: # Only keep if we actually found a person
                    lead = LeadRecord(
                        doc_num=doc_num,
                        doc_type=dtype,
                        owner=owner,
                        prop_address=address,
                        clerk_url=link,
                        flags=flags
                    )
                    
                    # If we have a name but no address, use the ArcGIS Magic Button
                    if owner and not address:
                        addr_data = enrich_address_via_arcgis(owner)
                        if addr_data:
                            lead.prop_address = addr_data['prop_address']
                            lead.prop_city = addr_data['prop_city']
                            lead.prop_zip = addr_data['prop_zip']
                            lead.mail_address = addr_data['mail_address']
                            lead.mail_city = addr_data['mail_city']
                            lead.mail_zip = addr_data['mail_zip']
                            lead.parcel_id = addr_data['parcel_id']

                    all_leads.append(lead)
                    logger.info(f"✅ Found Lead: {owner} | Address: {lead.prop_address}")
                
            except Exception as e:
                logger.debug(f"Could not scrape case {link}: {e}")

    return all_leads

# ==========================================
# OUTPUTS
# ==========================================
def safe_save(leads: List[LeadRecord]):
    if not leads:
        logger.warning("🚨 No leads found. Keeping old data to prevent loss.")
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
    
    with open(DASHBOARD_DIR / "records.json", "w") as f:
        json.dump(output, f, indent=2)
    with open(DATA_DIR / "records.json", "w") as f:
        json.dump(output, f, indent=2)
    
    # GHL CSV
    file_path = DATA_DIR / "ghl_export.csv"
    fields = ["First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
              "Property Address", "Property City", "Property State", "Property Zip", "Lead Type", "Document Number", "Seller Score"]
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
                "Lead Type": l.doc_type, "Document Number": l.doc_num, "Seller Score": 40
            })

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        logger.info("🚀 Starting DEEP dive scrape...")
        leads = await scrape_deep_tln(page)
        
        await browser.close()
        safe_save(leads)
        logger.info(f"DONE: Processed {len(leads)} real leads.")

if __name__ == "__main__":
    asyncio.run(main())
