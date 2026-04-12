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
LOOKBACK_DAYS = 7
SOURCE_NAME = f"{COUNTY_NAME} County, {STATE}"
DATA_DIR = Path("data")
DASHBOARD_DIR = Path("dashboard")

# URLs
CLERK_PORTAL_URL = "https://www.lucas-county.com/clerkcourts/CaseSearch.aspx" 
# The ArcGIS API is the "Magic Button" for addresses
ARCGIS_API_URL = "https://services2.arcgis.com/ziRJBiSjXODrMVP5/arcgis/rest/services/Ohio_Statewide_Parcel_Data/FeatureServer/0/query"

# Mapping for Lead Types
TYPE_MAP = {
    "LP": "Lis Pendens",
    "NOFC": "Notice of Foreclosure",
    "TAXDEED": "Tax Deed",
    "JUD": "Judgment",
    "LN": "Lien",
    "PRO": "Probate",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

@dataclass
class LeadRecord:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    owner: str = ""
    grantee: str = ""
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
    category: str = "General"

# ==========================================
# THE "MAGIC BUTTON" - ArcGIS Address Lookup
# ==========================================
def enrich_address_via_arcgis(owner_name: str) -> Optional[dict]:
    """
    Queries the ArcGIS REST API to find a property address based on owner name.
    """
    if not owner_name or len(owner_name) < 3:
        return None

    # Clean name for query (Handle "Last, First" or "First Last")
    search_name = owner_name.upper().replace(",", "").strip()
    
    params = {
        "where": f"COUNTY='{COUNTY_NAME.upper()}' AND OWNER1 LIKE '%{search_name}%'",
        "outFields": "OWNER1,SITEADDRESS,SITECITY,SITEZIP,PARCELID,MAILADDRESS,MAILCITY,MAILZIP",
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": 1
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
    except Exception as e:
        logger.error(f"ArcGIS Error for {owner_name}: {e}")
    
    return None

# ==========================================
# SCORING LOGIC
# ==========================================
def calculate_seller_score(lead: LeadRecord):
    score = 30  # Base Score
    
    # Flag-based scoring
    flag_score = len(lead.flags) * 10
    score += flag_score
    
    # Combo bonus: Lis Pendens + Foreclosure
    if any("Lis pendens" in f for f in lead.flags) and any("Foreclosure" in f for f in lead.flags):
        score += 20
        
    # Amount bonuses
    if lead.amount > 100000:
        score += 15
    elif lead.amount > 50000:
        score += 10
        
    # Freshness bonus
    cutoff = datetime.now() - timedelta(days=7)
    try:
        filed_date = datetime.strptime(lead.filed, "%Y-%m-%d")
        if filed_date > cutoff:
            score += 5
            lead.flags.append("New this week")
    except:
        pass
        
    # Address bonus
    if lead.prop_address:
        score += 5
        
    return min(score, 100)

# ==========================================
# MAIN SCRAPER
# ==========================================
async def scrape_clerk_portal():
    leads = []
    logger.info(f"Starting scrape of {COUNTY_NAME} Clerk Portal...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            await page.goto(CLERK_PORTAL_URL, timeout=60000)
            
            # --- This is where we search for the specific lead types ---
            # In a real production environment, we would iterate through 
            # 'Lis Pendens', 'Foreclosure', etc., in the portal search boxes.
            
            # For this implementation, we are extracting current cases
            # We simulate the extraction of lead data from the portal results
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            
            # MOCK DATA EXTRACTION (Replace with specific selector logic for Lucas County)
            # This section represents the logic of finding the rows in the results table
            rows = soup.find_all("tr") 
            
            for row in rows:
                # Logic to identify if row is a lead (LP, Foreclosure, etc)
                # Example: if "Lis Pendens" in row.text:
                
                # Simulate a found lead for demonstration:
                # In production, you'd use: owner = row.find('td', class_='owner').text
                mock_lead = LeadRecord(
                    doc_num="CI2024-12345",
                    doc_type="LP",
                    filed=datetime.now().strftime("%Y-%m-%d"),
                    owner="Douglas G Kanag",
                    amount=75000.0,
                    clerk_url=CLERK_PORTAL_URL,
                    flags=["Lis pendens"]
                )
                leads.append(mock_lead)
                
            await browser.close()
        except Exception as e:
            logger.error(f"Playwright Error: {e}")
            await browser.close()

    return leads

async def process_leads():
    # 1. Scrape basic lead data from Clerk
    raw_leads = await scrape_clerk_portal()
    
    final_leads = []
    
    # 2. Enrich each lead using the "Magic Button" (ArcGIS)
    logger.info(f"Enriching {len(raw_leads)} leads with addresses via ArcGIS...")
    for lead in raw_leads:
        address_data = enrich_address_via_arcgis(lead.owner)
        if address_data:
            lead.prop_address = address_data['prop_address']
            lead.prop_city = address_data['prop_city']
            lead.prop_zip = address_data['prop_zip']
            lead.mail_address = address_data['mail_address']
            lead.mail_city = address_data['mail_city']
            lead.mail_zip = address_data['mail_zip']
            lead.parcel_id = address_data['parcel_id']
            logger.info(f"✅ Found address for {lead.owner}: {lead.prop_address}")
        else:
            logger.warning(f"❌ No address found for {lead.owner}")

        # 3. Score the lead
        lead.score = calculate_seller_score(lead)
        final_leads.append(lead)
        
    return final_leads

# ==========================================
# OUTPUTS
# ==========================================
def export_to_ghl_csv(leads: List[LeadRecord]):
    DATA_DIR.mkdir(exist_ok=True)
    file_path = DATA_DIR / "ghl_export.csv"
    
    fields = [
        "First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip", 
        "Lead Type", "Document Type", "Date Filed", "Document Number", "Amount/Debt Owed", 
        "Seller Score", "Motivated Seller Flags", "Source", "Public Records URL"
    ]
    
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        
        for l in leads:
            name_parts = l.owner.split(" ", 1)
            first = name_parts[0]
            last = name_parts[1] if len(name_parts) > 1 else ""
            
            writer.writerow({
                "First Name": first,
                "Last Name": last,
                "Mailing Address": l.mail_address,
                "Mailing City": l.mail_city,
                "Mailing State": l.mail_state,
                "Mailing Zip": l.mail_zip,
                "Property Address": l.prop_address,
                "Property City": l.prop_city,
                "Property State": l.prop_state,
                "Property Zip": l.prop_zip,
                "Lead Type": TYPE_MAP.get(l.doc_type, "Other"),
                "Document Type": l.doc_type,
                "Date Filed": l.filed,
                "Document Number": l.doc_num,
                "Amount/Debt Owed": l.amount,
                "Seller Score": l.score,
                "Motivated Seller Flags": ", ".join(l.flags),
                "Source": SOURCE_NAME,
                "Public Records URL": l.clerk_url
            })
    logger.info(f"CSV exported to {file_path}")

def save_to_json(leads: List[LeadRecord]):
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
    logger.info("JSON files updated.")

# ==========================================
# ENTRY POINT
# ==========================================
async def main():
    try:
        leads = await process_leads()
        save_to_json(leads)
        export_to_ghl_csv(leads)
        logger.info(f"SUCCESS: Processed {len(leads)} leads.")
    except Exception as e:
        logger.error(f"CRITICAL FAILURE: {e}")

if __name__ == "__main__":
    asyncio.run(main())
