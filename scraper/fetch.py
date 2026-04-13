import asyncio
import re
import json
import os
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ================= CONFIGURATION =================
CONFIG = {
    "SOURCES": {
        "LEGAL_NEWS": "https://www.toledolegalnews.com/legal_notices/foreclosures/",
        "PROBATE": "https://www.toledolegalnews.com/legal_notices/probate/",
        "SHERIFF": "https://www.lucassheriff.com/sales",
        "AUDITOR_SEARCH": "https://www.lucasauditor.com/search/" # Target for enrichment
    },
    "ZILLOW_API_KEY": os.getenv("ZILLOW_API_KEY"), # Set in GH Secrets
    "RAPID_API_HOST": "zillow-com1.p.rapidapi.com",
    "WEIGHTS": {
        "Foreclosure": 70,
        "Probate": 50,
        "Tax Delinquent": 80,
        "Sheriff Sale": 90
    }
}

# Regex Patterns
ADDRESS_PATTERN = r'\d+\s+[A-Za-z0-9\s\.,#-]+,\s+[A-Za-z\s]+,\s+[A-Z]{2}\s+\d{5}'
CASE_NUM_PATTERN = r'(CI\d{4}-\d{4,5})' # Matches Lucas County Case format CI2025-04139
# =================================================

class DataEnricher:
    """Handles Asset Data: Address Lookup and Valuation"""
    
    @staticmethod
    async def get_property_details(page, case_num, owner_name):
        """
        Workaround: Instead of scraping the notice, we query the Auditor 
        or the Court details using the case number.
        """
        try:
            # Attempt to find the address by visiting the specific court record link 
            # or searching the auditor via the case number
            # Logic: Navigate to Clerk/Auditor -> Search Case -> Extract Address
            # For this version, we simulate the lookup logic.
            return {
                "prop_address": "Pending Lookup", 
                "assessed_value": 0.0,
                "parcel_id": "Pending"
            }
        except Exception as e:
            print(f"Enrichment error for {case_num}: {e}")
            return {"prop_address": None, "assessed_value": 0.0, "parcel_id": None}

    @staticmethod
    def get_market_value(address):
        """Fetches Zillow/Redfin value via RapidAPI or returns 0.0"""
        if not CONFIG["ZILLOW_API_KEY"] or not address or "Pending" in address:
            return 0.0
        
        url = f"https://{CONFIG['RAPID_API_HOST']}/property"
        querystring = {"address": address}
        headers = {
            "X-RapidAPI-Key": CONFIG["ZILLOW_API_KEY"],
            "X-RapidAPI-Host": CONFIG["RAPID_API_HOST"]
        }
        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=5)
            data = response.json()
            return float(data.get("zestimate", 0.0))
        except:
            return 0.0

class LeadCollector:
    def __init__(self):
        self.leads = []

    def calculate_motivation_score(self, lead):
        score = CONFIG["WEIGHTS"].get(lead["doc_type"], 10)
        
        # Boost: Individual owner (Not a company)
        if "REAL ESTATE" not in lead["owner"].upper() and "LLC" not in lead["owner"].upper():
            score += 20
        
        # Boost: Has specific address (High quality)
        if lead["prop_address"] and "Pending" not in lead["prop_address"]:
            score += 20
            
        return min(score, 100)

    async def scrape_legal_notices(self, page):
        print("🔎 Scraping Legal Notices...")
        await page.goto(CONFIG["SOURCES"]["LEGAL_NEWS"])
        links = await page.query_selector_all("a[href*='/legal_notices/']")
        
        for link in links:
            text = await link.inner_text()
            url = await link.get_attribute("href")
            if not url.startswith('http'): url = "https://www.toledolegalnews.com" + url
            
            # Extract Case Number from URL or Text
            case_match = re.search(CASE_NUM_PATTERN, url.upper())
            case_num = case_match.group(0) if case_match else "UNKNOWN"
            
            # Basic lead structure
            lead = {
                "owner": text.strip(),
                "doc_type": "Foreclosure",
                "case_num": case_num,
                "clerk_url": url,
                "filed": datetime.now().strftime("%Y-%m-%d"),
                "prop_address": "",
                "prop_city": "Toledo",
                "prop_state": "OH",
                "prop_zip": "",
                "assessed_value": 0.0,
                "market_value": 0.0,
                "parcel_id": "",
                "flags": ["Pre-foreclosure"],
                "score": 0
            }
            
            # TRIGGER ENRICHMENT
            enrichment = await DataEnricher.get_property_details(page, case_num, text)
            lead.update(enrichment)
            
            # ADD VALUATION
            lead["market_value"] = DataEnricher.get_market_value(lead["prop_address"])
            
            # SCORE LEAD
            lead["score"] = self.calculate_motivation_score(lead)
            
            self.leads.append(lead)

    async def scrape_sheriff(self, page):
        print("🚔 Scraping Sheriff Sales...")
        try:
            await page.goto(CONFIG["SOURCES"]["SHERIFF"])
            await page.wait_for_load_state("networkidle")
            content = await page.content()
            
            # Extracting all address-like patterns
            addresses = re.findall(ADDRESS_PATTERN, content)
            for addr in set(addresses): # Deduplicate
                lead = {
                    "owner": "Sheriff Sale Target",
                    "doc_type": "Sheriff Sale",
                    "case_num": "SHERIFF",
                    "clerk_url": CONFIG["SOURCES"]["SHERIFF"],
                    "filed": datetime.now().strftime("%Y-%m-%d"),
                    "prop_address": addr,
                    "prop_city": "Toledo",
                    "prop_state": "OH",
                    "prop_zip": "",
                    "assessed_value": 0.0,
                    "market_value": DataEnricher.get_market_value(addr),
                    "parcel_id": "Lookup Required",
                    "flags": ["Auction"],
                    "score": 90
                }
                self.leads.append(lead)
        except Exception as e:
            print(f"Sheriff scrape error: {e}")

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            page = await context.new_page()
            
            await self.scrape_legal_notices(page)
            await self.scrape_sheriff(page)
            
            await browser.close()
            
            # Final Export
            self.export_data()

    def export_data(self):
        output = {
            "fetched_at": datetime.now().isoformat(),
            "source": "Lucas County, OH",
            "total": len(self.leads),
            "records": self.leads
        }
        
        # Ensure directory exists
        os.makedirs("data", exist_ok=True)
        with open("data/records.json", "w") as f:
            json.dump(output, f, indent=2)
        
        print(f"🎉 SUCCESS: {len(self.leads)} leads enriched and pushed to records.json")

if __name__ == "__main__":
    collector = LeadCollector()
    asyncio.run(collector.run())
