import asyncio
import re
import json
import os
import requests
from datetime import datetime
from playwright.async_api import async_playwright

# ================= CONFIGURATION =================
CONFIG = {
    "SOURCES": {
        "LEGAL_NEWS": "https://www.toledolegalnews.com/legal_notices/foreclosures/",
        "PROBATE": "https://www.toledolegalnews.com/legal_notices/probate/",
        "SHERIFF": "https://www.lucassheriff.com/sales",
    },
    "ZILLOW_API_KEY": os.getenv("ZILLOW_API_KEY"),
    "RAPID_API_HOST": "zillow-com1.p.rapidapi.com",
    "WEIGHTS": {
        "Foreclosure": 70,
        "Probate": 50,
        "Sheriff Sale": 90
    },
    "NOISE_FILTER": ["annual reports", "dissolutions", "divorce", "zoning", "name changes", "bid notices", "public hearings", "whatsapp"]
}

ADDRESS_PATTERN = r'\d+\s+[A-Za-z0-9\s\.,#-]+,\s+(?:Toledo|Maumee|Perrysburg),\s+OH\s+\d{5}'
CASE_NUM_PATTERN = r'(CI\d{4}-\d{4,5}|CI\d{4}\d{5})'
# =================================================

class DataEnricher:
    @staticmethod
    async def deep_dive_address(page, url):
        """Visits the actual notice page to find the hidden address."""
        try:
            # Use wait_until="domcontentloaded" for speed and stability
            await page.goto(url, timeout=20000, wait_until="domcontentloaded")
            content = await page.content()
            match = re.search(ADDRESS_PATTERN, content)
            return match.group(0) if match else None
        except Exception as e:
            print(f"  [!] Deep dive failed for {url}: {e}")
            return None

    @staticmethod
    def get_market_value(address):
        """Uses the Zillow API to turn an address into a dollar value."""
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
            val = data.get("zestimate") or data.get("price") or data.get("valuation")
            return float(val) if val else 0.0
        except:
            return 0.0

class LeadCollector:
    def __init__(self):
        self.leads = []

    def calculate_score(self, lead):
        score = CONFIG["WEIGHTS"].get(lead["doc_type"], 10)
        if lead["prop_address"] and "Pending" not in lead["prop_address"]:
            score += 30 
        if "REAL ESTATE" not in lead["owner"].upper() and "LLC" not in lead["owner"].upper():
            score += 20 
        return min(score, 100)

    async def scrape_legal_notices(self, page):
        print("🔎 Phase 1: Gathering links...")
        await page.goto(CONFIG["SOURCES"]["LEGAL_NEWS"], wait_until="domcontentloaded")
        
        # WORKAROUND: Extract all data into a list FIRST to avoid "Execution context destroyed" error
        links_data = []
        elements = await page.query_selector_all("a[href*='/legal_notices/']")
        
        for el in elements:
            text = await el.inner_text()
            url = await el.get_attribute("href")
            if not url.startswith('http'): url = "https://www.toledolegalnews.com" + url
            
            # Apply Noise Filter immediately
            if any(word in text.lower() for word in CONFIG["NOISE_FILTER"]):
                continue
                
            links_data.append({"text": text.strip(), "url": url})

        print(f"🔎 Phase 2: Deep Diving {len(links_data)} individual leads...")
        for item in links_data:
            text = item["text"]
            url = item["url"]
            
            case_match = re.search(CASE_NUM_PATTERN, url.upper())
            case_num = case_match.group(0) if case_match else "UNKNOWN"
            
            print(f"  diving into: {text[:30]}...")
            actual_address = await DataEnricher.deep_dive_address(page, url)
            
            lead = {
                "owner": text,
                "doc_type": "Foreclosure",
                "case_num": case_num,
                "clerk_url": url,
                "filed": datetime.now().strftime("%Y-%m-%d"),
                "prop_address": actual_address if actual_address else "Pending Lookup",
                "prop_city": "Toledo",
                "prop_state": "OH",
                "prop_zip": "",
                "assessed_value": 0.0,
                "market_value": DataEnricher.get_market_value(actual_address),
                "parcel_id": "Pending",
                "flags": ["Pre-foreclosure"],
            }
            lead["score"] = self.calculate_score(lead)
            self.leads.append(lead)

    async def scrape_sheriff(self, page):
        print("🚔 Scraping Sheriff Sales...")
        try:
            await page.goto(CONFIG["SOURCES"]["SHERIFF"], wait_until="networkidle")
            content = await page.content()
            addresses = re.findall(ADDRESS_PATTERN, content)
            for addr in set(addresses):
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
                    "parcel_id": "Pending",
                    "flags": ["Auction"],
                }
                lead["score"] = self.calculate_score(lead)
                self.leads.append(lead)
        except Exception as e:
            print(f"Sheriff error: {e}")

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            page = await context.new_page()
            
            await self.scrape_legal_notices(page)
            await self.scrape_sheriff(page)
            
            await browser.close()
            self.export_data()

    def export_data(self):
        output = {
            "fetched_at": datetime.now().isoformat(),
            "source": "Lucas County, OH",
            "total": len(self.leads),
            "records": self.leads
        }
        os.makedirs("data", exist_ok=True)
        with open("data/records.json", "w") as f:
            json.dump(output, f, indent=2)
        print(f"🎉 SUCCESS: {len(self.leads)} leads processed.")

if __name__ == "__main__":
    collector = LeadCollector()
    asyncio.run(collector.run())
