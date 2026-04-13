import asyncio
from playwright.async_api import async_playwright
import re
import json
from datetime import datetime

# ================= CONFIGURATION =================
SOURCES = {
    "LEGAL_NEWS": "https://www.toledolegalnews.com/legal_notices/foreclosures/",
    "TOLEDO_CODE_VIO": "https://www.toledoohio.gov/search-code-violations", # Hypothetical path - adjusts to actual
    "LUCAS_PROBATE": "https://www.toledolegalnews.com/legal_notices/probate/",
    "SHERIFF_SALES": "https://www.lucassheriff.com/sales"
}

# Mapping logic for the "Analyzer"
SCORE_WEIGHTS = {
    "Water Shut-off": 90,
    "Tax Delinquent": 80,
    "Code Violation": 70,
    "Foreclosure": 60,
    "Probate": 50,
    "Vacant": 40
}
# =================================================

class ToledoLeadEngine:
    def __init__(self):
        self.leads = []

    def calculate_score(self, lead_type, has_address):
        base_score = SCORE_WEIGHTS.get(lead_type, 10)
        return base_score if has_address else base_score - 30

    async def scrape_legal_news(self, page):
        print("🔎 Scraping Legal News (Foreclosures & Probate)...")
        await page.goto(SOURCES["LEGAL_NEWS"])
        links = await page.query_selector_all("a[href*='/legal_notices/']")
        
        for link in links:
            text = await link.inner_text()
            url = await link.get_attribute("href")
            if not url.startswith('http'): url = "https://www.toledolegalnews.com" + url
            
            # Analyze if it's Probate or Foreclosure based on keywords
            l_type = "Probate" if any(word in text.lower() for word in ["estate", "probate", "heirs"]) else "Foreclosure"
            
            self.leads.append({
                "owner": text.strip(),
                "property": "Checking...", 
                "amount": "$0",
                "doc_no": url.split('/')[-2],
                "type": l_type,
                "filed": datetime.now().strftime("%Y-%m-%d"),
                "link": url,
                "source": "LegalNews",
                "score": self.calculate_score(l_type, True)
            })

    async def scrape_sheriff(self, page):
        print("🚔 Scraping Sheriff Sales (Using Browser Rendering)...")
        await page.goto(SOURCES["SHERIFF_SALES"])
        # Wait for the content to actually load since the last script failed on the table
        await page.wait_for_load_state("networkidle")
        
        # We target ALL text blocks that look like property addresses
        content = await page.content()
        addresses = re.findall(r'\d+\s+[A-Za-z0-9\s\.,#-]+,\s+Toledo,\s+OH\s+\d{5}', content)
        
        for addr in addresses:
            self.leads.append({
                "owner": "Sheriff Sale Owner",
                "property": addr,
                "amount": "TBD",
                "doc_no": "SHERIFF",
                "type": "Sheriff Sale",
                "filed": datetime.now().strftime("%Y-%m-%d"),
                "link": SOURCES["SHERIFF_SALES"],
                "source": "Sheriff",
                "score": 85
            })

    async def scrape_city_code_violations(self, page):
        print("🏗️ Searching for Code Violations...")
        # Most city portals require a search click. Playwright handles this.
        try:
            await page.goto(SOURCES["TOLEDO_CODE_VIO"])
            # Logic: Click "All Violations" -> Scrape Table
            # This section is customized once we hit the specific city portal login/search
            await asyncio.sleep(2) 
        except Exception as e:
            print(f"City portal restricted: {e}")

    def analyze_subject_to(self, lead):
        """
        Logic: If they owe more than the arrears (amount), they are a 
        prime candidate for Subject-To.
        """
        try:
            amt = float(lead['amount'].replace('$', '').replace(',', ''))
            if amt > 5000: # Example threshold
                lead['tags'] = "SUBJECT-TO"
                lead['score'] += 20
        except:
            pass
        return lead

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            await self.scrape_legal_news(page)
            await self.scrape_sheriff(page)
            await self.scrape_city_code_violations(page)
            
            await browser.close()
            
            # Final Pass: Run leads through the Subject-To analyzer
            final_leads = [self.analyze_subject_to(l) for l in self.leads]
            
            output = {
                "total": len(final_leads),
                "records": final_leads
            }
            
            with open("data/enriched_leads.json", "w") as f:
                json.dump(output, f, indent=2)
            print(f"🎉 PIPELINE COMPLETE: {len(final_leads)} leads pushed to JSON.")

if __name__ == "__main__":
    engine = ToledoLeadEngine()
    asyncio.run(engine.run())
