import requests
from bs4 import BeautifulSoup
import re
import json
import time
from datetime import datetime

# ================= CONFIGURATION =================
# Source 1: News Site
NEWS_BASE_URL = "https://www.toledolegalnews.com"
NEWS_TARGET_PAGE = "https://www.toledolegalnews.com/legal_notices/foreclosures/"
VALID_URL_PATH = "/legal_notices/" 
EXCLUDED_URL_PATHS = ["/news/", "/assignments/", "/death_notices/"]

# Source 2: Sheriff's Site (Replace URL when confirmed)
SHERIFF_URL = "https://www.lucassheriff.com/sales" 

# Regex Patterns
ADDRESS_PATTERN = r'\d+\s+[A-Za-z0-9\s\.,#-]+,\s+[A-Za-z\s]+,\s+[A-Z]{2}\s+\d{5}'
AMOUNT_PATTERN = r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?'
# =================================================

class LucasCountyLeadEngine:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        })
        self.all_leads = []

    def clean_text(self, text):
        """Removes extra whitespace and newlines."""
        return " ".join(text.split()) if text else ""

    def deep_dive_news(self, url):
        """
        Visits individual news articles to find the ACTUAL address 
        and amount to replace 'Unknown' on the dashboard.
        """
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            body_text = self.clean_text(soup.get_text(separator=' '))

            address_match = re.search(ADDRESS_PATTERN, body_text)
            amount_match = re.search(AMOUNT_PATTERN, body_text)

            return (address_match.group(0) if address_match else ""), (amount_match.group(0) if amount_match else "0.0")
        except Exception as e:
            print(f"  [!] Deep dive failed for {url}: {e}")
            return "", "0.0"

    def scrape_news_source(self):
        """Stage 1: Scraping the Legal News site."""
        print(f"🚀 Starting News Deep-Dive at {datetime.now()}")
        try:
            response = self.session.get(NEWS_TARGET_PAGE, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)
            
            count = 0
            for link in links:
                url = link['href']
                if not url.startswith('http'): url = NEWS_BASE_URL + url

                # Noise Filter
                if VALID_URL_PATH not in url or any(ex in url for ex in EXCLUDED_URL_PATHS):
                    continue
                if any(l['link'] == url for l in self.all_leads):
                    continue

                owner_raw = self.clean_text(link.get_text())
                print(f"🔎 Deep-scanning: {owner_raw[:30]}...")
                
                prop_address, amount = self.deep_dive_news(url)
                
                # Data Mapping for Dashboard
                self.all_leads.append({
                    "owner": owner_raw,
                    "property": prop_address if prop_address else "Address Not Found",
                    "amount": amount,
                    "doc_no": url.split('/')[-2] if '/' in url else "N/A",
                    "type": "Foreclosure" if "foreclosures" in url else "Probate",
                    "filed": datetime.now().strftime("%Y-%m-%d"),
                    "link": url,
                    "source": "News",
                    "score": 40 if prop_address else 10 # Higher score if we have an address
                })
                count += 1
                time.sleep(0.5) # Polite scraping
            print(f"✅ Found {count} news leads.")
        except Exception as e:
            print(f"❌ News Scraper Error: {e}")

    def scrape_sheriff_source(self):
        """Stage 2: Scraping the Sheriff's Sales."""
        print(f"🚔 Scraping Sheriff's Sales: {SHERIFF_URL}")
        try:
            response = self.session.get(SHERIFF_URL, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Attempt to find any table on the page
            table = soup.find('table')
            if not table:
                print("  [!] No table found on Sheriff page. (Need specific URL/HTML structure)")
                return

            rows = table.find_all('tr')
            count = 0
            for row in rows[1:]: # Skip header
                cols = row.find_all('td')
                if len(cols) >= 2:
                    self.all_leads.append({
                        "owner": self.clean_text(cols[0].text),
                        "property": self.clean_text(cols[1].text),
                        "amount": "TBD",
                        "doc_no": "SHERIFF-SALE",
                        "type": "Sheriff Sale",
                        "filed": datetime.now().strftime("%Y-%m-%d"),
                        "link": SHERIFF_URL,
                        "source": "Sheriff",
                        "score": 80 # Sheriff leads are high urgency
                    })
                    count += 1
            print(f"✅ Found {count} sheriff leads.")
        except Exception as e:
            print(f"❌ Sheriff Scraper Error: {e}")

    def save_results(self, filename="data/enriched_leads.json"):
        """Saves the data in the exact format the dashboard expects."""
        # Deduplicate by link
        unique_leads = {lead['link']: lead for lead in self.all_leads}.values()
        
        output = {
            "total": len(unique_leads),
            "records": list(unique_leads)
        }
        
        try:
            with open(filename, "w") as f:
                json.dump(output, f, indent=2)
            print(f"\n🎉 SUCCESS: {len(unique_leads)} total leads saved to {filename}")
        except Exception as e:
            print(f"❌ File Save Error: {e}")

# ================= EXECUTION =================
if __name__ == "__main__":
    engine = LucasCountyLeadEngine()
    
    # 1. Run News Scraper (The Deep Dive)
    engine.scrape_news_source()
    
    # 2. Run Sheriff Scraper (The Easy Win)
    engine.scrape_sheriff_source()
    
    # 3. Finalize and Save
    engine.save_results()
