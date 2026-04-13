import requests
from bs4 import BeautifulSoup
import re
import json
import time
from datetime import datetime

# ================= CONFIGURATION =================
BASE_URL = "https://www.toledolegalnews.com"
# We only want legal notices. We EXCLUDE news, assignments, and general articles.
VALID_URL_PATH = "/legal_notices/" 
EXCLUDED_URL_PATHS = ["/news/", "/assignments/", "/death_notices/"]

# Regex pattern to find US addresses (Street, City, State, Zip)
ADDRESS_PATTERN = r'\d+\s+[A-Za-z0-9\s\.,#-]+,\s+[A-Za-z\s]+,\s+[A-Z]{2}\s+\d{5}'
# Regex pattern to find dollar amounts
AMOUNT_PATTERN = r'\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?'
# =================================================

class ToledoScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        })

    def clean_text(self, text):
        """Removes extra whitespace and newlines."""
        return " ".join(text.split()) if text else ""

    def extract_deep_details(self, url):
        """
        STAGE 2: Visits the individual article URL to extract 
        the actual property address and the lawsuit amount.
        """
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Get all text from the article body
            # Most news sites put the main content in an <article> or specific <div>
            body_text = soup.get_text(separator=' ')
            body_text = self.clean_text(body_text)

            # 1. Hunt for Address
            address_match = re.search(ADDRESS_PATTERN, body_text)
            address = address_match.group(0) if address_match else ""

            # 2. Hunt for Amount
            amount_match = re.search(AMOUNT_PATTERN, body_text)
            amount = amount_match.group(0) if amount_match else "0.0"

            return address, amount
        except Exception as e:
            print(f"Error deep-scanning {url}: {e}")
            return "", "0.0"

    def scrape_leads(self, start_url):
        """
        STAGE 1: Scrapes the main list and initiates deep dive.
        """
        print(f"🚀 Starting Deep-Dive Scrape at {datetime.now()}")
        
        response = self.session.get(start_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # This selector depends on the site's HTML. 
        # We are looking for the links to the individual foreclosure/probate notices.
        links = soup.find_all('a', href=True)
        
        records = []
        
        for link in links:
            url = link['href']
            if not url.startswith('http'):
                url = BASE_URL + url

            # --- NOISE FILTER ---
            # Only proceed if it's a legal notice and NOT a general news article
            if VALID_URL_PATH not in url or any(ex in url for ex in EXCLUDED_URL_PATHS):
                continue

            # Prevent duplicates
            if any(r['clerk_url'] == url for r in records):
                continue

            print(f"🔎 Deep-scanning lead: {url}")
            
            # Extract basic info from the link text/surrounding area
            owner_raw = link.get_text()
            
            # --- THE DEEP DIVE ---
            # Visit the page to get the address and amount
            prop_address, amount = self.extract_deep_details(url)
            
            # Only save if we actually found an address OR it's a clear legal notice
            # This prevents "Case No" articles without properties from filling your list.
            if prop_address or "case-no" in url.lower():
                records.append({
                    "owner": self.clean_text(owner_raw),
                    "amount": amount,
                    "prop_address": prop_address,
                    "clerk_url": url,
                    "fetched_at": datetime.now().isoformat(),
                    "source": "Lucas County, OH",
                    "flags": ["Pre-foreclosure" if "foreclosures" in url else "Inherited"]
                })
                # Small sleep to avoid getting IP banned
                time.sleep(0.5)

        return records

# ================= EXECUTION =================
if __name__ == "__main__":
    # Update this URL to the specific foreclosure or probate list page
    TARGET_PAGE = "https://www.toledolegalnews.com/legal_notices/foreclosures/" 
    
    scraper = ToledoScraper()
    all_leads = scraper.scrape_leads(TARGET_PAGE)
    
    # Save to JSON
    output = {
        "total": len(all_leads),
        "records": all_leads
    }
    
    with open("enriched_leads.json", "w") as f:
        json.dump(output, f, indent=2)
        
    print(f"✅ Success! Found {len(all_leads)} high-quality leads with addresses.")
