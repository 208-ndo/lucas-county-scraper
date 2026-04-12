"""
Toledo / Lucas County — Motivated Seller Intelligence Platform
=============================================================
FREE PUBLIC SOURCES (no login required):
  1. Toledo Legal News Common Pleas daily filings  — foreclosures, liens, judgments
  2. Lucas County Sheriff Sale Auction site         — active sheriff sales
  3. Lucas County Common Pleas online dockets       — civil case search
  4. Lucas County Domestic Relations dockets        — divorces
  5. Lucas County Auditor iCare                     — parcel/owner/value lookup
  6. Lucas County Recorder (DTS)                    — deeds, mortgages, liens
  7. Ohio Secretary of State UCC                    — mechanic / commercial liens
  8. PACER / US Courts RSS                          — federal tax liens, bankruptcies

Runs daily via GitHub Actions, deploys to GitHub Pages.
"""
import argparse, asyncio, csv, json, logging, re, time, random
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlencode, quote

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
DATA_DIR      = BASE_DIR / "data"
DASHBOARD_DIR = BASE_DIR / "dashboard"
DEBUG_DIR     = DATA_DIR / "debug"

DEFAULT_OUTPUT_JSON_PATHS = [DATA_DIR / "records.json", DASHBOARD_DIR / "records.json"]
DEFAULT_OUTPUT_CSV_PATH   = DATA_DIR / "ghl_export.csv"
DEFAULT_ENRICHED_CSV_PATH = DATA_DIR / "records.enriched.csv"

LOOKBACK_DAYS = 90
SOURCE_NAME   = "Toledo / Lucas County, Ohio"
OH_APPR_RATE  = 0.04

# ── Source URLs ────────────────────────────────────────────────────────────
TLN_BASE             = "https://www.toledolegalnews.com"
TLN_COMMON_PLEAS_URL = "https://www.toledolegalnews.com/courts/common_pleas/"
TLN_PROBATE_URL      = "https://www.toledolegalnews.com/courts/probate/"
TLN_DOMESTIC_URL     = "https://www.toledolegalnews.com/courts/domestic_court/"

# Lucas County official free sources
SHERIFF_AUCTION_URL  = "https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=USER&zmethod=CALENDAR"
SHERIFF_LISTINGS_URL = "https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE="
LC_DOCKETS_URL       = "https://lcapps.co.lucas.oh.us/onlinedockets/SearchEntry.aspx"
LC_DOCKETS_SEARCH    = "https://lcapps.co.lucas.oh.us/onlinedockets/SearchResults.aspx"
LC_DR_DOCKETS_URL    = "https://www.co.lucas.oh.us/99/Domestic-Relations-Online-Dockets"
LC_AUDITOR_URL       = "https://icare.co.lucas.oh.us/LucasCare/search/commonsearch.aspx?mode=owner"
LC_FORECLOSURE_URL   = "http://lcapps.co.lucas.oh.us/foreclosure/search.aspx"
LC_RECORDER_URL      = "https://lucas.dts-oh.com/PaxWorld5/"

# Ohio SOS UCC lien search (mechanic liens, commercial)
OHIO_SOS_UCC_URL     = "https://www.ohiosos.gov/businesses/business-filings/ucc-filings/"

# Lucas County ArcGIS parcel data
LUCAS_ARCGIS_URLS = [
    "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer/2/query?where=1%3D1&outFields=*&f=json&resultRecordCount=1000&orderByFields=OBJECTID+DESC",
    "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer/3/query?where=1%3D1&outFields=*&f=json&resultRecordCount=1000",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Lead type maps ─────────────────────────────────────────────────────────
LEAD_TYPE_MAP = {
    "LP":"Lis Pendens","NOFC":"Pre-foreclosure","TAXDEED":"Tax Deed",
    "JUD":"Judgment","CCJ":"Certified Judgment","DRJUD":"Domestic Judgment",
    "LNCORPTX":"Corp Tax Lien","LNIRS":"IRS Lien","LNFED":"Federal Lien",
    "LN":"Lien","LNMECH":"Mechanic Lien","LNHOA":"HOA Lien","MEDLN":"Medicaid Lien",
    "PRO":"Probate / Estate","NOC":"Notice of Commencement","RELLP":"Release Lis Pendens",
    "TAX":"Tax Delinquent","SHERIFF":"Sheriff Sale","CODEVIOLATION":"Code Violation",
    "DIVORCE":"Divorce Filing","EVICTION":"Eviction","BK":"Bankruptcy",
}

STATE_CODES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC"
}

STACK_BONUS = {2: 15, 3: 25, 4: 40}

# ── Data class ─────────────────────────────────────────────────────────────
@dataclass
class LeadRecord:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""
    cat: str = ""
    cat_label: str = ""
    owner: str = ""
    grantee: str = ""
    amount: Optional[float] = None
    legal: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "OH"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""
    clerk_url: str = ""
    flags: List[str] = field(default_factory=list)
    score: int = 0
    match_method: str = "unmatched"
    match_score: float = 0.0
    with_address: int = 0
    distress_sources: List[str] = field(default_factory=list)
    distress_count: int = 0
    hot_stack: bool = False
    parcel_id: str = ""
    luc: str = ""
    is_vacant_land: bool = False
    is_vacant_home: bool = False
    is_absentee: bool = False
    is_out_of_state: bool = False
    is_inherited: bool = False
    assessed_value: Optional[float] = None
    estimated_value: Optional[float] = None
    last_sale_price: Optional[float] = None
    last_sale_year: Optional[int] = None
    est_mortgage_balance: Optional[float] = None
    est_equity: Optional[float] = None
    est_arrears: Optional[float] = None
    est_payoff: Optional[float] = None
    subject_to_score: int = 0
    mortgage_signals: List[str] = field(default_factory=list)
    sheriff_sale_date: str = ""
    appraised_value: Optional[float] = None
    lender: str = ""
    decedent_name: str = ""
    executor_name: str = ""

# ── Helpers ────────────────────────────────────────────────────────────────
def ensure_dirs():
    for d in [DATA_DIR, DASHBOARD_DIR, DEBUG_DIR]:
        d.mkdir(parents=True, exist_ok=True)

def log_setup():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

def save_debug(name: str, content: str):
    try:
        (DEBUG_DIR / name).write_text(content[:50000], encoding="utf-8")
    except Exception as e:
        logging.warning("debug save %s: %s", name, e)

def clean(v) -> str:
    if v is None: return ""
    return re.sub(r"\s+", " ", str(v)).strip()

def norm_state(v: str) -> str:
    v = re.sub(r"[^A-Z]", "", clean(v).upper())
    return v if v in STATE_CODES else ""

def retry_get(url: str, attempts: int = 3, timeout: int = 30, delay: float = 2.0, **kwargs):
    last = None
    for i in range(1, attempts + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout,
                             allow_redirects=True, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            logging.warning("GET attempt %s/%s %s: %s", i, attempts, url[:80], e)
            if i < attempts:
                time.sleep(delay * i + random.uniform(0, 1))
    raise last

def retry_post(url: str, data: dict, attempts: int = 3, timeout: int = 30, delay: float = 2.0):
    last = None
    for i in range(1, attempts + 1):
        try:
            r = requests.post(url, headers=HEADERS, data=data,
                              timeout=timeout, allow_redirects=True)
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            logging.warning("POST attempt %s/%s %s: %s", i, attempts, url[:80], e)
            if i < attempts:
                time.sleep(delay * i)
    raise last

async def pw_fetch(url: str, wait_ms: int = 2500, click_selector: str = None) -> str:
    """Playwright fetch with full browser simulation and anti-bot bypass."""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--window-size=1366,768",
                ]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="America/New_York",
                extra_http_headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "DNT": "1",
                }
            )
            await ctx.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
                window.chrome = {runtime: {}};
            """)
            page = await ctx.new_page()
            try:
                # Warm up with homepage first
                domain = re.match(r"https?://[^/]+", url)
                if domain:
                    try:
                        await page.goto(domain.group(0), wait_until="domcontentloaded", timeout=15000)
                        await page.wait_for_timeout(1000 + random.randint(0, 500))
                    except:
                        pass
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(wait_ms + random.randint(0, 800))
                if click_selector:
                    try:
                        await page.click(click_selector, timeout=5000)
                        await page.wait_for_timeout(1500)
                    except:
                        pass
                html = await page.content()
                logging.info("pw_fetch %s chars from %s", len(html), url[:80])
                return html
            finally:
                await page.close()
                await browser.close()
    except Exception as e:
        logging.warning("pw_fetch failed %s: %s", url[:80], e)
        return ""

def parse_amount(v: str) -> Optional[float]:
    if not v: return None
    c = re.sub(r"[^0-9.\-]", "", v)
    try: return float(c) if c else None
    except: return None

def norm_addr_key(address: str) -> str:
    addr = clean(address).upper()
    for old, new in [
        ("NORTH", "N"), ("SOUTH", "S"), ("EAST", "E"), ("WEST", "W"),
        ("N.", "N"), ("S.", "S"), ("E.", "E"), ("W.", "W"),
        ("STREET", "ST"), ("AVENUE", "AVE"), ("ROAD", "RD"),
        ("DRIVE", "DR"), ("BOULEVARD", "BLVD"), ("LANE", "LN"),
        ("COURT", "CT"), ("PLACE", "PL"), ("TERRACE", "TER"),
        ("CIRCLE", "CIR"), ("PARKWAY", "PKWY"),
    ]:
        addr = re.sub(r'\b' + old + r'\b', new, addr)
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9\s]", "", addr)).strip()

def norm_name(n: str) -> str:
    n = clean(n).upper()
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9,&.\- /']", " ", n)).strip()

def name_variants(name: str) -> List[str]:
    n = clean(name).upper()
    if not n: return []
    variants = {n}
    parts = re.split(r"[\s,]+", n)
    parts = [p for p in parts if p]
    if len(parts) >= 2:
        variants.add(f"{parts[0]} {parts[-1]}")
        variants.add(f"{parts[-1]} {parts[0]}")
        variants.add(f"{parts[-1]}, {parts[0]}")
    return list(variants)

def likely_corp(n: str) -> bool:
    CORP = {"LLC","INC","CORP","CO","TRUST","BANK","LTD","LP","PLC","HOLDINGS",
            "PROPERTIES","REALTY","INVESTMENTS","CAPITAL","GROUP","PARTNERS",
            "MANAGEMENT","ENTERPRISES","SOLUTIONS","SERVICES","ASSOCIATES"}
    return any(t in CORP for t in norm_name(n).split())

def try_parse_date(text: str) -> Optional[str]:
    text = clean(text)
    if not text: return None
    patterns = [
        r"\b(\d{4}-\d{2}-\d{2})\b",
        r"\b(\d{1,2}/\d{1,2}/\d{4})\b",
        r"\b(\d{1,2}/\d{1,2}/\d{2})\b",
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(0)
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%B %d %Y"):
                try: return datetime.strptime(raw, fmt).date().isoformat()
                except: continue
    return None

def is_recent(filed: str, days: int = LOOKBACK_DAYS) -> bool:
    try:
        return datetime.fromisoformat(filed).date() >= (datetime.now().date() - timedelta(days=days))
    except:
        return True  # keep if can't parse

def infer_doc_type(text: str) -> Optional[str]:
    t = clean(text).upper()
    if any(x in t for x in ["LIS PENDENS", " LP ", "LP-"]): return "LP"
    if any(x in t for x in ["NOTICE OF FORECLOSURE", "FORECLOS", "NOFC", "COMPLAINT TO FORECLOSE", "MTG ON"]): return "NOFC"
    if any(x in t for x in ["SHERIFF", "SHERIFF SALE", "AUCTION"]): return "SHERIFF"
    if any(x in t for x in ["DIVORCE", "DISSOLUTION OF MARRIAGE", "DOMESTIC REL"]): return "DIVORCE"
    if any(x in t for x in ["EVICTION", "FORCIBLE ENTRY"]): return "EVICTION"
    if any(x in t for x in ["CERTIFIED JUDGMENT", "JUDGMENT LIEN"]): return "CCJ"
    if "JUDGMENT" in t: return "JUD"
    if any(x in t for x in ["TAX DEED", "TAXDEED"]): return "TAXDEED"
    if any(x in t for x in ["IRS", "INTERNAL REVENUE"]): return "LNIRS"
    if any(x in t for x in ["FEDERAL TAX", "US TAX", "UNITED STATES TAX", "LNFED"]): return "LNFED"
    if any(x in t for x in ["STATE TAX", "OHIO TAX", "INCOME TAX", "DEPT OF TAXATION"]): return "LNCORPTX"
    if "MECHANIC" in t: return "LNMECH"
    if "HOA" in t or "HOMEOWNER" in t: return "LNHOA"
    if "MEDICAID" in t: return "MEDLN"
    if "CHILD SUPPORT" in t: return "LN"
    if "LIEN" in t: return "LN"
    if any(x in t for x in ["PROBATE", "ESTATE OF", "IN RE ESTATE"]): return "PRO"
    if "NOTICE OF COMMENCEMENT" in t: return "NOC"
    if "BANKRUPTCY" in t or " BK " in t: return "BK"
    return None

def classify_distress(doc_type: str) -> Optional[str]:
    dt = clean(doc_type).upper()
    m = {
        "LP": "lis_pendens", "RELLP": "lis_pendens",
        "NOFC": "foreclosure", "TAXDEED": "tax_delinquent",
        "JUD": "judgment", "CCJ": "judgment", "DRJUD": "judgment",
        "LN": "lien", "LNHOA": "lien", "LNFED": "lien",
        "LNIRS": "lien", "LNCORPTX": "lien", "MEDLN": "lien",
        "LNMECH": "mechanic_lien",
        "TAX": "tax_delinquent",
        "PRO": "probate",
        "SHERIFF": "sheriff_sale",
        "CODEVIOLATION": "code_violation",
        "DIVORCE": "divorce",
        "EVICTION": "eviction",
        "BK": "bankruptcy",
        "NOC": "mechanic_lien",
    }
    return m.get(dt)

def cat_flags(doc_type: str, owner: str = "") -> List[str]:
    flags = []
    dt = clean(doc_type).upper()
    ou = norm_name(owner)
    if dt == "LP": flags.append("Lis pendens")
    if dt == "NOFC": flags.append("Pre-foreclosure")
    if dt in {"JUD", "CCJ", "DRJUD"}: flags.append("Judgment lien")
    if dt in {"TAXDEED", "LNCORPTX", "LNIRS", "LNFED", "TAX"}: flags.append("Tax lien")
    if dt == "LNMECH" or dt == "NOC": flags.append("Mechanic lien")
    if dt == "PRO": flags.append("Probate / estate")
    if dt == "SHERIFF": flags.append("Sheriff sale scheduled")
    if dt == "CODEVIOLATION": flags.append("Code violation")
    if dt == "DIVORCE": flags.append("Divorce filing")
    if dt == "EVICTION": flags.append("Eviction filed")
    if dt == "BK": flags.append("Bankruptcy")
    if likely_corp(ou): flags.append("LLC / corp owner")
    return list(dict.fromkeys(flags))

def is_absentee(prop_addr: str, mail_addr: str, mail_state: str = "") -> bool:
    if not prop_addr or not mail_addr: return False
    if re.search(r"\bP\.?\s*O\.?\s*BOX\b", mail_addr.upper()): return True
    s = norm_state(mail_state)
    if s and s != "OH": return True
    pk = norm_addr_key(prop_addr)
    mk = norm_addr_key(mail_addr)
    if not pk or not mk or pk == mk: return False
    def core(a):
        parts = a.split()
        return " ".join(parts[:2]) if len(parts) >= 2 else a
    return core(pk) != core(mk)

def is_oos(mail_state: str) -> bool:
    s = norm_state(mail_state)
    return bool(s and s != "OH")

# ── Mortgage / equity estimation ───────────────────────────────────────────
def estimate_financials(record: "LeadRecord") -> "LeadRecord":
    signals = []
    sto = 0
    mv = record.estimated_value

    if not mv and record.last_sale_price and record.last_sale_price > 5000:
        yrs = max(0, datetime.now().year - (record.last_sale_year or datetime.now().year))
        mv = record.last_sale_price * ((1 + OH_APPR_RATE) ** yrs)
    if not mv and record.assessed_value and record.assessed_value > 1000:
        mv = record.assessed_value / 0.35
    if mv:
        record.estimated_value = round(mv, 2)

    if record.last_sale_price and record.last_sale_year and record.last_sale_price > 5000:
        yrs_elapsed = max(0, min(30, datetime.now().year - record.last_sale_year))
        orig = record.last_sale_price * 0.80
        mr = 0.065 / 12; n = 360; paid = yrs_elapsed * 12
        if mr > 0 and paid < n:
            bal = orig * ((1 + mr) ** n - (1 + mr) ** paid) / ((1 + mr) ** n - 1)
            record.est_mortgage_balance = round(max(0, bal), 2)
        elif paid >= n:
            record.est_mortgage_balance = 0.0

    if record.estimated_value and record.est_mortgage_balance is not None:
        record.est_equity = round(record.estimated_value - record.est_mortgage_balance, 2)
    elif record.estimated_value and record.est_mortgage_balance is None and not record.last_sale_price:
        record.est_mortgage_balance = round(record.estimated_value * 0.50, 2)
        record.est_equity = round(record.estimated_value * 0.50, 2)
        record.est_payoff = record.est_mortgage_balance

    if record.doc_type in {"LP", "NOFC", "TAXDEED", "SHERIFF"} and record.amount and record.amount > 0:
        record.est_arrears = record.amount
        record.est_payoff = record.est_mortgage_balance or record.amount
        signals.append(f"Arrears ~${record.est_arrears:,.0f}")
    if "Tax lien" in record.flags and record.amount and record.amount > 0:
        record.est_arrears = (record.est_arrears or 0) + record.amount
        signals.append(f"Tax owed ~${record.amount:,.0f}")

    if record.est_equity is not None:
        if record.est_equity > 50000: sto += 30; signals.append("High equity")
        elif record.est_equity > 20000: sto += 20; signals.append("Moderate equity")
        elif record.est_equity > 0: sto += 10
        else: signals.append("Underwater")

    if record.doc_type in {"LP", "NOFC", "SHERIFF"}: sto += 25; signals.append("Active foreclosure")
    if record.doc_type == "PRO": sto += 20; signals.append("Estate / probate")
    if record.is_absentee: sto += 15; signals.append("Absentee owner")
    if record.is_out_of_state: sto += 10; signals.append("Out-of-state owner")
    if record.is_inherited: sto += 20; signals.append("Inherited property")
    if "Tax lien" in record.flags: sto += 15

    if record.est_mortgage_balance and record.estimated_value:
        ltv = record.est_mortgage_balance / record.estimated_value
        if ltv < 0.5: sto += 20; signals.append("Low LTV <50%")
        elif ltv < 0.7: sto += 10; signals.append("LTV <70%")
        elif ltv > 0.95: signals.append("High LTV >95%")

    if sto >= 50 and "Subject-To Candidate" not in " ".join(record.flags):
        record.flags.append("Subject-To Candidate")
    if sto >= 70 and "Prime Subject-To" not in " ".join(record.flags):
        record.flags.append("Prime Subject-To")

    record.subject_to_score = min(sto, 100)
    record.mortgage_signals = signals
    return record

def score_record(record: "LeadRecord") -> int:
    score = 30
    lf = {f.lower() for f in record.flags}
    fs = 0
    if "lis pendens" in lf: fs += 20
    if "pre-foreclosure" in lf: fs += 20
    if "judgment lien" in lf: fs += 15
    if "tax lien" in lf: fs += 15
    if "mechanic lien" in lf: fs += 10
    if "probate / estate" in lf: fs += 15
    if "sheriff sale scheduled" in lf: fs += 35
    if "code violation" in lf: fs += 20
    if "eviction filed" in lf: fs += 18
    if "divorce filing" in lf: fs += 15
    if "absentee owner" in lf: fs += 10
    if "out-of-state owner" in lf: fs += 12
    if "bankruptcy" in lf: fs += 12
    if "inherited property" in lf: fs += 15
    if "subject-to candidate" in lf: fs += 15
    if "prime subject-to" in lf: fs += 20
    score += min(fs, 70)
    if "lis pendens" in lf and "pre-foreclosure" in lf: score += 20
    if record.amount is not None:
        score += 15 if record.amount > 100000 else (10 if record.amount > 50000 else 5)
    if record.filed:
        try:
            if datetime.fromisoformat(record.filed).date() >= (datetime.now().date() - timedelta(days=7)):
                if "New this week" not in record.flags:
                    record.flags.append("New this week")
                score += 5
        except:
            pass
    if record.prop_address: score += 5
    if record.mail_address: score += 3
    dc = len(set(record.distress_sources))
    record.distress_count = dc
    bk = min(dc, 4)
    if bk >= 2:
        score += STACK_BONUS.get(bk, STACK_BONUS[4])
        record.hot_stack = True
        if "Hot Stack" not in " ".join(record.flags):
            record.flags.append("Hot Stack")
    return min(score, 100)

# ── Parcel data ────────────────────────────────────────────────────────────
def load_parcel_data() -> Dict[str, dict]:
    parcels: Dict[str, dict] = {}
    logging.info("Loading Lucas County parcel data...")

    urls_to_try = [
        "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer/2/query?where=1%3D1&outFields=*&f=json&resultRecordCount=2000&orderByFields=OBJECTID+DESC",
        "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer/3/query?where=1%3D1&outFields=*&f=json&resultRecordCount=2000",
        "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer/4/query?where=1%3D1&outFields=*&f=json&resultRecordCount=2000",
        "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer/5/query?where=1%3D1&outFields=*&f=json&resultRecordCount=2000",
    ]

    raw_records = []
    for url in urls_to_try:
        try:
            r = retry_get(url, timeout=60)
            data = r.json()
            features = data.get("features", [])
            if not features:
                continue
            sample = features[0].get("attributes", features[0].get("properties", {}))
            keys = list(sample.keys()) if isinstance(sample, dict) else []
            has_owner = any(x in str(keys).upper() for x in ["OWNER", "OWN1", "NAME"])
            has_addr = any(x in str(keys).upper() for x in ["ADDR", "ADDRESS", "SITE"])
            logging.info("Layer %s: %s features | owner=%s addr=%s | fields=%s",
                         url[-20:], len(features), has_owner, has_addr, keys[:6])
            if has_owner or has_addr:
                raw_records = [f.get("attributes", f.get("properties", f)) for f in features]
                save_debug("parcel_fields.json", json.dumps(keys[:30]))
                break
        except Exception as e:
            logging.warning("Parcel URL failed: %s", e)

    if not raw_records:
        logging.warning("No parcel layer accessible - leads work without enrichment")
        return parcels

    for props in raw_records:
        if not props or not isinstance(props, dict):
            continue
        P = {k.upper(): v for k, v in props.items()}
        owner = clean(P.get("OWNER") or P.get("OWNER_NAME") or P.get("OWN1") or P.get("OWNERNAME") or "")
        addr = clean(P.get("SITE_ADDR") or P.get("SITEADDR") or P.get("ADDRESS") or P.get("SADDR") or "")
        city = clean(P.get("SITE_CITY") or P.get("CITY") or P.get("SCITY") or "") or "Toledo"
        zip_ = clean(P.get("SITE_ZIP") or P.get("ZIP") or P.get("ZIPCODE") or P.get("SZIP") or "")
        mail_addr = clean(P.get("MAIL_ADDR") or P.get("MAILADR1") or P.get("MAIL_ADDRESS") or "")
        mail_city = clean(P.get("MAIL_CITY") or P.get("MAILCITY") or "")
        mail_state = clean(P.get("MAIL_STATE") or P.get("MAILSTATE") or "") or "OH"
        mail_zip = clean(P.get("MAIL_ZIP") or P.get("MAILZIP") or "")
        parcel_id = clean(P.get("PARCEL_ID") or P.get("PARCELID") or P.get("PARID") or P.get("PID") or "")
        assessed = None
        for k in ["ASSESSED_VALUE", "ASSDVAL", "TOTAL_APPR", "TOTALAPPR"]:
            v = clean(P.get(k) or "")
            if v:
                try:
                    assessed = float(re.sub(r"[^0-9.]", "", v))
                    if assessed > 100: break
                except: pass
        luc = clean(P.get("LUC") or P.get("LAND_USE") or "")
        if not addr: continue
        key = norm_addr_key(addr)
        if not key: continue
        rec = {
            "parcel_id": parcel_id, "owner": owner.title(),
            "prop_address": addr.title(), "prop_city": city.title(), "prop_zip": zip_,
            "mail_address": mail_addr.title(), "mail_city": mail_city.title(),
            "mail_state": norm_state(mail_state) or "OH", "mail_zip": mail_zip,
            "assessed_value": assessed,
            "est_market_value": round(assessed / 0.35) if assessed and assessed > 100 else None,
            "luc": luc,
        }
        parcels[key] = rec
        for v in name_variants(owner):
            parcels[f"OWNER:{v}"] = rec

    addr_count = len([k for k in parcels if not k.startswith("OWNER:")])
    logging.info("Parcel data: %s addresses indexed", addr_count)
    return parcels

def match_parcel(owner: str, prop_address: str, parcels: Dict[str, dict]) -> Optional[dict]:
    if prop_address:
        key = norm_addr_key(prop_address)
        if key and key in parcels:
            return parcels[key]
    if owner:
        for v in name_variants(owner):
            k = f"OWNER:{v}"
            if k in parcels:
                return parcels[k]
    return None

def enrich(record: "LeadRecord", parcels: Dict[str, dict]) -> "LeadRecord":
    matched = match_parcel(record.owner, record.prop_address, parcels)
    if matched:
        record.prop_address = record.prop_address or matched.get("prop_address", "")
        record.prop_city = record.prop_city or matched.get("prop_city", "") or "Toledo"
        record.prop_zip = record.prop_zip or matched.get("prop_zip", "")
        record.mail_address = record.mail_address or matched.get("mail_address", "")
        record.mail_city = record.mail_city or matched.get("mail_city", "")
        record.mail_state = record.mail_state or matched.get("mail_state", "OH")
        record.mail_zip = record.mail_zip or matched.get("mail_zip", "")
        record.parcel_id = record.parcel_id or matched.get("parcel_id", "")
        record.luc = record.luc or matched.get("luc", "")
        if not record.assessed_value:
            record.assessed_value = matched.get("assessed_value")
        if not record.estimated_value:
            record.estimated_value = matched.get("est_market_value")
        record.match_method = "parcel_lookup"
        record.match_score = 0.9

    if not record.prop_city: record.prop_city = "Toledo"
    if not record.prop_state: record.prop_state = "OH"
    record.with_address = 1 if record.prop_address else 0
    record.is_absentee = is_absentee(record.prop_address, record.mail_address, record.mail_state)
    record.is_out_of_state = is_oos(record.mail_state)
    if record.is_absentee and "Absentee owner" not in record.flags:
        record.flags.append("Absentee owner")
    if record.is_out_of_state and "Out-of-state owner" not in record.flags:
        record.flags.append("Out-of-state owner")
    record.flags = list(dict.fromkeys(record.flags + cat_flags(record.doc_type, record.owner)))
    record = estimate_financials(record)
    record.score = score_record(record)
    return record

# ── SCRAPER 1: TLN Common Pleas daily filings (MAIN SOURCE) ───────────────
async def scrape_tln_common_pleas() -> List[LeadRecord]:
    """
    Toledo Legal News Common Pleas daily filings.
    Scrapes last 14 days of articles for foreclosures, liens, judgments.
    """
    records: List[LeadRecord] = []
    seen: set = set()

    logging.info("Scraping TLN Common Pleas...")

    # Get index page
    html = await pw_fetch(TLN_COMMON_PLEAS_URL, wait_ms=3000)
    if not html:
        logging.warning("TLN Common Pleas index empty")
        return records

    soup = BeautifulSoup(html, "lxml")
    save_debug("tln_cp_index.html", html[:5000])

    # Collect article links from index
    article_links = []
    for a in soup.select("a[href]"):
        href = clean(a.get("href", ""))
        if not href or any(x in href for x in ["facebook", "twitter", "mailto", "#", "signup", "login"]):
            continue
        if "article_" in href or "filings-received" in href:
            full = href if href.startswith("http") else urljoin(TLN_BASE, href)
            if "toledolegalnews.com" in full and full not in article_links:
                article_links.append(full)

    # Also generate date-based URLs for last 14 days
    for days_back in range(0, 15):
        d = (datetime.now() - timedelta(days=days_back)).strftime("%B-%-d-%Y").lower()
        url = f"{TLN_BASE}/courts/common_pleas/common-pleas-filings-received-on-{d}/"
        if url not in article_links:
            article_links.append(url)

    logging.info("TLN CP: %s URLs to scrape", len(article_links))

    for url in article_links[:20]:  # cap at 20 most recent
        try:
            art_html = await pw_fetch(url, wait_ms=2000)
            if not art_html or len(art_html) < 500:
                continue
            art_soup = BeautifulSoup(art_html, "lxml")
            text = art_soup.get_text(" ")

            if "404" in text[:300] or "not found" in text[:300].lower():
                continue

            # ---- PATTERN 1: Foreclosure "vs" format ----
            # CI2026XXXXX Plaintiff vs Defendant. Action for $X foreclosure of mtg on ADDRESS
            fc_pat = re.compile(
                r"(CI[0-9]{4}[0-9]+)\s+"        # case number
                r"(.{5,80}?)\s+vs\.?\s+"          # plaintiff
                r"(.{5,80}?)\.\s+"                # defendant
                r".*?(?:foreclosure of mtg on|property located at|premises known as)\s+"
                r"([0-9]{1,5}\s+[A-Za-z][A-Za-z0-9\s\.]{3,35}),"
                r"\s*([A-Za-z\s]+),\s*Ohio\s*([0-9]{5})?",
                re.IGNORECASE | re.DOTALL
            )
            for m in fc_pat.finditer(text):
                doc_num = clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                plaintiff = clean(m.group(2))
                defendant = clean(m.group(3)).title()
                prop_address = clean(m.group(4)).title()
                prop_city = clean(m.group(5)).title()
                prop_zip = clean(m.group(6)) if m.group(6) else ""
                amt_m = re.search(r"\$\s*([\d,]+(?:\.\d{2})?)", text[m.start():m.start() + 300])
                amt = parse_amount(amt_m.group(1)) if amt_m else None
                filed = try_parse_date(text[max(0, m.start() - 200):m.start() + 50]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                rec = LeadRecord(
                    doc_num=doc_num, doc_type="NOFC", filed=filed,
                    cat="NOFC", cat_label="Pre-foreclosure",
                    owner=defendant, grantee=plaintiff, amount=amt,
                    prop_address=prop_address, prop_city=prop_city,
                    prop_state="OH", prop_zip=prop_zip,
                    clerk_url=url,
                    flags=["Pre-foreclosure", "Lis pendens"],
                    distress_sources=["foreclosure", "lis_pendens"],
                )
                records.append(rec)

            # ---- PATTERN 2: Lien "vs" format ----
            # LN2026-XXXXX; Plaintiff vs Defendant
            ln_pat = re.compile(
                r"(LN[0-9]{4}[0-9\-]+)[;,\s]+"
                r"([^;,\n]{3,80}?)\s+vs\.?\s+"
                r"([^;,\n\.]{3,60}?)(?:[;,\.]|\s{2}|$)",
                re.IGNORECASE
            )
            for m in ln_pat.finditer(text):
                doc_num = clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                plaintiff = clean(m.group(2))
                owner = clean(m.group(3)).title()
                if not owner or len(owner) < 3: continue
                filed = try_parse_date(text[max(0, m.start() - 100):m.start() + 50]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                dt = infer_doc_type(plaintiff) or "LNFED"
                # Try to extract amount
                amt_m = re.search(r"\$\s*([\d,]+(?:\.\d{2})?)", text[m.start():m.start() + 200])
                amt = parse_amount(amt_m.group(1)) if amt_m else None
                rec = LeadRecord(
                    doc_num=doc_num, doc_type=dt, filed=filed,
                    cat=dt, cat_label=LEAD_TYPE_MAP.get(dt, dt),
                    owner=owner, grantee=plaintiff, amount=amt,
                    clerk_url=url,
                    flags=cat_flags(dt, owner),
                    distress_sources=[s for s in [classify_distress(dt)] if s],
                )
                records.append(rec)

            # ---- PATTERN 3: Semicolon-delimited lien list ----
            # LN2026-02722; $5,000; PERSONAL INCOME TAX; STATE OF OHIO; OWNER NAME; ADDRESS
            sc_pat = re.compile(
                r"(LN[0-9]{4}[0-9\-]+)[;, ]+"
                r"\$?([\d,\.]+)[;, ]+"
                r"([^;\n]{3,60})[;, ]+"
                r"([^;\n]{3,60})[;, ]+"
                r"([^;\n]{3,80})",
                re.IGNORECASE
            )
            for m in sc_pat.finditer(text):
                doc_num = clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                try: amt = float(m.group(2).replace(",", ""))
                except: amt = None
                if amt and amt > 10_000_000: continue
                plaintiff = clean(m.group(3))
                owner = clean(m.group(4)).title()
                addr_raw = clean(m.group(5))
                if not owner or len(owner) < 3: continue
                dt = infer_doc_type(plaintiff) or "LN"
                filed = try_parse_date(text[max(0, m.start() - 200):m.start() + 100]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                addr_m = re.search(
                    r"(\d{2,5}\s+[A-Z][A-Za-z\s\.]{3,30}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL|WAY|TER|CIR)\.?)",
                    addr_raw, re.IGNORECASE
                )
                prop_address = clean(addr_m.group(1)).title() if addr_m else ""
                city_m = re.search(
                    r"(TOLEDO|MAUMEE|SYLVANIA|OREGON|PERRYSBURG|WATERVILLE|WHITEHOUSE|HOLLAND|SWANTON)",
                    addr_raw, re.IGNORECASE
                )
                prop_city = clean(city_m.group(0)).title() if city_m else "Toledo"
                zip_m = re.search(r"(43\d{3})", addr_raw)
                prop_zip = zip_m.group(1) if zip_m else ""
                rec = LeadRecord(
                    doc_num=doc_num, doc_type=dt, filed=filed,
                    cat=dt, cat_label=LEAD_TYPE_MAP.get(dt, dt),
                    owner=owner, grantee=plaintiff, amount=amt,
                    prop_address=prop_address, prop_city=prop_city,
                    prop_state="OH", prop_zip=prop_zip,
                    clerk_url=url,
                    flags=cat_flags(dt, owner),
                    distress_sources=[s for s in [classify_distress(dt)] if s],
                )
                records.append(rec)

            await asyncio.sleep(1.5 + random.uniform(0, 1))

        except Exception as e:
            logging.warning("TLN CP article %s: %s", url[-50:], e)

    logging.info("TLN Common Pleas: %s records", len(records))
    return records

# ── SCRAPER 2: Sheriff Sale Auction site ───────────────────────────────────
async def scrape_sheriff_sales() -> List[LeadRecord]:
    """
    Scrape lucas.sheriffsaleauction.ohio.gov for upcoming sheriff sales.
    All properties are listed publicly - no login needed.
    """
    records: List[LeadRecord] = []
    logging.info("Scraping sheriff sales...")

    # Get auction calendar to find upcoming sale dates
    cal_url = "https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=USER&zmethod=CALENDAR"
    html = await pw_fetch(cal_url, wait_ms=4000)
    if not html:
        logging.warning("Sheriff auction calendar empty")
        return records

    save_debug("sheriff_calendar.html", html[:8000])
    soup = BeautifulSoup(html, "lxml")

    # Find auction date links
    auction_urls = []
    for a in soup.select("a[href]"):
        href = clean(a.get("href", ""))
        if "AUCTIONDATE" in href.upper() or "PREVIEW" in href.upper() or "AUCTION" in href.upper():
            full = href if href.startswith("http") else f"https://lucas.sheriffsaleauction.ohio.gov{href}"
            if full not in auction_urls:
                auction_urls.append(full)

    # Also try direct date-based URLs for next 30 days
    for days_ahead in range(0, 31):
        d = (datetime.now() + timedelta(days=days_ahead)).strftime("%m/%d/%Y")
        url = f"https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={quote(d)}"
        if url not in auction_urls:
            auction_urls.append(url)

    logging.info("Sheriff auction URLs: %s", len(auction_urls[:15]))

    seen = set()
    for url in auction_urls[:15]:
        try:
            html = await pw_fetch(url, wait_ms=3000)
            if not html or len(html) < 500: continue
            soup2 = BeautifulSoup(html, "lxml")
            save_debug("sheriff_auction_page.html", html[:8000])

            # Look for property listings - RealAuction format
            # Each property has case#, address, appraised value, opening bid
            for prop_div in soup2.select(".AUCTION_ITEM, .property-item, tr"):
                text = clean(prop_div.get_text(" "))
                if len(text) < 20: continue

                # Case number
                case_m = re.search(r"(CI[0-9]{4}[0-9\-]+|TF[0-9]+|[0-9]{4}CV[0-9]+)", text, re.IGNORECASE)
                doc_num = clean(case_m.group(1)) if case_m else ""
                if not doc_num: continue
                if doc_num in seen: continue
                seen.add(doc_num)

                # Address
                addr_m = re.search(
                    r"(\d{2,5}\s+[A-Z][A-Za-z\s\.]{3,35}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL|WAY|TER|CIR)\.?)",
                    text, re.IGNORECASE
                )
                prop_address = clean(addr_m.group(1)).title() if addr_m else ""

                # Amount / appraised value
                amt_m = re.search(r"Appraised[:\s]*\$?([\d,]+)", text, re.IGNORECASE)
                if not amt_m:
                    amt_m = re.search(r"\$\s*([\d,]+(?:\.\d{2})?)", text)
                amt = parse_amount(amt_m.group(1)) if amt_m else None

                # Sale date
                date_m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
                sale_date = date_m.group(1) if date_m else ""
                if sale_date:
                    try:
                        filed = datetime.strptime(sale_date, "%m/%d/%Y").date().isoformat()
                    except:
                        filed = datetime.now().date().isoformat()
                else:
                    filed = datetime.now().date().isoformat()

                # City/zip
                city_m = re.search(
                    r"(TOLEDO|MAUMEE|SYLVANIA|OREGON|PERRYSBURG|WATERVILLE|WHITEHOUSE|HOLLAND|SWANTON)",
                    text, re.IGNORECASE
                )
                prop_city = clean(city_m.group(0)).title() if city_m else "Toledo"
                zip_m = re.search(r"(43\d{3})", text)
                prop_zip = zip_m.group(1) if zip_m else ""

                # Parcel ID
                parcel_m = re.search(r"Parcel[:\s#]*([A-Z0-9\-]{8,20})", text, re.IGNORECASE)
                parcel = clean(parcel_m.group(1)) if parcel_m else ""

                # Get property link
                prop_link = prop_div.find("a", href=True)
                prop_url = ""
                if prop_link:
                    href = clean(prop_link.get("href", ""))
                    prop_url = href if href.startswith("http") else f"https://lucas.sheriffsaleauction.ohio.gov{href}"

                rec = LeadRecord(
                    doc_num=doc_num, doc_type="SHERIFF", filed=filed,
                    cat="SHERIFF", cat_label="Sheriff Sale",
                    amount=amt, appraised_value=amt,
                    prop_address=prop_address, prop_city=prop_city,
                    prop_state="OH", prop_zip=prop_zip,
                    parcel_id=parcel, sheriff_sale_date=sale_date,
                    clerk_url=prop_url or url,
                    flags=["Sheriff sale scheduled", "Pre-foreclosure", "Hot Stack"],
                    distress_sources=["sheriff_sale", "foreclosure"],
                    distress_count=2, hot_stack=True,
                    with_address=1 if prop_address else 0,
                )
                records.append(rec)

            await asyncio.sleep(2)
        except Exception as e:
            logging.warning("Sheriff auction %s: %s", url[-60:], e)

    logging.info("Sheriff sales: %s", len(records))
    return records

# ── SCRAPER 3: Lucas County Foreclosure Search (free public app) ───────────
async def scrape_lucas_foreclosure_search() -> List[LeadRecord]:
    """
    Lucas County official foreclosure search app.
    http://lcapps.co.lucas.oh.us/foreclosure/search.aspx
    Searchable by date range - completely free/public.
    """
    records: List[LeadRecord] = []
    logging.info("Scraping Lucas County foreclosure search...")

    try:
        url = "http://lcapps.co.lucas.oh.us/foreclosure/search.aspx"
        html = await pw_fetch(url, wait_ms=3000)
        if not html:
            return records
        save_debug("lc_foreclosure_search.html", html[:5000])
        soup = BeautifulSoup(html, "lxml")

        # This is an ASP.NET app - need to POST with date range
        # Get viewstate tokens
        vs = soup.find("input", {"id": "__VIEWSTATE"})
        evv = soup.find("input", {"id": "__EVENTVALIDATION"})
        vsgen = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})

        today = datetime.now().date()
        start = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
        end = today.strftime("%m/%d/%Y")

        # Search by date range
        data = {
            "__VIEWSTATE": vs["value"] if vs else "",
            "__EVENTVALIDATION": evv["value"] if evv else "",
            "__VIEWSTATEGENERATOR": vsgen["value"] if vsgen else "",
            "ctl00$ContentPlaceHolder1$txtStartDate": start,
            "ctl00$ContentPlaceHolder1$txtEndDate": end,
            "ctl00$ContentPlaceHolder1$btnSearch": "Search",
        }

        r = retry_post(url, data)
        result_soup = BeautifulSoup(r.text, "lxml")
        save_debug("lc_foreclosure_results.html", r.text[:10000])

        # Parse results table
        for row in result_soup.select("tr"):
            cells = [clean(td.get_text(" ")) for td in row.select("td")]
            if not cells or len(cells) < 3: continue
            rt = " ".join(cells)
            if not rt or len(rt) < 10: continue
            if any(x in rt.upper() for x in ["CASE", "PARCEL", "ADDRESS", "DATE", "PLAINTIFF"]) and len(rt) < 100:
                continue

            case_m = re.search(r"(CI[0-9]{4}[0-9\-]+|TF[0-9]+)", rt)
            doc_num = clean(case_m.group(1)) if case_m else f"FC-{len(records)+1:04d}"
            addr_m = re.search(
                r"(\d{2,5}\s+[A-Z][A-Za-z\s\.]{3,30}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL)\.?)",
                rt, re.IGNORECASE
            )
            prop_address = clean(addr_m.group(1)).title() if addr_m else ""
            amt_m = re.search(r"\$([\d,]+(?:\.\d{2})?)", rt)
            amt = parse_amount(amt_m.group(1)) if amt_m else None
            filed = try_parse_date(rt) or datetime.now().date().isoformat()
            if not is_recent(filed): continue

            link = row.find("a", href=True)
            href = clean(link.get("href", "")) if link else ""
            clerk_url = urljoin(url, href) if href else url

            # Parse plaintiff/defendant
            vs_m = re.search(r"(.{5,60}?)\s+vs\.?\s+(.{5,60}?)(?:\.|$)", rt, re.IGNORECASE)
            plaintiff = clean(vs_m.group(1)) if vs_m else ""
            defendant = clean(vs_m.group(2)).title() if vs_m else ""

            is_tax = "TF" in doc_num
            dt = "TAX" if is_tax else "NOFC"
            rec = LeadRecord(
                doc_num=doc_num, doc_type=dt, filed=filed,
                cat=dt, cat_label=LEAD_TYPE_MAP.get(dt, dt),
                owner=defendant, grantee=plaintiff, amount=amt,
                prop_address=prop_address, prop_city="Toledo", prop_state="OH",
                clerk_url=clerk_url,
                flags=["Pre-foreclosure", "Tax lien"] if is_tax else ["Pre-foreclosure"],
                distress_sources=["foreclosure"],
            )
            records.append(rec)

    except Exception as e:
        logging.warning("Lucas foreclosure search failed: %s", e)

    logging.info("Lucas foreclosure search: %s", len(records))
    return records

# ── SCRAPER 4: TLN Probate articles ───────────────────────────────────────
async def scrape_tln_probate() -> List[LeadRecord]:
    """Scrape TLN Probate Court section for estate filings."""
    records: List[LeadRecord] = []
    logging.info("Scraping TLN Probate...")
    try:
        html = await pw_fetch(TLN_PROBATE_URL, wait_ms=3000)
        if not html: return records
        soup = BeautifulSoup(html, "lxml")
        save_debug("tln_probate.html", html[:8000])
        text = soup.get_text(" ")

        # Collect article links
        links = []
        for a in soup.select("a[href]"):
            href = clean(a.get("href", ""))
            if "article_" in href or "probate" in href:
                full = href if href.startswith("http") else urljoin(TLN_BASE, href)
                if "toledolegalnews.com" in full:
                    links.append(full)

        all_text = text
        for link in links[:10]:
            try:
                art_html = await pw_fetch(link, wait_ms=2000)
                if art_html:
                    all_text += " " + BeautifulSoup(art_html, "lxml").get_text(" ")
                await asyncio.sleep(1)
            except:
                pass

        # Pattern: "Estate of [Name], deceased" or "In re Estate of [Name]"
        estate_pat = re.compile(
            r"(?:Estate\s+of|In\s+re\s+Estate\s+of)\s+"
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})"
            r"(?:,\s*(?:deceased|Deceased|DECEASED))?",
            re.IGNORECASE
        )
        seen = set()
        for m in estate_pat.finditer(all_text):
            name = clean(m.group(1))
            if name in seen or len(name) < 5: continue
            seen.add(name)
            surrounding = all_text[max(0, m.start() - 50):m.end() + 400]
            filed = try_parse_date(surrounding) or datetime.now().date().isoformat()
            if not is_recent(filed): continue

            # Try to get executor name
            exec_m = re.search(r"(?:executor|administrator|fiduciary)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)", surrounding, re.IGNORECASE)
            executor = clean(exec_m.group(1)) if exec_m else ""

            rec = LeadRecord(
                doc_num=f"PRO-{name.replace(' ','-')}-{len(records)+1}",
                doc_type="PRO", filed=filed, cat="PRO", cat_label="Probate / Estate",
                owner=name.title(), decedent_name=name.title(),
                executor_name=executor.title(), is_inherited=True,
                flags=["Probate / estate", "Inherited property"],
                distress_sources=["probate"], distress_count=1,
                clerk_url=TLN_PROBATE_URL, match_method="probate_name",
            )
            records.append(rec)

    except Exception as e:
        logging.warning("Probate scrape failed: %s", e)

    logging.info("Probate: %s", len(records))
    return records

# ── SCRAPER 5: TLN Domestic/Divorce filings ───────────────────────────────
async def scrape_tln_divorces() -> List[LeadRecord]:
    """Scrape TLN Domestic Relations for divorce filings."""
    records: List[LeadRecord] = []
    logging.info("Scraping divorces...")
    seen = set()

    for url in [TLN_DOMESTIC_URL]:
        try:
            html = await pw_fetch(url, wait_ms=3000)
            if not html: continue
            soup = BeautifulSoup(html, "lxml")
            save_debug("tln_domestic.html", html[:5000])

            # Get article links and scrape them
            links = []
            for a in soup.select("a[href]"):
                href = clean(a.get("href", ""))
                if "article_" in href or "domestic" in href or "filings" in href:
                    full = href if href.startswith("http") else urljoin(TLN_BASE, href)
                    if "toledolegalnews.com" in full:
                        links.append(full)

            all_texts = [soup.get_text(" ")]
            for link in links[:8]:
                try:
                    ah = await pw_fetch(link, wait_ms=1500)
                    if ah:
                        all_texts.append(BeautifulSoup(ah, "lxml").get_text(" "))
                    await asyncio.sleep(1)
                except:
                    pass

            for text in all_texts:
                # DR case numbers: DR2026XXXXXX
                dr_pat = re.compile(
                    r"(DR[0-9]{4}[0-9\-]+|DM[0-9]+)[;,\s]+"
                    r"([A-Z][A-Za-z\s,\.]{3,40}?)\s+vs\.?\s+"
                    r"([A-Z][A-Za-z\s,\.]{3,40}?)(?:[;,\.]|\s{2}|$)",
                    re.IGNORECASE
                )
                for m in dr_pat.finditer(text):
                    doc_num = clean(m.group(1))
                    if doc_num in seen: continue
                    seen.add(doc_num)
                    plaintiff = clean(m.group(2)).title()
                    defendant = clean(m.group(3)).title()
                    filed = try_parse_date(text[max(0, m.start() - 100):m.start() + 50]) or datetime.now().date().isoformat()
                    if not is_recent(filed): continue
                    rec = LeadRecord(
                        doc_num=doc_num, doc_type="DIVORCE", filed=filed,
                        cat="DIVORCE", cat_label="Divorce Filing",
                        owner=plaintiff, grantee=defendant,
                        clerk_url=url,
                        flags=["Divorce filing"],
                        distress_sources=["divorce"],
                    )
                    records.append(rec)

                # Also catch "DIVORCE" keyword near names
                div_pat = re.compile(
                    r"(?:dissolution|divorce)\s+(?:of marriage\s+)?(?:between\s+)?"
                    r"([A-Z][A-Za-z\s]{3,30})\s+and\s+([A-Z][A-Za-z\s]{3,30})",
                    re.IGNORECASE
                )
                for m in div_pat.finditer(text):
                    key = f"DIV-{clean(m.group(1))[:15]}"
                    if key in seen: continue
                    seen.add(key)
                    filed = try_parse_date(text[max(0, m.start() - 100):m.start() + 50]) or datetime.now().date().isoformat()
                    if not is_recent(filed): continue
                    rec = LeadRecord(
                        doc_num=f"DIVORCE-{len(records)+1:04d}",
                        doc_type="DIVORCE", filed=filed,
                        cat="DIVORCE", cat_label="Divorce Filing",
                        owner=clean(m.group(1)).title(),
                        grantee=clean(m.group(2)).title(),
                        clerk_url=url,
                        flags=["Divorce filing"],
                        distress_sources=["divorce"],
                    )
                    records.append(rec)

        except Exception as e:
            logging.warning("Divorce scrape %s: %s", url, e)

    logging.info("Divorces: %s", len(records))
    return records

# ── SCRAPER 6: Ohio SOS UCC / Mechanic Liens ──────────────────────────────
async def scrape_ucc_mechanic_liens() -> List[LeadRecord]:
    """
    Ohio Secretary of State UCC search for mechanic liens / commercial liens.
    Also checks Lucas County Recorder for NOC (Notice of Commencement) filings.
    """
    records: List[LeadRecord] = []
    logging.info("Scraping UCC / mechanic liens...")

    try:
        # Ohio SOS UCC - lien filings searchable by county
        ucc_url = "https://www5.sos.state.oh.us/ords/f?p=100:1::::::"
        html = await pw_fetch(ucc_url, wait_ms=3000)
        if html:
            save_debug("sos_ucc.html", html[:5000])
            soup = BeautifulSoup(html, "lxml")
            # Parse whatever UCC data is visible
            text = soup.get_text(" ")
            # Look for filing numbers and debtor names
            ucc_pat = re.compile(
                r"([0-9]{8,12}OH)\s+([A-Z][A-Za-z\s,\.]{3,50})\s+(?:LUCAS|TOLEDO|OH)",
                re.IGNORECASE
            )
            seen = set()
            for m in ucc_pat.finditer(text):
                doc_num = clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                owner = clean(m.group(2)).title()
                rec = LeadRecord(
                    doc_num=doc_num, doc_type="LNMECH",
                    filed=datetime.now().date().isoformat(),
                    cat="LNMECH", cat_label="Mechanic Lien",
                    owner=owner, clerk_url=ucc_url,
                    flags=["Mechanic lien"],
                    distress_sources=["mechanic_lien"],
                )
                records.append(rec)

        # Lucas County Recorder - Notice of Commencement filings (free public search)
        # NOC filings signal active construction = potential mechanic lien targets
        recorder_search_url = "https://lucas.dts-oh.com/PaxWorld5/Search/Index"
        # DTS requires account for full access - try public search endpoint
        try:
            r = retry_get(recorder_search_url, timeout=20)
            if r and "NOC" in r.text.upper():
                soup = BeautifulSoup(r.text, "lxml")
                # Parse any visible NOC records
                for row in soup.select("tr"):
                    cells = [clean(td.get_text()) for td in row.select("td")]
                    if not cells or len(cells) < 3: continue
                    rt = " ".join(cells)
                    if "NOC" in rt.upper() or "COMMENCEMENT" in rt.upper():
                        doc_m = re.search(r"([0-9]{6,10})", rt)
                        doc_num = clean(doc_m.group(1)) if doc_m else f"NOC-{len(records)+1}"
                        owner_m = re.search(r"([A-Z][A-Za-z\s]{5,40}(?:LLC|INC|CORP)?)", rt)
                        owner = clean(owner_m.group(1)).title() if owner_m else ""
                        addr_m = re.search(r"(\d{3,5}\s+[A-Za-z\s]{5,30})", rt)
                        prop_address = clean(addr_m.group(1)).title() if addr_m else ""
                        rec = LeadRecord(
                            doc_num=doc_num, doc_type="NOC",
                            filed=datetime.now().date().isoformat(),
                            cat="NOC", cat_label="Notice of Commencement",
                            owner=owner, prop_address=prop_address,
                            prop_city="Toledo", prop_state="OH",
                            clerk_url=recorder_search_url,
                            flags=["Mechanic lien"],
                            distress_sources=["mechanic_lien"],
                        )
                        records.append(rec)
        except:
            pass

    except Exception as e:
        logging.warning("UCC/mechanic lien scrape failed: %s", e)

    logging.info("UCC/Mechanic liens: %s", len(records))
    return records

# ── SCRAPER 7: PACER / Federal Court - Federal Tax Liens & Bankruptcies ───
async def scrape_federal_liens() -> List[LeadRecord]:
    """
    PACER RSS feeds for Northern District of Ohio - federal liens, bankruptcies.
    These are public RSS feeds requiring no login.
    """
    records: List[LeadRecord] = []
    logging.info("Scraping federal liens / bankruptcies...")

    federal_urls = [
        # Northern District Ohio bankruptcy RSS
        "https://ecf.ohnb.uscourts.gov/cgi-bin/rss_outside.pl",
        # Public PACER case locator
        "https://pcl.uscourts.gov/pcl/pages/search/results/bankruptcies.jsf",
    ]

    try:
        for url in federal_urls:
            try:
                r = retry_get(url, timeout=20)
                if not r or len(r.text) < 200: continue
                soup = BeautifulSoup(r.text, "lxml")

                # RSS item format
                for item in soup.select("item"):
                    title = clean(item.find("title").get_text() if item.find("title") else "")
                    link = clean(item.find("link").get_text() if item.find("link") else "")
                    desc = clean(item.find("description").get_text() if item.find("description") else "")
                    pubdate = clean(item.find("pubdate").get_text() if item.find("pubdate") else "")
                    full_text = f"{title} {desc}"

                    # Filter for Toledo/Lucas County
                    if not any(x in full_text.upper() for x in ["TOLEDO", "LUCAS", "43601", "43602", "43603", "43604", "43605", "43606", "43607", "43608", "43609", "43610", "43611", "43612", "43613", "43614", "43615", "43616", "43617", "43618", "43619", "43620"]):
                        continue

                    dt = infer_doc_type(full_text) or "BK"
                    filed = try_parse_date(pubdate) or datetime.now().date().isoformat()
                    if not is_recent(filed): continue

                    # Extract debtor name
                    name_m = re.search(r"(?:In re|Debtor)[:\s]+([A-Z][A-Za-z\s,\.]{3,50})", full_text, re.IGNORECASE)
                    owner = clean(name_m.group(1)).title() if name_m else title[:50].title()

                    amt_m = re.search(r"\$([\d,]+)", full_text)
                    amt = parse_amount(amt_m.group(1)) if amt_m else None

                    case_m = re.search(r"([0-9]{2}-[0-9]{5}|[A-Z]{2}[0-9]{2}-[0-9]{4,})", full_text)
                    doc_num = clean(case_m.group(1)) if case_m else f"FED-{len(records)+1:04d}"

                    rec = LeadRecord(
                        doc_num=doc_num, doc_type=dt, filed=filed,
                        cat=dt, cat_label=LEAD_TYPE_MAP.get(dt, dt),
                        owner=owner, amount=amt,
                        clerk_url=link or url,
                        flags=cat_flags(dt, owner),
                        distress_sources=[s for s in [classify_distress(dt)] if s],
                    )
                    records.append(rec)
            except Exception as e:
                logging.warning("Federal URL %s: %s", url[:60], e)

    except Exception as e:
        logging.warning("Federal liens failed: %s", e)

    logging.info("Federal liens/BK: %s", len(records))
    return records

# ── SCRAPER 8: Toledo Municipal Court - Code Violations / Evictions ────────
async def scrape_municipal_court() -> List[LeadRecord]:
    """
    Toledo Municipal Court - code violations and evictions.
    Public case search at egov.toledo.gov
    """
    records: List[LeadRecord] = []
    logging.info("Scraping Toledo Municipal Court...")

    try:
        # Toledo code enforcement
        code_urls = [
            "https://egov.toledo.gov/trcis/SearchListing.aspx",
            "https://toledo.oh.gov/services/neighborhoods/code-enforcement",
        ]

        for url in code_urls:
            try:
                html = await pw_fetch(url, wait_ms=3000)
                if not html or len(html) < 500: continue
                save_debug("toledo_code.html", html[:5000])
                soup = BeautifulSoup(html, "lxml")
                text = soup.get_text(" ")

                # Look for violation records
                viol_pat = re.compile(
                    r"(\d{4}[A-Z]{2,4}\d{4,8})\s+"
                    r"(\d{3,5}\s+[A-Za-z\s\.]{5,30})\s+"
                    r"(Toledo|Maumee|Sylvania|Oregon)",
                    re.IGNORECASE
                )
                seen = set()
                for m in viol_pat.finditer(text):
                    doc_num = clean(m.group(1))
                    if doc_num in seen: continue
                    seen.add(doc_num)
                    prop_address = clean(m.group(2)).title()
                    prop_city = clean(m.group(3)).title()
                    filed = try_parse_date(text[max(0, m.start() - 100):m.start() + 100]) or datetime.now().date().isoformat()
                    if not is_recent(filed): continue
                    rec = LeadRecord(
                        doc_num=doc_num, doc_type="CODEVIOLATION",
                        filed=filed, cat="CODEVIOLATION", cat_label="Code Violation",
                        prop_address=prop_address, prop_city=prop_city, prop_state="OH",
                        clerk_url=url,
                        flags=["Code violation"],
                        distress_sources=["code_violation"],
                    )
                    records.append(rec)
                if records: break
            except Exception as e:
                logging.warning("Municipal court %s: %s", url[:60], e)

    except Exception as e:
        logging.warning("Municipal court failed: %s", e)

    logging.info("Code violations/evictions: %s", len(records))
    return records

# ── SCRAPER 9: Lucas County Tax Delinquent ────────────────────────────────
async def scrape_tax_delinquent() -> List[LeadRecord]:
    """
    Lucas County tax delinquent / tax foreclosure list.
    TF (tax foreclosure) case numbers from Common Pleas.
    Also checks Treasurer's published delinquent lists.
    """
    records: List[LeadRecord] = []
    logging.info("Scraping tax delinquent...")

    tax_urls = [
        "https://www.lucascountytreasurer.org/delinquent-taxes",
        "https://www.lucascountytreasurer.org/tax-foreclosure",
        "https://co.lucas.oh.us/500/Treasurer",
        # TLN foreclosure notices section (tax foreclosures appear here too)
        "https://www.toledolegalnews.com/legal_notices/foreclosures/",
    ]

    for url in tax_urls:
        try:
            html = await pw_fetch(url, wait_ms=3000)
            if not html or len(html) < 500: continue
            soup = BeautifulSoup(html, "lxml")
            save_debug(f"tax_delin_{len(records)}.html", html[:5000])
            text = soup.get_text(" ")

            # TF case numbers
            tf_pat = re.compile(
                r"(TF[0-9]{4}[0-9\-]+|TF\s*[0-9]{6,})\s+"
                r"(.{10,80}?)\s+\$?([\d,]+(?:\.\d{2})?)?",
                re.IGNORECASE
            )
            seen = set()
            for m in tf_pat.finditer(text):
                doc_num = re.sub(r"\s+", "", clean(m.group(1)))
                if doc_num in seen: continue
                seen.add(doc_num)
                details = clean(m.group(2))
                try: amt = float(m.group(3).replace(",", "")) if m.group(3) else None
                except: amt = None
                addr_m = re.search(
                    r"(\d{2,5}\s+[A-Za-z][A-Za-z\s\.]{3,25}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL)\.?)",
                    details, re.IGNORECASE
                )
                prop_address = clean(addr_m.group(1)).title() if addr_m else ""
                parcel_m = re.search(r"([A-Z0-9]{2}-\d{6}|\d{2}-\d{6}-\d{3})", details)
                parcel = clean(parcel_m.group(1)) if parcel_m else ""
                filed = try_parse_date(text[max(0, m.start() - 200):m.start() + 100]) or datetime.now().date().isoformat()
                rec = LeadRecord(
                    doc_num=doc_num, doc_type="TAX",
                    filed=filed, cat="TAX", cat_label="Tax Delinquent",
                    amount=amt, prop_address=prop_address,
                    prop_city="Toledo", prop_state="OH",
                    parcel_id=parcel, clerk_url=url,
                    flags=["Tax delinquent", "Tax lien"],
                    distress_sources=["tax_delinquent"],
                )
                records.append(rec)

            # Also look for article links on TLN foreclosure page
            if "toledolegalnews.com" in url:
                for a in soup.select("a[href]"):
                    href = clean(a.get("href", ""))
                    if "article_" in href:
                        full = href if href.startswith("http") else urljoin(TLN_BASE, href)
                        try:
                            art_html = await pw_fetch(full, wait_ms=2000)
                            if art_html:
                                art_text = BeautifulSoup(art_html, "lxml").get_text(" ")
                                for m in tf_pat.finditer(art_text):
                                    doc_num = re.sub(r"\s+", "", clean(m.group(1)))
                                    if doc_num in seen: continue
                                    seen.add(doc_num)
                                    try: amt = float(m.group(3).replace(",", "")) if m.group(3) else None
                                    except: amt = None
                                    rec = LeadRecord(
                                        doc_num=doc_num, doc_type="TAX",
                                        filed=datetime.now().date().isoformat(),
                                        cat="TAX", cat_label="Tax Delinquent",
                                        amount=amt, prop_city="Toledo", prop_state="OH",
                                        clerk_url=full,
                                        flags=["Tax delinquent", "Tax lien"],
                                        distress_sources=["tax_delinquent"],
                                    )
                                    records.append(rec)
                            await asyncio.sleep(1)
                        except:
                            pass

            if records: break
        except Exception as e:
            logging.warning("Tax delinquent %s: %s", url[:60], e)

    logging.info("Tax delinquent: %s", len(records))
    return records

# ── Cross-stacking & deduplication ────────────────────────────────────────
def cross_stack(records: List[LeadRecord]) -> List[LeadRecord]:
    addr_map: Dict[str, List[int]] = defaultdict(list)
    for i, r in enumerate(records):
        if r.prop_address:
            key = norm_addr_key(r.prop_address)
            if key: addr_map[key].append(i)

    stacked = 0
    for key, idxs in addr_map.items():
        if len(idxs) < 2: continue
        all_sources: set = set()
        for i in idxs: all_sources.update(records[i].distress_sources or [])
        if len(all_sources) < 2: continue
        for i in idxs:
            r = records[i]
            r.distress_sources = list(set(list(r.distress_sources or []) + list(all_sources)))
            r.distress_count = len(r.distress_sources)
            r.hot_stack = True
            if "Hot Stack" not in " ".join(r.flags): r.flags.append("Hot Stack")
            if "Cross-List Match" not in " ".join(r.flags): r.flags.append("Cross-List Match")
            r = estimate_financials(r); r.score = score_record(r); records[i] = r
        stacked += 1

    logging.info("Cross-stacked %s property groups", stacked)
    return records

def dedupe(records: List[LeadRecord]) -> List[LeadRecord]:
    final, seen = [], set()
    for r in records:
        nd = re.sub(r"^(PCF1|PCF2)-", "", clean(r.doc_num).upper())
        key = (nd, clean(r.doc_type).upper(), clean(r.owner)[:20].upper(), clean(r.filed))
        if key in seen: continue
        seen.add(key); final.append(r)
    return final

# ── Output helpers ─────────────────────────────────────────────────────────
def split_name(n: str):
    parts = clean(n).split()
    if not parts: return "", ""
    if len(parts) == 1: return parts[0], ""
    return parts[0], " ".join(parts[1:])

def write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logging.info("Wrote %s (%s records)", path, payload.get("total", "?"))

def build_payload(records: List[LeadRecord]) -> dict:
    return {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "date_range": {
            "from": (datetime.now() - timedelta(days=LOOKBACK_DAYS)).date().isoformat(),
            "to": datetime.now().date().isoformat(),
        },
        "total": len(records),
        "with_address": sum(1 for r in records if r.prop_address),
        "hot_stack_count": sum(1 for r in records if r.hot_stack),
        "sheriff_sale_count": sum(1 for r in records if r.doc_type == "SHERIFF"),
        "probate_count": sum(1 for r in records if r.doc_type == "PRO"),
        "tax_delinquent_count": sum(1 for r in records if r.doc_type == "TAX"),
        "foreclosure_count": sum(1 for r in records if r.doc_type in {"NOFC", "LP"}),
        "lien_count": sum(1 for r in records if r.doc_type in {"LN","LNMECH","LNFED","LNIRS","LNCORPTX"}),
        "absentee_count": sum(1 for r in records if r.is_absentee),
        "out_of_state_count": sum(1 for r in records if r.is_out_of_state),
        "subject_to_count": sum(1 for r in records if r.subject_to_score >= 50),
        "divorce_count": sum(1 for r in records if r.doc_type == "DIVORCE"),
        "bankruptcy_count": sum(1 for r in records if r.doc_type == "BK"),
        "records": [asdict(r) for r in records],
    }

def write_category_json(records: List[LeadRecord]):
    categories = {
        "hot_stack":        [r for r in records if r.hot_stack],
        "sheriff_sales":    [r for r in records if r.doc_type == "SHERIFF"],
        "probate":          [r for r in records if r.doc_type == "PRO"],
        "tax_delinquent":   [r for r in records if r.doc_type == "TAX"],
        "foreclosure":      [r for r in records if r.doc_type in {"NOFC","LP","TAXDEED"}],
        "liens":            [r for r in records if r.doc_type in {"LN","LNMECH","LNFED","LNIRS","LNCORPTX","MEDLN"}],
        "absentee":         [r for r in records if r.is_absentee],
        "out_of_state":     [r for r in records if r.is_out_of_state],
        "subject_to":       [r for r in records if r.subject_to_score >= 50],
        "divorces":         [r for r in records if r.doc_type == "DIVORCE"],
        "bankruptcy":       [r for r in records if r.doc_type == "BK"],
        "code_violations":  [r for r in records if r.doc_type == "CODEVIOLATION"],
    }
    descs = {
        "hot_stack":       "2+ distress signals - highest priority",
        "sheriff_sales":   "Properties scheduled for sheriff auction",
        "probate":         "Estate / probate filings - inherited properties",
        "tax_delinquent":  "Tax delinquent / tax foreclosure filings",
        "foreclosure":     "Active foreclosure / lis pendens",
        "liens":           "Judgment, federal, mechanic liens",
        "absentee":        "Absentee owner - mailing differs from property",
        "out_of_state":    "Out-of-state owner",
        "subject_to":      "Subject-To candidates (score 50+)",
        "divorces":        "Divorce / dissolution filings",
        "bankruptcy":      "Bankruptcy filings",
        "code_violations": "Code violations / housing orders",
    }
    for cat, recs in categories.items():
        recs_s = sorted(recs, key=lambda r: (r.hot_stack, r.distress_count, r.subject_to_score, r.score), reverse=True)
        payload = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source": SOURCE_NAME,
            "category": cat, "description": descs[cat],
            "total": len(recs_s),
            "records": [asdict(r) for r in recs_s],
        }
        for path in [DATA_DIR / f"{cat}.json", DASHBOARD_DIR / f"{cat}.json"]:
            write_json(path, payload)

def write_csv(records: List[LeadRecord], csv_path: Path):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Subject-To Score","Motivated Seller Flags","Distress Sources","Distress Count",
        "Hot Stack","Absentee Owner","Out-of-State Owner","Inherited",
        "Assessed Value","Est Market Value","Est Equity","Est Arrears","Est Payoff","Mortgage Signals",
        "Parcel ID","LUC Code","Match Method","Source","Public Records URL",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in records:
            fn, ln = split_name(r.owner)
            w.writerow({
                "First Name": fn, "Last Name": ln,
                "Mailing Address": r.mail_address, "Mailing City": r.mail_city,
                "Mailing State": r.mail_state, "Mailing Zip": r.mail_zip,
                "Property Address": r.prop_address, "Property City": r.prop_city,
                "Property State": r.prop_state, "Property Zip": r.prop_zip,
                "Lead Type": r.cat_label, "Document Type": r.doc_type,
                "Date Filed": r.filed, "Document Number": r.doc_num,
                "Amount/Debt Owed": f"${r.amount:,.2f}" if r.amount else "",
                "Seller Score": r.score, "Subject-To Score": r.subject_to_score,
                "Motivated Seller Flags": "; ".join(r.flags),
                "Distress Sources": "; ".join(r.distress_sources),
                "Distress Count": r.distress_count,
                "Hot Stack": "YES" if r.hot_stack else "",
                "Absentee Owner": "YES" if r.is_absentee else "",
                "Out-of-State Owner": "YES" if r.is_out_of_state else "",
                "Inherited": "YES" if r.is_inherited else "",
                "Assessed Value": f"${r.assessed_value:,.0f}" if r.assessed_value else "",
                "Est Market Value": f"${r.estimated_value:,.0f}" if r.estimated_value else "",
                "Est Equity": f"${r.est_equity:,.0f}" if r.est_equity is not None else "",
                "Est Arrears": f"${r.est_arrears:,.0f}" if r.est_arrears else "",
                "Est Payoff": f"${r.est_payoff:,.0f}" if r.est_payoff else "",
                "Mortgage Signals": "; ".join(r.mortgage_signals),
                "Parcel ID": r.parcel_id, "LUC Code": r.luc,
                "Match Method": r.match_method,
                "Source": SOURCE_NAME, "Public Records URL": r.clerk_url,
            })
    logging.info("Wrote CSV: %s (%s rows)", csv_path, len(records))

# ── Main ───────────────────────────────────────────────────────────────────
async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-csv", default=str(DEFAULT_ENRICHED_CSV_PATH))
    args = ap.parse_args()

    ensure_dirs()
    log_setup()
    logging.info("=== Toledo / Lucas County - Motivated Seller Intelligence ===")
    logging.info("Lookback: %s days | Started: %s", LOOKBACK_DAYS, datetime.now().isoformat())

    # 1. Load parcel data
    parcels = load_parcel_data()

    # 2. Run all scrapers concurrently where possible
    logging.info("Starting all scrapers...")

    # Run in order to avoid overwhelming TLN with too many concurrent Playwright sessions
    cp_records    = await scrape_tln_common_pleas()
    sheriff_recs  = await scrape_sheriff_sales()
    fc_recs       = await scrape_lucas_foreclosure_search()
    probate_recs  = await scrape_tln_probate()
    divorce_recs  = await scrape_tln_divorces()
    ucc_recs      = await scrape_ucc_mechanic_liens()
    federal_recs  = await scrape_federal_liens()
    muni_recs     = await scrape_municipal_court()
    tax_recs      = await scrape_tax_delinquent()

    all_records = (
        cp_records + sheriff_recs + fc_recs + probate_recs +
        divorce_recs + ucc_recs + federal_recs + muni_recs + tax_recs
    )
    logging.info("Total before enrich: %s", len(all_records))

    # 3. Enrich all records
    enriched = []
    for r in all_records:
        try:
            enriched.append(enrich(r, parcels))
        except Exception as e:
            logging.warning("Enrich failed %s: %s", r.doc_num, e)
            enriched.append(r)
    all_records = enriched

    # 4. Cross-stack + dedupe + sort
    all_records = cross_stack(all_records)
    all_records = dedupe(all_records)
    all_records.sort(
        key=lambda r: (
            r.doc_type == "SHERIFF",
            r.hot_stack,
            r.distress_count,
            r.subject_to_score,
            r.score,
            r.filed,
        ),
        reverse=True
    )
    logging.info("Total after dedupe: %s", len(all_records))

    # 5. Write all outputs
    payload = build_payload(all_records)
    for path in DEFAULT_OUTPUT_JSON_PATHS:
        write_json(path, payload)

    write_category_json(all_records)
    write_csv(all_records, DEFAULT_OUTPUT_CSV_PATH)
    if Path(args.out_csv) != DEFAULT_OUTPUT_CSV_PATH:
        write_csv(all_records, Path(args.out_csv))

    # Final summary
    logging.info(
        "=== DONE === Total:%s | Sheriff:%s | HotStack:%s | Probate:%s | "
        "Foreclosure:%s | Liens:%s | TaxDelin:%s | Absentee:%s | OOS:%s | "
        "SubjectTo:%s | Divorce:%s | BK:%s | CodeViol:%s",
        len(all_records),
        sum(1 for r in all_records if r.doc_type == "SHERIFF"),
        sum(1 for r in all_records if r.hot_stack),
        sum(1 for r in all_records if r.doc_type == "PRO"),
        sum(1 for r in all_records if r.doc_type in {"NOFC","LP"}),
        sum(1 for r in all_records if r.doc_type in {"LN","LNMECH","LNFED"}),
        sum(1 for r in all_records if r.doc_type == "TAX"),
        sum(1 for r in all_records if r.is_absentee),
        sum(1 for r in all_records if r.is_out_of_state),
        sum(1 for r in all_records if r.subject_to_score >= 50),
        sum(1 for r in all_records if r.doc_type == "DIVORCE"),
        sum(1 for r in all_records if r.doc_type == "BK"),
        sum(1 for r in all_records if r.doc_type == "CODEVIOLATION"),
    )

if __name__ == "__main__":
    asyncio.run(main())
