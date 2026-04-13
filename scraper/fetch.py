"""
Toledo / Lucas County — Motivated Seller Intelligence Platform v2
=================================================================
FREE PUBLIC SOURCES:
  1. Toledo Legal News Common Pleas daily filings  — foreclosures, liens, judgments
  2. Toledo Legal News Foreclosure Notices          — individual case articles
  3. Lucas County Sheriff Sale Auction site         — active sheriff sales
  4. Lucas County Foreclosure Search app            — tax + mortgage foreclosures
  5. TLN Probate Court                              — estate filings
  6. TLN Domestic Relations                         — divorces
  7. Ohio SOS UCC                                   — mechanic / commercial liens
  8. PACER RSS                                      — federal liens, bankruptcies
  9. Toledo Municipal Court                         — code violations / evictions
 10. Lucas County Treasurer                         — tax delinquent

PARCEL ENRICHMENT (local DBF files):
  - Parcels.dbf         (geometry / lot info — not used directly)
  - ParcelsAddress.dbf  (owner, property address, mailing address, LUC, PARID)

  Place ParcelsAddress.dbf at:
    <repo_root>/data/parcels/ParcelsAddress.dbf
  OR set env var:
    DBF_PARCELS_ADDRESS=/full/path/to/ParcelsAddress.dbf
  OR pass CLI flag:
    python fetch.py --dbf-address /full/path/to/ParcelsAddress.dbf

  Key fields used from ParcelsAddress.dbf:
    OWNER       — owner name (e.g. "SMITH JOHN A")
    PROPERTY_A  — property address (e.g. "1456 SUMMIT ST, TOLEDO OH 43604")
    MAILING_AD  — mailing address  (e.g. "650 W PEACHTREE SQ NW, ATLANTA GA 30308")
    PARID       — parcel ID
    LUC         — land use code
    ZONING      — zoning code

VALUATION (with API key):
  - Zillow via RapidAPI (ZILLOW_API_KEY secret)
  - Fallback: county assessed value / 0.35 (35% assessment ratio)
"""
import argparse, asyncio, csv, json, logging, os, re, time, random
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
DATA_DIR      = BASE_DIR / "data"
DASHBOARD_DIR = BASE_DIR / "dashboard"
DEBUG_DIR     = DATA_DIR / "debug"

DEFAULT_OUTPUT_JSON_PATHS = [DATA_DIR / "records.json", DASHBOARD_DIR / "records.json"]
DEFAULT_OUTPUT_CSV_PATH   = DATA_DIR / "ghl_export.csv"
DEFAULT_ENRICHED_CSV_PATH = DATA_DIR / "records.enriched.csv"

# ── DBF parcel file paths ──────────────────────────────────────────────────
# ParcelsAddress.dbf is the one that has owner/address data.
# Parcels.dbf only has geometry — not used for enrichment.
DBF_PARCELS_ADDRESS = Path(
    os.getenv(
        "DBF_PARCELS_ADDRESS",
        str(BASE_DIR / "data" / "parcels" / "ParcelsAddress.dbf")
    )
)

LOOKBACK_DAYS = 90
SOURCE_NAME   = "Toledo / Lucas County, Ohio"
OH_APPR_RATE  = 0.04

# ── Zillow API config ──────────────────────────────────────────────────────
ZILLOW_API_KEY    = os.getenv("ZILLOW_API_KEY", "")
ZILLOW_API_HOST   = "zillow-com1.p.rapidapi.com"
ZILLOW_CACHE: Dict[str, Optional[float]] = {}
ZILLOW_CALLS      = 0
ZILLOW_MAX_CALLS  = 400

# Tokens in an address that indicate it's junk / not a real property address
ZILLOW_SKIP_TOKENS = {
    "increments", "bids", "am et", "property addr", "pending",
    "unknown", "tbd", "n/a", "none",
}

# ── Source URLs ────────────────────────────────────────────────────────────
TLN_BASE             = "https://www.toledolegalnews.com"
TLN_COMMON_PLEAS_URL = "https://www.toledolegalnews.com/courts/common_pleas/"
TLN_FORECLOSURES_URL = "https://www.toledolegalnews.com/legal_notices/foreclosures/"
TLN_PROBATE_URL      = "https://www.toledolegalnews.com/courts/probate/"
TLN_DOMESTIC_URL     = "https://www.toledolegalnews.com/courts/domestic_court/"
SHERIFF_AUCTION_URL  = "https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=USER&zmethod=CALENDAR"
LC_FORECLOSURE_URL   = "http://lcapps.co.lucas.oh.us/foreclosure/search.aspx"

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

NOISE_WORDS = [
    "annual reports","dissolutions","zoning","name changes","bid notices",
    "public hearings","whatsapp","facebook","twitter","sign up","log in",
    "contact us","terms of use","privacy","e-edition","classifieds",
    "marriage license","death notice","building permit","vendor license",
]

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
    zillow_value: Optional[float] = None
    value_source: str = ""
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
            logging.warning("GET %s/%s %s: %s", i, attempts, url[:80], e)
            if i < attempts:
                time.sleep(delay * i + random.uniform(0, 1))
    raise last

def retry_post(url: str, data: dict, attempts: int = 3, timeout: int = 30):
    last = None
    for i in range(1, attempts + 1):
        try:
            r = requests.post(url, headers=HEADERS, data=data,
                              timeout=timeout, allow_redirects=True)
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            if i < attempts: time.sleep(2 * i)
    raise last

async def pw_fetch(url: str, wait_ms: int = 2500) -> str:
    """Playwright fetch with anti-bot bypass."""
    # Skip non-HTTP URLs (mailto, share links, etc.)
    if not url.startswith("http"):
        return ""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-blink-features=AutomationControlled",
                      "--disable-dev-shm-usage","--disable-gpu"]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="America/New_York",
            )
            await ctx.add_init_script("""
                Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
                Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});
                window.chrome={runtime:{}};
            """)
            page = await ctx.new_page()
            try:
                domain = re.match(r"https?://[^/]+", url)
                if domain:
                    try:
                        await page.goto(domain.group(0), wait_until="domcontentloaded", timeout=15000)
                        await page.wait_for_timeout(800 + random.randint(0, 400))
                    except: pass
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(wait_ms + random.randint(0, 600))
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
        ("NORTH","N"),("SOUTH","S"),("EAST","E"),("WEST","W"),
        ("N.","N"),("S.","S"),("E.","E"),("W.","W"),
        ("STREET","ST"),("AVENUE","AVE"),("ROAD","RD"),
        ("DRIVE","DR"),("BOULEVARD","BLVD"),("LANE","LN"),
        ("COURT","CT"),("PLACE","PL"),("TERRACE","TER"),
        ("CIRCLE","CIR"),("PARKWAY","PKWY"),
    ]:
        addr = re.sub(r'\b' + old + r'\b', new, addr)
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9\s]", "", addr)).strip()

def norm_name(n: str) -> str:
    n = clean(n).upper()
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9,&.\- /']", " ", n)).strip()

def name_variants(name: str) -> List[str]:
    """
    Generate lookup variants for a name.
    Lucas County DBF stores names as: LASTNAME FIRSTNAME MI  (no comma)
    Government filings use:           FIRSTNAME LASTNAME  or  LASTNAME, FIRSTNAME
    We generate all permutations so both directions match.
    """
    n = clean(name).upper()
    if not n: return []
    # Strip common suffixes
    n = re.sub(r'\b(JR|SR|II|III|IV|ESQ|DEC|DECEASED|ET\s+AL|ETAL)\.?\b', '', n).strip()
    variants: set = {n}
    parts = [p for p in re.split(r"[\s,]+", n) if p and len(p) > 1]
    if len(parts) >= 2:
        variants.add(" ".join(parts))                   # AS-IS joined
        variants.add(f"{parts[-1]} {parts[0]}")         # LAST FIRST
        variants.add(f"{parts[0]} {parts[-1]}")         # FIRST LAST
        variants.add(f"{parts[-1]}, {parts[0]}")        # LAST, FIRST
        variants.add(f"{parts[0]}, {parts[-1]}")        # FIRST, LAST
        variants.add(" ".join(sorted(parts)))            # token-sorted
        variants.add(parts[0])                          # first token only
        variants.add(parts[-1])                         # last token only
    return [v.strip() for v in variants if v.strip()]

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
            for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%B %d, %Y","%B %d %Y"):
                try: return datetime.strptime(raw, fmt).date().isoformat()
                except: continue
    return None

def is_recent(filed: str, days: int = LOOKBACK_DAYS) -> bool:
    try:
        return datetime.fromisoformat(filed).date() >= (datetime.now().date() - timedelta(days=days))
    except:
        return True

def is_noise(text: str) -> bool:
    t = text.lower()
    return any(w in t for w in NOISE_WORDS)

def is_valid_address(addr: str) -> bool:
    """Return False for junk / placeholder addresses."""
    if not addr: return False
    a = addr.lower()
    if any(t in a for t in ZILLOW_SKIP_TOKENS): return False
    # Must start with a real house number (1-5 digits then a letter)
    if not re.match(r"^\d{1,5}\s+[a-zA-Z]", addr.strip()): return False
    return True

def infer_doc_type(text: str) -> Optional[str]:
    t = clean(text).upper()
    if any(x in t for x in ["LIS PENDENS"," LP ","LP-"]): return "LP"
    if any(x in t for x in ["NOTICE OF FORECLOSURE","FORECLOS","NOFC","COMPLAINT TO FORECLOSE","MTG ON","MORTGAGE FORECLOSURE"]): return "NOFC"
    if any(x in t for x in ["SHERIFF","AUCTION"]): return "SHERIFF"
    if any(x in t for x in ["DIVORCE","DISSOLUTION OF MARRIAGE"]): return "DIVORCE"
    if any(x in t for x in ["EVICTION","FORCIBLE ENTRY"]): return "EVICTION"
    if "CERTIFIED JUDGMENT" in t: return "CCJ"
    if "JUDGMENT" in t: return "JUD"
    if any(x in t for x in ["TAX DEED","TAXDEED"]): return "TAXDEED"
    if any(x in t for x in ["IRS","INTERNAL REVENUE"]): return "LNIRS"
    if any(x in t for x in ["FEDERAL TAX","US TAX","UNITED STATES TAX","LNFED","DEPT OF TAXATION","INCOME TAX","STATE TAX"]): return "LNFED"
    if "MECHANIC" in t: return "LNMECH"
    if "HOA" in t or "HOMEOWNER" in t: return "LNHOA"
    if "MEDICAID" in t: return "MEDLN"
    if "CHILD SUPPORT" in t: return "LN"
    if "LIEN" in t: return "LN"
    if any(x in t for x in ["PROBATE","ESTATE OF","IN RE ESTATE"]): return "PRO"
    if "NOTICE OF COMMENCEMENT" in t: return "NOC"
    if "BANKRUPTCY" in t or " BK " in t: return "BK"
    return None

def classify_distress(doc_type: str) -> Optional[str]:
    return {
        "LP":"lis_pendens","RELLP":"lis_pendens",
        "NOFC":"foreclosure","TAXDEED":"tax_delinquent",
        "JUD":"judgment","CCJ":"judgment","DRJUD":"judgment",
        "LN":"lien","LNHOA":"lien","LNFED":"lien",
        "LNIRS":"lien","LNCORPTX":"lien","MEDLN":"lien",
        "LNMECH":"mechanic_lien","NOC":"mechanic_lien",
        "TAX":"tax_delinquent","PRO":"probate",
        "SHERIFF":"sheriff_sale","CODEVIOLATION":"code_violation",
        "DIVORCE":"divorce","EVICTION":"eviction","BK":"bankruptcy",
    }.get(clean(doc_type).upper())

def cat_flags(doc_type: str, owner: str = "") -> List[str]:
    flags = []
    dt = clean(doc_type).upper()
    if dt == "LP": flags.append("Lis pendens")
    if dt == "NOFC": flags.append("Pre-foreclosure")
    if dt in {"JUD","CCJ","DRJUD"}: flags.append("Judgment lien")
    if dt in {"TAXDEED","LNCORPTX","LNIRS","LNFED","TAX"}: flags.append("Tax lien")
    if dt in {"LNMECH","NOC"}: flags.append("Mechanic lien")
    if dt == "PRO": flags.append("Probate / estate")
    if dt == "SHERIFF": flags.append("Sheriff sale scheduled")
    if dt == "CODEVIOLATION": flags.append("Code violation")
    if dt == "DIVORCE": flags.append("Divorce filing")
    if dt == "EVICTION": flags.append("Eviction filed")
    if dt == "BK": flags.append("Bankruptcy")
    if likely_corp(norm_name(owner)): flags.append("LLC / corp owner")
    return list(dict.fromkeys(flags))

def is_absentee(prop_addr: str, mail_addr: str, mail_state: str = "") -> bool:
    if not prop_addr or not mail_addr: return False
    if re.search(r"\bP\.?\s*O\.?\s*BOX\b", mail_addr.upper()): return True
    s = norm_state(mail_state)
    if s and s != "OH": return True
    pk = norm_addr_key(prop_addr); mk = norm_addr_key(mail_addr)
    if not pk or not mk or pk == mk: return False
    def core(a):
        parts = a.split()
        return " ".join(parts[:2]) if len(parts) >= 2 else a
    return core(pk) != core(mk)

def is_oos(mail_state: str) -> bool:
    s = norm_state(mail_state)
    return bool(s and s != "OH")

# ── Zillow API ─────────────────────────────────────────────────────────────
def get_zillow_value(address: str, city: str = "Toledo", state: str = "OH", zip_code: str = "") -> Optional[float]:
    global ZILLOW_CALLS
    if not ZILLOW_API_KEY: return None
    if not is_valid_address(address): return None
    if ZILLOW_CALLS >= ZILLOW_MAX_CALLS:
        logging.warning("Zillow call limit reached (%s)", ZILLOW_MAX_CALLS)
        return None

    full_addr = f"{address}, {city}, {state}"
    if zip_code: full_addr += f" {zip_code}"
    cache_key = norm_addr_key(full_addr)
    if cache_key in ZILLOW_CACHE: return ZILLOW_CACHE[cache_key]

    try:
        url = f"https://{ZILLOW_API_HOST}/propertyExtendedSearch"
        headers = {"X-RapidAPI-Key": ZILLOW_API_KEY, "X-RapidAPI-Host": ZILLOW_API_HOST}
        r = requests.get(url, headers=headers, params={"location": full_addr}, timeout=10)
        r.raise_for_status()
        ZILLOW_CALLS += 1
        data = r.json()
        zestimate = None
        props = data.get("props", [])
        if props:
            first = props[0]
            zestimate = (
                first.get("zestimate") or first.get("price") or
                first.get("listPrice") or
                first.get("hdpData", {}).get("homeInfo", {}).get("zestimate")
            )
        if not zestimate and props:
            zpid = props[0].get("zpid")
            if zpid:
                dr = requests.get(f"https://{ZILLOW_API_HOST}/property",
                                  headers=headers, params={"zpid": zpid}, timeout=10)
                ZILLOW_CALLS += 1
                if dr.status_code == 200:
                    dd = dr.json()
                    zestimate = (dd.get("zestimate") or dd.get("price") or
                                 dd.get("homeDetails", {}).get("zestimate"))
        if zestimate:
            val = float(str(zestimate).replace(",", "").replace("$", ""))
            if val > 1000:
                ZILLOW_CACHE[cache_key] = val
                logging.info("Zillow: %s -> $%,.0f (call #%s)", address[:40], val, ZILLOW_CALLS)
                return val
    except Exception as e:
        logging.warning("Zillow lookup failed %s: %s", address[:40], e)

    ZILLOW_CACHE[cache_key] = None
    return None

def get_best_value_estimate(record: "LeadRecord") -> tuple:
    if ZILLOW_API_KEY and is_valid_address(record.prop_address):
        zval = get_zillow_value(
            record.prop_address,
            record.prop_city or "Toledo",
            record.prop_state or "OH",
            record.prop_zip or ""
        )
        if zval and zval > 5000:
            return zval, "Zillow Zestimate"
    if record.assessed_value and record.assessed_value > 1000:
        return round(record.assessed_value / 0.35, 2), "Assessed Value (est)"
    if record.last_sale_price and record.last_sale_price > 5000:
        yrs = max(0, datetime.now().year - (record.last_sale_year or datetime.now().year))
        return round(record.last_sale_price * ((1 + OH_APPR_RATE) ** yrs), 2), "Last Sale (appreciated)"
    return None, ""

# ── Mortgage / equity estimation ───────────────────────────────────────────
def estimate_financials(record: "LeadRecord") -> "LeadRecord":
    signals = []
    sto = 0
    if not record.estimated_value:
        val, source = get_best_value_estimate(record)
        if val:
            record.estimated_value = val
            record.value_source = source
            if not record.zillow_value and source == "Zillow Zestimate":
                record.zillow_value = val
    mv = record.estimated_value
    if record.last_sale_price and record.last_sale_year and record.last_sale_price > 5000:
        yrs_elapsed = max(0, min(30, datetime.now().year - record.last_sale_year))
        orig = record.last_sale_price * 0.80
        mr = 0.065 / 12; n = 360; paid = yrs_elapsed * 12
        if mr > 0 and paid < n:
            bal = orig * ((1 + mr) ** n - (1 + mr) ** paid) / ((1 + mr) ** n - 1)
            record.est_mortgage_balance = round(max(0, bal), 2)
        elif paid >= n:
            record.est_mortgage_balance = 0.0
    if mv and record.est_mortgage_balance is not None:
        record.est_equity = round(mv - record.est_mortgage_balance, 2)
    elif mv and record.est_mortgage_balance is None and not record.last_sale_price:
        record.est_mortgage_balance = round(mv * 0.50, 2)
        record.est_equity = round(mv * 0.50, 2)
        record.est_payoff = record.est_mortgage_balance
    if record.doc_type in {"LP","NOFC","TAXDEED","SHERIFF"} and record.amount and record.amount > 0:
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
    if record.doc_type in {"LP","NOFC","SHERIFF"}: sto += 25; signals.append("Active foreclosure")
    if record.doc_type == "PRO": sto += 20; signals.append("Estate / probate")
    if record.is_absentee: sto += 15; signals.append("Absentee owner")
    if record.is_out_of_state: sto += 10; signals.append("Out-of-state owner")
    if record.is_inherited: sto += 20; signals.append("Inherited property")
    if "Tax lien" in record.flags: sto += 15
    if record.est_mortgage_balance and record.estimated_value and record.estimated_value > 0:
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
    if record.zillow_value or record.estimated_value: score += 5
    if record.filed:
        try:
            if datetime.fromisoformat(record.filed).date() >= (datetime.now().date() - timedelta(days=7)):
                if "New this week" not in record.flags:
                    record.flags.append("New this week")
                score += 5
        except: pass
    if is_valid_address(record.prop_address): score += 5
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

# ── DBF Parcel data loader ─────────────────────────────────────────────────
def _parse_dbf_property_address(raw: str) -> tuple:
    """
    Parse ParcelsAddress PROPERTY_A field.
    Format: '1456 SUMMIT ST, TOLEDO OH 43604'
    Returns: (street, city, state, zip)
    """
    if not raw: return "", "Toledo", "OH", ""
    raw = raw.strip()
    m = re.match(r"^(\d+\s+.+?),\s*([A-Za-z\s]+?)\s+([A-Z]{2})\s+(\d{5})?$", raw)
    if m:
        return (clean(m.group(1)).title(), clean(m.group(2)).title(),
                m.group(3).upper(), m.group(4) or "")
    m2 = re.match(r"^(\d+\s+.+?),\s*([A-Za-z\s]+?)\s+([A-Z]{2})$", raw)
    if m2:
        return clean(m2.group(1)).title(), clean(m2.group(2)).title(), m2.group(3).upper(), ""
    return raw.title(), "Toledo", "OH", ""

def _parse_dbf_mailing_address(raw: str) -> tuple:
    """
    Parse ParcelsAddress MAILING_AD field.
    Format: '650 W PEACHTREE SQ NW, ATLANTA GA 30308'
    Returns: (street, city, state, zip)
    """
    if not raw: return "", "", "", ""
    raw = raw.strip()
    m = re.match(r"^(.+?),\s*([A-Za-z\s\.]+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)?$", raw)
    if m:
        return (clean(m.group(1)).title(), clean(m.group(2)).title(),
                m.group(3).upper(), (m.group(4) or "")[:5])
    m2 = re.match(r"^(.+?),\s*([A-Za-z\s]+?)\s+([A-Z]{2})$", raw)
    if m2:
        return clean(m2.group(1)).title(), clean(m2.group(2)).title(), m2.group(3).upper(), ""
    return raw.title(), "", "", ""

def load_parcel_data() -> Dict[str, dict]:
    """
    Load ParcelsAddress.dbf from local filesystem.

    Indexes records two ways:
      norm_addr_key(prop_street)  -> parcel dict   (address lookup)
      'OWNER:<name_variant>'      -> parcel dict   (name lookup for probate)

    Returns empty dict if file not found or dbfread not installed.
    """
    parcels: Dict[str, dict] = {}

    if not DBF_PARCELS_ADDRESS.exists():
        logging.warning(
            "ParcelsAddress.dbf not found at: %s\n"
            "  → Copy it to data/parcels/ParcelsAddress.dbf\n"
            "  → OR set env var: DBF_PARCELS_ADDRESS=/path/to/ParcelsAddress.dbf\n"
            "  → OR pass: --dbf-address /path/to/ParcelsAddress.dbf\n"
            "  Parcel enrichment (absentee/OOS/address fill) will be skipped.",
            DBF_PARCELS_ADDRESS
        )
        return parcels

    try:
        from dbfread import DBF as DbfReader
    except ImportError:
        logging.warning("dbfread not installed. Run: pip install dbfread")
        return parcels

    logging.info("Loading ParcelsAddress.dbf from %s ...", DBF_PARCELS_ADDRESS)
    count = 0

    try:
        table = DbfReader(str(DBF_PARCELS_ADDRESS), load=False, encoding="latin-1")
        for row in table:
            try:
                R = {k.upper(): clean(v) for k, v in dict(row).items()}

                owner_raw = R.get("OWNER", "")
                prop_raw  = R.get("PROPERTY_A", "")
                mail_raw  = R.get("MAILING_AD", "")
                parid     = R.get("PARID", "")
                luc       = R.get("LUC", "")
                zoning    = R.get("ZONING", "")

                if not owner_raw and not prop_raw:
                    continue

                prop_street, prop_city, prop_state, prop_zip = _parse_dbf_property_address(prop_raw)
                mail_street, mail_city, mail_state, mail_zip = _parse_dbf_mailing_address(mail_raw)

                if not prop_street:
                    continue

                rec = {
                    "parcel_id":    parid,
                    "owner":        owner_raw.title(),
                    "prop_address": prop_street,
                    "prop_city":    prop_city or "Toledo",
                    "prop_state":   prop_state or "OH",
                    "prop_zip":     prop_zip,
                    "mail_address": mail_street,
                    "mail_city":    mail_city,
                    "mail_state":   mail_state or "OH",
                    "mail_zip":     mail_zip,
                    "luc":          luc,
                    "zoning":       zoning,
                    "assessed_value":   None,
                    "est_market_value": None,
                }

                # Index 1: property address key
                addr_key = norm_addr_key(prop_street)
                if addr_key:
                    parcels[addr_key] = rec

                # Index 2: all owner name variants (critical for probate name matching)
                if owner_raw:
                    for v in name_variants(owner_raw):
                        k = f"OWNER:{v}"
                        if k not in parcels:  # don't overwrite better address matches
                            parcels[k] = rec

                count += 1
            except Exception as row_err:
                logging.debug("DBF row skip: %s", row_err)

    except Exception as e:
        logging.error("Failed to read ParcelsAddress.dbf: %s", e)
        return parcels

    addr_count  = sum(1 for k in parcels if not k.startswith("OWNER:"))
    owner_count = sum(1 for k in parcels if k.startswith("OWNER:"))
    logging.info(
        "Parcel DBF: %s rows loaded | %s address keys | %s owner name keys",
        count, addr_count, owner_count
    )
    return parcels

def match_parcel(owner: str, prop_address: str, parcels: Dict[str, dict]) -> tuple:
    """
    Match a lead to a parcel record.
    Returns (parcel_dict, method_string) or (None, 'unmatched').

    Priority:
      1. Exact normalized property address  (most reliable)
      2. Exact owner name variant match
      3. Token-sorted name match            (handles word-order differences)
      4. Last-name/single-token fallback    (individuals only, not corps)
    """
    if not parcels:
        return None, "unmatched"

    # 1. Address match
    if prop_address and is_valid_address(prop_address):
        key = norm_addr_key(prop_address)
        if key and key in parcels:
            return parcels[key], "address_exact"

    # 2 & 3 & 4. Name match
    if owner:
        owner_up = norm_name(owner)
        variants = name_variants(owner_up)

        # Exact variant
        for v in variants:
            k = f"OWNER:{v}"
            if k in parcels:
                return parcels[k], "name_exact"

        # Token-sorted (catches "JOHN SMITH" vs DBF "SMITH JOHN")
        tokens_sorted = " ".join(sorted(owner_up.split()))
        if f"OWNER:{tokens_sorted}" in parcels:
            return parcels[f"OWNER:{tokens_sorted}"], "name_token_sorted"

        # Last-name-only fallback for individuals
        if not likely_corp(owner):
            parts = [p for p in re.split(r"[\s,]+", owner_up) if len(p) > 2]
            for part in parts:
                k = f"OWNER:{part}"
                if k in parcels:
                    return parcels[k], "name_lastname_only"

    return None, "unmatched"

def enrich(record: "LeadRecord", parcels: Dict[str, dict]) -> "LeadRecord":
    """
    Enrich a lead with parcel data.
    For PRO/inherited records, also tries decedent_name as the lookup key
    if the owner match fails — this is the key improvement for probate leads.
    """
    matched, method = match_parcel(record.owner, record.prop_address, parcels)

    # Probate fallback: try decedent name if owner didn't match
    if matched is None and record.doc_type == "PRO" and record.decedent_name:
        matched, method = match_parcel(record.decedent_name, "", parcels)
        if matched:
            method = f"probate_{method}"

    if matched:
        # Fill property address only if missing or junk
        if not is_valid_address(record.prop_address):
            record.prop_address = matched.get("prop_address", "")
        if not record.prop_city:
            record.prop_city = matched.get("prop_city", "") or "Toledo"
        if not record.prop_zip:
            record.prop_zip = matched.get("prop_zip", "")

        # Always fill mailing from parcel (ground truth — county records)
        if not record.mail_address:
            record.mail_address = matched.get("mail_address", "")
        if not record.mail_city:
            record.mail_city = matched.get("mail_city", "")
        if not record.mail_state:
            record.mail_state = matched.get("mail_state", "OH")
        if not record.mail_zip:
            record.mail_zip = matched.get("mail_zip", "")

        # Parcel metadata
        if not record.parcel_id:
            record.parcel_id = matched.get("parcel_id", "")
        if not record.luc:
            record.luc = matched.get("luc", "")
        if not record.assessed_value:
            record.assessed_value = matched.get("assessed_value")
        if not record.estimated_value:
            record.estimated_value = matched.get("est_market_value")

        record.match_method = method
        record.match_score = {
            "address_exact":      1.00,
            "name_exact":         0.92,
            "name_token_sorted":  0.85,
            "name_lastname_only": 0.65,
        }.get(method.replace("probate_", ""), 0.75)

    # Defaults
    if not record.prop_city:  record.prop_city  = "Toledo"
    if not record.prop_state: record.prop_state = "OH"

    # Clear any junk prop_address that slipped through
    if record.prop_address and not is_valid_address(record.prop_address):
        record.prop_address = ""

    record.with_address   = 1 if is_valid_address(record.prop_address) else 0
    record.is_absentee    = is_absentee(record.prop_address, record.mail_address, record.mail_state)
    record.is_out_of_state = is_oos(record.mail_state)

    if record.is_absentee and "Absentee owner" not in record.flags:
        record.flags.append("Absentee owner")
    if record.is_out_of_state and "Out-of-state owner" not in record.flags:
        record.flags.append("Out-of-state owner")

    record.flags = list(dict.fromkeys(record.flags + cat_flags(record.doc_type, record.owner)))
    record = estimate_financials(record)
    record.score = score_record(record)
    return record

# ── SCRAPER 1: TLN Common Pleas daily filings ──────────────────────────────
async def scrape_tln_common_pleas() -> List[LeadRecord]:
    records: List[LeadRecord] = []
    seen: set = set()
    logging.info("Scraping TLN Common Pleas...")

    html = await pw_fetch(TLN_COMMON_PLEAS_URL, wait_ms=3000)
    if not html: return records
    soup = BeautifulSoup(html, "lxml")
    save_debug("tln_cp_index.html", html[:5000])

    article_links = []
    for a in soup.select("a[href]"):
        href = clean(a.get("href",""))
        if not href or is_noise(href): continue
        if href.startswith("mailto:") or "wa.me" in href or "facebook.com" in href: continue
        if "article_" in href or "filings-received" in href:
            full = href if href.startswith("http") else urljoin(TLN_BASE, href)
            if "toledolegalnews.com" in full and full not in article_links:
                article_links.append(full)

    for days_back in range(0, 15):
        d = (datetime.now() - timedelta(days=days_back)).strftime("%B-%-d-%Y").lower()
        url = f"{TLN_BASE}/courts/common_pleas/common-pleas-filings-received-on-{d}/"
        if url not in article_links:
            article_links.append(url)

    logging.info("TLN CP: %s URLs", len(article_links))

    for url in article_links[:20]:
        try:
            art_html = await pw_fetch(url, wait_ms=2000)
            if not art_html or len(art_html) < 500: continue
            art_soup = BeautifulSoup(art_html, "lxml")
            text = art_soup.get_text(" ")
            if "404" in text[:300] or "not found" in text[:300].lower(): continue

            fc_pat = re.compile(
                r"(CI[0-9]{4}[0-9]+)\s+(.{5,80}?)\s+vs\.?\s+(.{5,80}?)\.\s+"
                r".*?(?:foreclosure of mtg on|property located at|premises known as|real estate located at)\s+"
                r"([0-9]{1,5}\s+[A-Za-z][A-Za-z0-9\s\.]{3,35}),"
                r"\s*([A-Za-z\s]+),\s*Ohio\s*([0-9]{5})?",
                re.IGNORECASE | re.DOTALL
            )
            for m in fc_pat.finditer(text):
                doc_num = clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                plaintiff  = clean(m.group(2))
                defendant  = clean(m.group(3)).title()
                prop_address = clean(m.group(4)).title()
                prop_city  = clean(m.group(5)).title()
                prop_zip   = clean(m.group(6)) if m.group(6) else ""
                amt_m = re.search(r"\$\s*([\d,]+(?:\.\d{2})?)", text[m.start():m.start()+300])
                amt   = parse_amount(amt_m.group(1)) if amt_m else None
                filed = try_parse_date(text[max(0,m.start()-200):m.start()+50]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                records.append(LeadRecord(
                    doc_num=doc_num, doc_type="NOFC", filed=filed,
                    cat="NOFC", cat_label="Pre-foreclosure",
                    owner=defendant, grantee=plaintiff, amount=amt,
                    prop_address=prop_address, prop_city=prop_city,
                    prop_state="OH", prop_zip=prop_zip, clerk_url=url,
                    flags=["Pre-foreclosure","Lis pendens"],
                    distress_sources=["foreclosure","lis_pendens"],
                ))

            ln_pat = re.compile(
                r"(LN[0-9]{4}[0-9\-]+)[;,\s]+([^;,\n]{3,80}?)\s+vs\.?\s+([^;,\n\.]{3,60}?)(?:[;,\.]|\s{2}|$)",
                re.IGNORECASE
            )
            for m in ln_pat.finditer(text):
                doc_num = clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                plaintiff = clean(m.group(2))
                owner     = clean(m.group(3)).title()
                if not owner or len(owner) < 3: continue
                filed = try_parse_date(text[max(0,m.start()-100):m.start()+50]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                dt    = infer_doc_type(plaintiff) or "LNFED"
                amt_m = re.search(r"\$\s*([\d,]+(?:\.\d{2})?)", text[m.start():m.start()+200])
                amt   = parse_amount(amt_m.group(1)) if amt_m else None
                records.append(LeadRecord(
                    doc_num=doc_num, doc_type=dt, filed=filed,
                    cat=dt, cat_label=LEAD_TYPE_MAP.get(dt, dt),
                    owner=owner, grantee=plaintiff, amount=amt, clerk_url=url,
                    flags=cat_flags(dt, owner),
                    distress_sources=[s for s in [classify_distress(dt)] if s],
                ))

            sc_pat = re.compile(
                r"(LN[0-9]{4}[0-9\-]+)[;, ]+\$?([\d,\.]+)[;, ]+([^;\n]{3,60})[;, ]+([^;\n]{3,60})[;, ]+([^;\n]{3,80})",
                re.IGNORECASE
            )
            for m in sc_pat.finditer(text):
                doc_num = clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                try: amt = float(m.group(2).replace(",",""))
                except: amt = None
                if amt and amt > 10_000_000: continue
                plaintiff = clean(m.group(3))
                owner     = clean(m.group(4)).title()
                addr_raw  = clean(m.group(5))
                if not owner or len(owner) < 3: continue
                dt    = infer_doc_type(plaintiff) or "LN"
                filed = try_parse_date(text[max(0,m.start()-200):m.start()+100]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                addr_m = re.search(r"(\d{2,5}\s+[A-Z][A-Za-z\s\.]{3,30}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL|WAY|TER|CIR)\.?)", addr_raw, re.IGNORECASE)
                prop_address = clean(addr_m.group(1)).title() if addr_m else ""
                city_m = re.search(r"(TOLEDO|MAUMEE|SYLVANIA|OREGON|PERRYSBURG|WATERVILLE|WHITEHOUSE|HOLLAND|SWANTON)", addr_raw, re.IGNORECASE)
                prop_city = clean(city_m.group(0)).title() if city_m else "Toledo"
                zip_m = re.search(r"(43\d{3})", addr_raw)
                records.append(LeadRecord(
                    doc_num=doc_num, doc_type=dt, filed=filed,
                    cat=dt, cat_label=LEAD_TYPE_MAP.get(dt, dt),
                    owner=owner, grantee=plaintiff, amount=amt,
                    prop_address=prop_address, prop_city=prop_city,
                    prop_state="OH", prop_zip=zip_m.group(1) if zip_m else "",
                    clerk_url=url,
                    flags=cat_flags(dt, owner),
                    distress_sources=[s for s in [classify_distress(dt)] if s],
                ))

            await asyncio.sleep(1.5 + random.uniform(0, 1))
        except Exception as e:
            logging.warning("TLN CP article %s: %s", url[-50:], e)

    logging.info("TLN Common Pleas: %s records", len(records))
    return records

# ── SCRAPER 2: TLN Foreclosure Notices ────────────────────────────────────
async def scrape_tln_foreclosure_notices() -> List[LeadRecord]:
    records: List[LeadRecord] = []
    seen: set = set()
    logging.info("Scraping TLN Foreclosure Notices...")

    ADDRESS_PAT = re.compile(
        r'(\d{1,5}\s+[A-Za-z][A-Za-z0-9\s\.,#\-]{3,50}),\s+'
        r'(?:Toledo|Maumee|Perrysburg|Sylvania|Oregon|Waterville|Whitehouse|Holland|Swanton|Maumee),?\s+'
        r'OH\s+(\d{5})',
        re.IGNORECASE
    )
    OWNER_PAT  = re.compile(
        r'(?:defendant|owner|mortgagor)[s]?[:\s]+([A-Z][A-Za-z\s,\.]{3,50}?)(?:,|\.|whose|last known|and)',
        re.IGNORECASE
    )
    AMOUNT_PAT = re.compile(r'\$([\d,]+(?:\.\d{2})?)')

    html = await pw_fetch(TLN_FORECLOSURES_URL, wait_ms=3000)
    if not html: return records
    soup = BeautifulSoup(html, "lxml")
    save_debug("tln_foreclosures_index.html", html[:5000])

    case_links = []
    for a in soup.select("a[href]"):
        href = clean(a.get("href",""))
        text = clean(a.get_text())
        if not href or is_noise(text) or is_noise(href): continue
        if "/legal_notices/foreclosures/" in href and "article_" in href:
            full = href if href.startswith("http") else urljoin(TLN_BASE, href)
            if full not in case_links:
                case_links.append((text, full))

    logging.info("TLN Foreclosure: %s case links found", len(case_links))

    for link_text, url in case_links[:60]:
        try:
            case_m  = re.search(r"(CI[0-9]{4}[0-9\-]+|TF[0-9]+)", url + " " + link_text, re.IGNORECASE)
            doc_num = clean(case_m.group(1)).upper() if case_m else f"FC-{len(records)+1:04d}"
            if doc_num in seen: continue
            seen.add(doc_num)
            if is_noise(link_text): continue

            art_html = await pw_fetch(url, wait_ms=2000)
            if not art_html or len(art_html) < 300: continue
            art_soup = BeautifulSoup(art_html, "lxml")
            text = art_soup.get_text(" ")

            addr_m       = ADDRESS_PAT.search(text)
            prop_address = clean(addr_m.group(1)).title() if addr_m else ""
            prop_zip     = addr_m.group(2) if addr_m else ""
            city_m       = re.search(r"(Toledo|Maumee|Perrysburg|Sylvania|Oregon|Waterville|Whitehouse|Holland|Swanton)", text, re.IGNORECASE)
            prop_city    = clean(city_m.group(0)).title() if city_m else "Toledo"

            owner_m = OWNER_PAT.search(text)
            if owner_m:
                owner = clean(owner_m.group(1)).title()
            else:
                vs_m  = re.search(r"vs\.?\s+([A-Z][A-Za-z\s,\.]{3,50}?)(?:,|\.|and\s+Jane|and\s+John|whose|$)", text, re.IGNORECASE)
                owner = clean(vs_m.group(1)).title() if vs_m else clean(link_text).title()
            owner = re.sub(r"^Case\s+No\.?\s+CI[0-9\-]+", "", owner, flags=re.IGNORECASE).strip()
            if not owner or len(owner) < 3: owner = clean(link_text).title()

            amt_m = AMOUNT_PAT.search(text)
            amt   = parse_amount(amt_m.group(1)) if amt_m else None
            filed = try_parse_date(text[:500]) or datetime.now().date().isoformat()
            is_tax = "tax" in url.lower() or "tax" in link_text.lower()
            dt     = "TAX" if is_tax else "NOFC"

            records.append(LeadRecord(
                doc_num=doc_num, doc_type=dt, filed=filed,
                cat=dt, cat_label=LEAD_TYPE_MAP.get(dt, dt),
                owner=owner, amount=amt,
                prop_address=prop_address, prop_city=prop_city,
                prop_state="OH", prop_zip=prop_zip, clerk_url=url,
                flags=["Pre-foreclosure"] + (["Tax lien"] if is_tax else []),
                distress_sources=["foreclosure"] + (["tax_delinquent"] if is_tax else []),
            ))
            await asyncio.sleep(1 + random.uniform(0, 0.5))
        except Exception as e:
            logging.warning("TLN FC article %s: %s", url[-50:], e)

    logging.info("TLN Foreclosure Notices: %s records", len(records))
    return records

# ── SCRAPER 3: Sheriff Sale Auction ───────────────────────────────────────
async def scrape_sheriff_sales() -> List[LeadRecord]:
    records: List[LeadRecord] = []
    logging.info("Scraping sheriff sales...")
    seen = set()

    auction_urls = []
    for days_ahead in range(0, 45):
        d = (datetime.now() + timedelta(days=days_ahead)).strftime("%m/%d/%Y")
        url = f"https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={quote(d)}"
        auction_urls.append(url)

    for url in auction_urls[:20]:
        try:
            html = await pw_fetch(url, wait_ms=3000)
            if not html or len(html) < 500: continue
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ")
            if "no auction" in text.lower() or "no properties" in text.lower(): continue
            save_debug("sheriff_page.html", html[:5000])

            for prop_div in soup.select(".AUCTION_ITEM, .property-item, tr, div"):
                item_text = clean(prop_div.get_text(" "))
                if len(item_text) < 20: continue

                case_m  = re.search(r"(CI[0-9]{4}[0-9\-]+|TF[0-9]+|[0-9]{4}CV[0-9]+)", item_text, re.IGNORECASE)
                doc_num = clean(case_m.group(1)) if case_m else ""
                if not doc_num or doc_num in seen: continue
                seen.add(doc_num)

                # Require proper street suffix to avoid junk addresses
                addr_m = re.search(
                    r"(\d{2,5}\s+[A-Z][A-Za-z\s\.]{3,35}"
                    r"(?:ST|AVE|RD|DR|BLVD|LN|CT|PL|WAY|TER|CIR|PKWY|HWY|PIKE)\.?)",
                    item_text, re.IGNORECASE
                )
                prop_address = clean(addr_m.group(1)).title() if addr_m else ""
                if not is_valid_address(prop_address):
                    prop_address = ""  # will be filled by parcel enrichment

                amt_m = re.search(r"(?:Appraised|Value|Bid)[:\s]*\$?([\d,]+)", item_text, re.IGNORECASE)
                if not amt_m: amt_m = re.search(r"\$([\d,]+(?:\.\d{2})?)", item_text)
                amt = parse_amount(amt_m.group(1)) if amt_m else None

                date_m    = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", item_text)
                sale_date = date_m.group(1) if date_m else ""
                try: filed = datetime.strptime(sale_date, "%m/%d/%Y").date().isoformat()
                except: filed = datetime.now().date().isoformat()

                city_m    = re.search(r"(Toledo|Maumee|Sylvania|Oregon|Perrysburg|Waterville|Whitehouse|Holland)", item_text, re.IGNORECASE)
                prop_city = clean(city_m.group(0)).title() if city_m else "Toledo"
                zip_m     = re.search(r"(43\d{3})", item_text)
                prop_zip  = zip_m.group(1) if zip_m else ""

                records.append(LeadRecord(
                    doc_num=doc_num, doc_type="SHERIFF", filed=filed,
                    cat="SHERIFF", cat_label="Sheriff Sale",
                    amount=amt, appraised_value=amt,
                    prop_address=prop_address, prop_city=prop_city,
                    prop_state="OH", prop_zip=prop_zip,
                    sheriff_sale_date=sale_date, clerk_url=url,
                    flags=["Sheriff sale scheduled","Pre-foreclosure","Hot Stack"],
                    distress_sources=["sheriff_sale","foreclosure"],
                    distress_count=2, hot_stack=True,
                    with_address=1 if is_valid_address(prop_address) else 0,
                ))

            await asyncio.sleep(1.5)
        except Exception as e:
            logging.warning("Sheriff %s: %s", url[-60:], e)

    logging.info("Sheriff sales: %s", len(records))
    return records

# ── SCRAPER 4: TLN Probate ────────────────────────────────────────────────
async def scrape_tln_probate() -> List[LeadRecord]:
    """
    Scrapes TLN probate filings. Marks all records is_inherited=True.
    Property + mailing addresses are filled in during enrich() by matching
    decedent_name against the OWNER field in ParcelsAddress.dbf.
    """
    records: List[LeadRecord] = []
    logging.info("Scraping probate...")
    try:
        html = await pw_fetch(TLN_PROBATE_URL, wait_ms=3000)
        if not html: return records
        soup     = BeautifulSoup(html, "lxml")
        all_text = soup.get_text(" ")

        links = []
        for a in soup.select("a[href]"):
            href = clean(a.get("href",""))
            if not href: continue
            if href.startswith("mailto:") or "wa.me" in href or "facebook.com" in href: continue
            if "article_" in href or "probate" in href:
                full = href if href.startswith("http") else urljoin(TLN_BASE, href)
                if "toledolegalnews.com" in full: links.append(full)

        for link in links[:10]:
            try:
                ah = await pw_fetch(link, wait_ms=2000)
                if ah: all_text += " " + BeautifulSoup(ah, "lxml").get_text(" ")
                await asyncio.sleep(1)
            except: pass

        estate_pat = re.compile(
            r"(?:Estate\s+of|In\s+re\s+Estate\s+of)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})"
            r"(?:,\s*(?:deceased|Deceased|DECEASED))?", re.IGNORECASE
        )
        seen = set()
        for m in estate_pat.finditer(all_text):
            name = clean(m.group(1))
            if name in seen or len(name) < 5: continue
            seen.add(name)
            surrounding = all_text[max(0,m.start()-50):m.end()+400]
            filed = try_parse_date(surrounding) or datetime.now().date().isoformat()
            if not is_recent(filed): continue
            exec_m   = re.search(r"(?:executor|administrator)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)", surrounding, re.IGNORECASE)
            executor = clean(exec_m.group(1)) if exec_m else ""
            records.append(LeadRecord(
                doc_num=f"PRO-{name.replace(' ','-')}-{len(records)+1}",
                doc_type="PRO", filed=filed, cat="PRO", cat_label="Probate / Estate",
                owner=name.title(), decedent_name=name.title(),
                executor_name=executor.title(), is_inherited=True,
                flags=["Probate / estate","Inherited property"],
                distress_sources=["probate"], distress_count=1,
                clerk_url=TLN_PROBATE_URL, match_method="probate_name",
            ))
    except Exception as e:
        logging.warning("Probate failed: %s", e)
    logging.info("Probate: %s", len(records))
    return records

# ── SCRAPER 5: TLN Divorces ───────────────────────────────────────────────
async def scrape_tln_divorces() -> List[LeadRecord]:
    records: List[LeadRecord] = []
    seen: set = set()
    logging.info("Scraping divorces...")
    try:
        html = await pw_fetch(TLN_DOMESTIC_URL, wait_ms=3000)
        if not html: return records
        soup      = BeautifulSoup(html, "lxml")
        all_texts = [soup.get_text(" ")]

        links = []
        for a in soup.select("a[href]"):
            href = clean(a.get("href",""))
            if not href: continue
            if href.startswith("mailto:") or "wa.me" in href or "facebook.com" in href: continue
            if "article_" in href or "domestic" in href or "filings" in href:
                full = href if href.startswith("http") else urljoin(TLN_BASE, href)
                if "toledolegalnews.com" in full: links.append(full)

        for link in links[:8]:
            try:
                ah = await pw_fetch(link, wait_ms=1500)
                if ah: all_texts.append(BeautifulSoup(ah, "lxml").get_text(" "))
                await asyncio.sleep(1)
            except: pass

        for text in all_texts:
            dr_pat = re.compile(
                r"(DR[0-9]{4}[0-9\-]+|DM[0-9]+)[;,\s]+([A-Z][A-Za-z\s,\.]{3,40}?)\s+vs\.?\s+([A-Z][A-Za-z\s,\.]{3,40}?)(?:[;,\.]|\s{2}|$)",
                re.IGNORECASE
            )
            for m in dr_pat.finditer(text):
                doc_num = clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                plaintiff = clean(m.group(2)).title()
                defendant = clean(m.group(3)).title()
                filed = try_parse_date(text[max(0,m.start()-100):m.start()+50]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                records.append(LeadRecord(
                    doc_num=doc_num, doc_type="DIVORCE", filed=filed,
                    cat="DIVORCE", cat_label="Divorce Filing",
                    owner=plaintiff, grantee=defendant,
                    clerk_url=TLN_DOMESTIC_URL,
                    flags=["Divorce filing"],
                    distress_sources=["divorce"],
                ))
    except Exception as e:
        logging.warning("Divorce scrape failed: %s", e)
    logging.info("Divorces: %s", len(records))
    return records

# ── SCRAPER 6: Tax Delinquent ─────────────────────────────────────────────
async def scrape_tax_delinquent() -> List[LeadRecord]:
    records: List[LeadRecord] = []
    logging.info("Scraping tax delinquent...")
    tax_urls = [
        "https://www.lucascountytreasurer.org/delinquent-taxes",
        "https://www.toledolegalnews.com/legal_notices/foreclosures/",
    ]
    seen   = set()
    tf_pat = re.compile(r"(TF[0-9]{4}[0-9\-]+|TF\s*[0-9]{6,})\s+(.{10,80}?)\s+\$?([\d,]+(?:\.\d{2})?)?", re.IGNORECASE)

    for url in tax_urls:
        try:
            html = await pw_fetch(url, wait_ms=3000)
            if not html or len(html) < 500: continue
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ")
            for m in tf_pat.finditer(text):
                doc_num = re.sub(r"\s+","",clean(m.group(1)))
                if doc_num in seen: continue
                seen.add(doc_num)
                try: amt = float(m.group(3).replace(",","")) if m.group(3) else None
                except: amt = None
                addr_m = re.search(r"(\d{2,5}\s+[A-Za-z][A-Za-z\s\.]{3,25}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL)\.?)", clean(m.group(2)), re.IGNORECASE)
                records.append(LeadRecord(
                    doc_num=doc_num, doc_type="TAX",
                    filed=datetime.now().date().isoformat(),
                    cat="TAX", cat_label="Tax Delinquent",
                    amount=amt,
                    prop_address=clean(addr_m.group(1)).title() if addr_m else "",
                    prop_city="Toledo", prop_state="OH", clerk_url=url,
                    flags=["Tax delinquent","Tax lien"],
                    distress_sources=["tax_delinquent"],
                ))
            if records: break
        except Exception as e:
            logging.warning("Tax delin %s: %s", url[:60], e)

    logging.info("Tax delinquent: %s", len(records))
    return records

# ── Cross-stacking & deduplication ────────────────────────────────────────
def cross_stack(records: List[LeadRecord]) -> List[LeadRecord]:
    addr_map: Dict[str, List[int]] = defaultdict(list)
    for i, r in enumerate(records):
        if is_valid_address(r.prop_address):
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
            r.distress_count   = len(r.distress_sources)
            r.hot_stack        = True
            if "Hot Stack" not in " ".join(r.flags):       r.flags.append("Hot Stack")
            if "Cross-List Match" not in " ".join(r.flags): r.flags.append("Cross-List Match")
            r = estimate_financials(r); r.score = score_record(r); records[i] = r
        stacked += 1
    logging.info("Cross-stacked %s property groups", stacked)
    return records

def dedupe(records: List[LeadRecord]) -> List[LeadRecord]:
    final, seen = [], set()
    for r in records:
        nd  = re.sub(r"^(PCF1|PCF2)-","",clean(r.doc_num).upper())
        key = (nd, clean(r.doc_type).upper(), clean(r.owner)[:20].upper(), clean(r.filed))
        if key in seen: continue
        seen.add(key); final.append(r)
    return final

# ── Output ─────────────────────────────────────────────────────────────────
def split_name(n: str):
    parts = clean(n).split()
    if not parts: return "", ""
    if len(parts) == 1: return parts[0], ""
    return parts[0], " ".join(parts[1:])

def write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

def build_payload(records: List[LeadRecord]) -> dict:
    return {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_NAME,
        "date_range": {
            "from": (datetime.now()-timedelta(days=LOOKBACK_DAYS)).date().isoformat(),
            "to":   datetime.now().date().isoformat(),
        },
        "total":               len(records),
        "with_address":        sum(1 for r in records if is_valid_address(r.prop_address)),
        "hot_stack_count":     sum(1 for r in records if r.hot_stack),
        "sheriff_sale_count":  sum(1 for r in records if r.doc_type=="SHERIFF"),
        "probate_count":       sum(1 for r in records if r.doc_type=="PRO"),
        "inherited_count":     sum(1 for r in records if r.is_inherited),
        "tax_delinquent_count":sum(1 for r in records if r.doc_type=="TAX"),
        "foreclosure_count":   sum(1 for r in records if r.doc_type in {"NOFC","LP"}),
        "lien_count":          sum(1 for r in records if r.doc_type in {"LN","LNMECH","LNFED","LNIRS","LNCORPTX"}),
        "absentee_count":      sum(1 for r in records if r.is_absentee),
        "out_of_state_count":  sum(1 for r in records if r.is_out_of_state),
        "subject_to_count":    sum(1 for r in records if r.subject_to_score>=50),
        "divorce_count":       sum(1 for r in records if r.doc_type=="DIVORCE"),
        "zillow_enriched_count":sum(1 for r in records if r.zillow_value),
        "zillow_api_calls":    ZILLOW_CALLS,
        "parcel_matched_count":sum(1 for r in records if r.match_method != "unmatched"),
        "records":             [asdict(r) for r in records],
    }

def write_category_json(records: List[LeadRecord]):
    categories = {
        "hot_stack":       [r for r in records if r.hot_stack],
        "sheriff_sales":   [r for r in records if r.doc_type=="SHERIFF"],
        "probate":         [r for r in records if r.doc_type=="PRO"],
        "inherited":       [r for r in records if r.is_inherited],
        "tax_delinquent":  [r for r in records if r.doc_type=="TAX"],
        "foreclosure":     [r for r in records if r.doc_type in {"NOFC","LP","TAXDEED"}],
        "pre_foreclosure": [r for r in records if r.doc_type in {"NOFC","LP"}],
        "liens":           [r for r in records if r.doc_type in {"LN","LNMECH","LNFED","LNIRS","LNCORPTX","MEDLN"}],
        "absentee":        [r for r in records if r.is_absentee],
        "out_of_state":    [r for r in records if r.is_out_of_state],
        "subject_to":      [r for r in records if r.subject_to_score>=50],
        "divorces":        [r for r in records if r.doc_type=="DIVORCE"],
        "zillow_valued":   [r for r in records if r.zillow_value],
    }
    descs = {
        "hot_stack":       "2+ distress signals - highest priority",
        "sheriff_sales":   "Properties scheduled for sheriff auction",
        "probate":         "Estate / probate filings",
        "inherited":       "Inherited / estate properties enriched with parcel addresses",
        "tax_delinquent":  "Tax delinquent / tax foreclosure",
        "foreclosure":     "Active foreclosure / lis pendens / tax deed",
        "pre_foreclosure": "Pre-foreclosure and lis pendens filings",
        "liens":           "Judgment, federal, mechanic liens",
        "absentee":        "Absentee owner - mailing differs from property",
        "out_of_state":    "Out-of-state owner",
        "subject_to":      "Subject-To candidates (score 50+)",
        "divorces":        "Divorce / dissolution filings",
        "zillow_valued":   "Leads with Zillow home value estimates",
    }
    for cat, recs in categories.items():
        recs_s  = sorted(recs, key=lambda r:(r.hot_stack,r.distress_count,r.subject_to_score,r.score), reverse=True)
        payload = {
            "fetched_at":  datetime.now(timezone.utc).isoformat(),
            "source":      SOURCE_NAME,
            "category":    cat,
            "description": descs.get(cat,""),
            "total":       len(recs_s),
            "records":     [asdict(r) for r in recs_s],
        }
        for path in [DATA_DIR/f"{cat}.json", DASHBOARD_DIR/f"{cat}.json"]:
            write_json(path, payload)
        logging.info("Wrote %s: %s records", cat, len(recs_s))

def write_csv(records: List[LeadRecord], csv_path: Path):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Subject-To Score","Motivated Seller Flags","Distress Sources","Distress Count",
        "Hot Stack","Absentee Owner","Out-of-State Owner","Inherited",
        "Assessed Value","Est Market Value","Zillow Value","Value Source",
        "Est Equity","Est Arrears","Est Payoff","Mortgage Signals",
        "Parcel ID","LUC Code","Match Method","Match Score","Source","Public Records URL",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in records:
            fn, ln = split_name(r.owner)
            w.writerow({
                "First Name":fn, "Last Name":ln,
                "Mailing Address":r.mail_address, "Mailing City":r.mail_city,
                "Mailing State":r.mail_state, "Mailing Zip":r.mail_zip,
                "Property Address":r.prop_address, "Property City":r.prop_city,
                "Property State":r.prop_state, "Property Zip":r.prop_zip,
                "Lead Type":r.cat_label, "Document Type":r.doc_type,
                "Date Filed":r.filed, "Document Number":r.doc_num,
                "Amount/Debt Owed": f"${r.amount:,.2f}" if r.amount else "",
                "Seller Score":r.score, "Subject-To Score":r.subject_to_score,
                "Motivated Seller Flags":"; ".join(r.flags),
                "Distress Sources":"; ".join(r.distress_sources),
                "Distress Count":r.distress_count,
                "Hot Stack":      "YES" if r.hot_stack else "",
                "Absentee Owner": "YES" if r.is_absentee else "",
                "Out-of-State Owner":"YES" if r.is_out_of_state else "",
                "Inherited":      "YES" if r.is_inherited else "",
                "Assessed Value": f"${r.assessed_value:,.0f}" if r.assessed_value else "",
                "Est Market Value":f"${r.estimated_value:,.0f}" if r.estimated_value else "",
                "Zillow Value":   f"${r.zillow_value:,.0f}" if r.zillow_value else "",
                "Value Source":r.value_source,
                "Est Equity":  f"${r.est_equity:,.0f}" if r.est_equity is not None else "",
                "Est Arrears": f"${r.est_arrears:,.0f}" if r.est_arrears else "",
                "Est Payoff":  f"${r.est_payoff:,.0f}" if r.est_payoff else "",
                "Mortgage Signals":"; ".join(r.mortgage_signals),
                "Parcel ID":r.parcel_id, "LUC Code":r.luc,
                "Match Method":r.match_method, "Match Score":f"{r.match_score:.2f}",
                "Source":SOURCE_NAME, "Public Records URL":r.clerk_url,
            })
    logging.info("Wrote CSV: %s (%s rows)", csv_path, len(records))

# ── Main ───────────────────────────────────────────────────────────────────
async def main():
    ap = argparse.ArgumentParser(description="Toledo / Lucas County Motivated Seller Scraper")
    ap.add_argument("--out-csv",     default=str(DEFAULT_ENRICHED_CSV_PATH))
    ap.add_argument("--dbf-address", default="",
                    help="Full path to ParcelsAddress.dbf (overrides DBF_PARCELS_ADDRESS env var)")
    args = ap.parse_args()

    # Allow CLI override of DBF path
    global DBF_PARCELS_ADDRESS
    if args.dbf_address:
        DBF_PARCELS_ADDRESS = Path(args.dbf_address)

    ensure_dirs()
    log_setup()
    logging.info("=== Toledo / Lucas County Motivated Seller Intelligence v2 ===")
    logging.info("Zillow API   : %s", "ENABLED" if ZILLOW_API_KEY else "DISABLED (set ZILLOW_API_KEY)")
    logging.info("Parcel DBF   : %s", DBF_PARCELS_ADDRESS)

    # 1. Load local parcel DBF
    parcels = load_parcel_data()
    if not parcels:
        logging.warning(
            "No parcel data loaded — absentee/OOS/SubTo/address enrichment will be empty.\n"
            "  Pass: --dbf-address /path/to/ParcelsAddress.dbf\n"
            "  OR copy file to: data/parcels/ParcelsAddress.dbf"
        )

    # 2. Run all scrapers
    cp_records   = await scrape_tln_common_pleas()
    fc_records   = await scrape_tln_foreclosure_notices()
    sheriff_recs = await scrape_sheriff_sales()
    probate_recs = await scrape_tln_probate()
    divorce_recs = await scrape_tln_divorces()
    tax_recs     = await scrape_tax_delinquent()

    all_records = cp_records + fc_records + sheriff_recs + probate_recs + divorce_recs + tax_recs
    logging.info("Total before enrich: %s", len(all_records))

    # 3. Enrich with parcel data + Zillow + scoring
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
        key=lambda r:(r.doc_type=="SHERIFF", r.hot_stack, r.distress_count,
                      r.subject_to_score, r.score, r.filed),
        reverse=True
    )

    parcel_matched = sum(1 for r in all_records if r.match_method != "unmatched")
    logging.info(
        "Total final: %s | Parcel matched: %s | Zillow calls: %s/%s",
        len(all_records), parcel_matched, ZILLOW_CALLS, ZILLOW_MAX_CALLS
    )

    # 5. Write outputs
    payload = build_payload(all_records)
    for path in DEFAULT_OUTPUT_JSON_PATHS:
        write_json(path, payload)
        logging.info("Wrote %s (%s records)", path, len(all_records))

    write_category_json(all_records)
    write_csv(all_records, DEFAULT_OUTPUT_CSV_PATH)
    if Path(args.out_csv) != DEFAULT_OUTPUT_CSV_PATH:
        write_csv(all_records, Path(args.out_csv))

    logging.info(
        "=== DONE === Total:%s | Sheriff:%s | HotStack:%s | Probate:%s | "
        "PreFC:%s | FC(all):%s | Liens:%s | Tax:%s | "
        "Absentee:%s | OOS:%s | SubTo:%s | Inherited:%s | "
        "Divorce:%s | ZillowValued:%s | ParcelMatched:%s",
        len(all_records),
        sum(1 for r in all_records if r.doc_type=="SHERIFF"),
        sum(1 for r in all_records if r.hot_stack),
        sum(1 for r in all_records if r.doc_type=="PRO"),
        sum(1 for r in all_records if r.doc_type in {"NOFC","LP"}),
        sum(1 for r in all_records if r.doc_type in {"NOFC","LP","TAXDEED"}),
        sum(1 for r in all_records if r.doc_type in {"LN","LNMECH","LNFED"}),
        sum(1 for r in all_records if r.doc_type=="TAX"),
        sum(1 for r in all_records if r.is_absentee),
        sum(1 for r in all_records if r.is_out_of_state),
        sum(1 for r in all_records if r.subject_to_score>=50),
        sum(1 for r in all_records if r.is_inherited),
        sum(1 for r in all_records if r.doc_type=="DIVORCE"),
        sum(1 for r in all_records if r.zillow_value),
        parcel_matched,
    )

if __name__ == "__main__":
    asyncio.run(main())
