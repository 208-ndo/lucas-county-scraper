"""
Toledo / Lucas County — Motivated Seller Intelligence Platform v3
=================================================================
Sources, parcel enrichment, Redfin AVM — all free, no auth required.
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

DBF_PARCELS_ADDRESS = Path(os.getenv("DBF_PARCELS_ADDRESS",
                            str(BASE_DIR / "data" / "parcels" / "ParcelsAddress.dbf")))
CSV_PARCELS_ADDRESS = BASE_DIR / "data" / "parcels" / "ParcelsAddress.csv"

LOOKBACK_DAYS = 90
SOURCE_NAME   = "Toledo / Lucas County, Ohio"
OH_APPR_RATE  = 0.04

ZILLOW_API_KEY   = os.getenv("ZILLOW_API_KEY", "")
ZILLOW_API_HOST  = "zillow-com1.p.rapidapi.com"
ZILLOW_CACHE: Dict[str, Optional[float]] = {}
ZILLOW_CALLS = 0; ZILLOW_MAX_CALLS = 400

REDFIN_CACHE: Dict[str, Optional[dict]] = {}
REDFIN_CALLS = 0; REDFIN_MAX_CALLS = 200

TLN_BASE             = "https://www.toledolegalnews.com"
TLN_COMMON_PLEAS_URL = "https://www.toledolegalnews.com/courts/common_pleas/"
TLN_FORECLOSURES_URL = "https://www.toledolegalnews.com/legal_notices/foreclosures/"
TLN_PROBATE_URL      = "https://www.toledolegalnews.com/courts/probate/"
TLN_DOMESTIC_URL     = "https://www.toledolegalnews.com/courts/domestic_court/"
SHERIFF_BASE         = "https://lucas.sheriffsaleauction.ohio.gov"
LC_TREASURER_URL     = "https://www.lucascountytreasurer.org/delinquent-taxes"

# Lucas County Auditor property search — free, no auth
LC_AUDITOR_SEARCH = "https://lucascountyauditor.org/property-search"
LC_AUDITOR_API    = "https://lucascountyauditor.org/api/property"

# Lucas County Common Pleas case search — for sheriff owner lookup
LC_CPC_CASE  = "http://lcapps.co.lucas.oh.us/CPC/CaseSearch.aspx"
LC_CPC_CASE2 = "https://ewservices.co.lucas.oh.us/CaseView/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

REDFIN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json,*/*",
    "Referer": "https://www.redfin.com/",
}

LEGAL_BOILERPLATE = {
    "named above be required to answer","to the defendant the unknown spouse",
    "unknown spouse","unknown heirs","john doe","jane doe","et al",
    "all unknown parties","parties unknown","defendant unknown",
    "to be named","all others claiming","whose last known",
    "and all other persons","notice of foreclosure","defendants",
    "plaintiff","named above","be required","required to answer",
}

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

STACK_BONUS = {2:15, 3:25, 4:40}
NOISE_WORDS = [
    "annual reports","dissolutions","zoning","name changes","bid notices",
    "public hearings","whatsapp","facebook","twitter","sign up","log in",
    "contact us","terms of use","privacy","e-edition","classifieds",
]
JUNK_ADDR = {"increments","bids pl","am et case","property addr","pending","unknown","tbd"}

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
    redfin_value: Optional[float] = None
    zillow_value: Optional[float] = None
    value_source: str = ""
    last_sale_price: Optional[float] = None
    last_sale_year: Optional[int] = None
    beds: Optional[int] = None
    baths: Optional[float] = None
    sqft: Optional[int] = None
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
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")

def save_debug(name: str, content: str):
    try: (DEBUG_DIR / name).write_text(content[:50000], encoding="utf-8")
    except: pass

def clean(v) -> str:
    if v is None: return ""
    return re.sub(r"\s+", " ", str(v)).strip()

def norm_state(v: str) -> str:
    v = re.sub(r"[^A-Z]", "", clean(v).upper())
    return v if v in STATE_CODES else ""

def retry_get(url, attempts=3, timeout=20, delay=2.0, hdrs=None, **kw):
    h = hdrs or HEADERS; last = None
    for i in range(1, attempts+1):
        try:
            r = requests.get(url, headers=h, timeout=timeout, allow_redirects=True, **kw)
            r.raise_for_status(); return r
        except Exception as e:
            last = e
            if i < attempts: time.sleep(delay * i + random.uniform(0,1))
    raise last

async def pw_fetch(url: str, wait_ms: int = 2500) -> str:
    if not url or not url.startswith("http"): return ""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-blink-features=AutomationControlled",
                      "--disable-dev-shm-usage","--disable-gpu"]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                viewport={"width":1366,"height":768},
                locale="en-US", timezone_id="America/New_York",
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
                        await page.wait_for_timeout(400 + random.randint(0,200))
                    except: pass
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(wait_ms + random.randint(0,400))
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
    c = re.sub(r"[^0-9.\-]","",v)
    try: return float(c) if c else None
    except: return None

def norm_addr_key(address: str) -> str:
    addr = clean(address).upper()
    for old, new in [
        ("NORTH","N"),("SOUTH","S"),("EAST","E"),("WEST","W"),
        ("N.","N"),("S.","S"),("E.","E"),("W.","W"),
        ("STREET","ST"),("AVENUE","AVE"),("ROAD","RD"),("DRIVE","DR"),
        ("BOULEVARD","BLVD"),("LANE","LN"),("COURT","CT"),("PLACE","PL"),
        ("TERRACE","TER"),("CIRCLE","CIR"),("PARKWAY","PKWY"),
    ]:
        addr = re.sub(r'\b'+old+r'\b', new, addr)
    return re.sub(r"\s+"," ",re.sub(r"[^A-Z0-9\s]","",addr)).strip()

def norm_name(n: str) -> str:
    n = clean(n).upper()
    return re.sub(r"\s+"," ",re.sub(r"[^A-Z0-9,&.\- /']"," ",n)).strip()

def name_variants(name: str) -> List[str]:
    n = clean(name).upper()
    if not n: return []
    n = re.sub(r'\b(JR|SR|II|III|IV|ESQ|DEC|DECEASED|ET\s+AL|ETAL)\.?\b','',n).strip()
    variants: set = {n}
    parts = [p for p in re.split(r"[\s,]+",n) if p and len(p)>1]
    if len(parts)>=2:
        variants.update([
            " ".join(parts), f"{parts[-1]} {parts[0]}",
            f"{parts[0]} {parts[-1]}", f"{parts[-1]}, {parts[0]}",
            f"{parts[0]}, {parts[-1]}", " ".join(sorted(parts)),
            parts[0], parts[-1],
        ])
    return [v.strip() for v in variants if v.strip()]

def likely_corp(n: str) -> bool:
    CORP = {"LLC","INC","CORP","CO","TRUST","BANK","LTD","LP","PLC","HOLDINGS",
            "PROPERTIES","REALTY","INVESTMENTS","CAPITAL","GROUP","PARTNERS"}
    return any(t in CORP for t in norm_name(n).split())

def is_boilerplate(name: str) -> bool:
    if not name or len(name.strip())<3: return True
    n = clean(name).lower()
    if any(p in n for p in LEGAL_BOILERPLATE): return True
    if re.fullmatch(r"[\d\s\-\.]+",n): return True
    return False

def try_parse_date(text: str) -> Optional[str]:
    text = clean(text)
    if not text: return None
    for pat in [r"\b(\d{4}-\d{2}-\d{2})\b",r"\b(\d{1,2}/\d{1,2}/\d{4})\b",
                r"\b(\d{1,2}/\d{1,2}/\d{2})\b",
                r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b"]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(0)
            for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%B %d, %Y","%B %d %Y"):
                try: return datetime.strptime(raw,fmt).date().isoformat()
                except: continue
    return None

def is_recent(filed: str, days: int = LOOKBACK_DAYS) -> bool:
    try: return datetime.fromisoformat(filed).date() >= (datetime.now().date() - timedelta(days=days))
    except: return True

def is_noise(text: str) -> bool:
    return any(w in text.lower() for w in NOISE_WORDS)

def is_valid_address(addr: str) -> bool:
    if not addr: return False
    a = addr.lower()
    if any(t in a for t in JUNK_ADDR): return False
    if not re.match(r"^\d{1,5}\s+[a-zA-Z]", addr.strip()): return False
    return True

def infer_doc_type(text: str) -> Optional[str]:
    t = clean(text).upper()
    if any(x in t for x in ["LIS PENDENS"," LP ","LP-"]): return "LP"
    if any(x in t for x in ["FORECLOS","NOFC","COMPLAINT TO FORECLOSE","MTG ON"]): return "NOFC"
    if "SHERIFF" in t: return "SHERIFF"
    if any(x in t for x in ["DIVORCE","DISSOLUTION","DR-","DM-"]): return "DIVORCE"
    if "EVICTION" in t: return "EVICTION"
    if "CERTIFIED JUDGMENT" in t: return "CCJ"
    if "JUDGMENT" in t: return "JUD"
    if any(x in t for x in ["TAX DEED","TAXDEED"]): return "TAXDEED"
    if any(x in t for x in ["IRS","INTERNAL REVENUE"]): return "LNIRS"
    if any(x in t for x in ["FEDERAL TAX","US TAX","LNFED","DEPT OF TAXATION"]): return "LNFED"
    if "MECHANIC" in t: return "LNMECH"
    if "LIEN" in t: return "LN"
    if any(x in t for x in ["PROBATE","ESTATE OF"]): return "PRO"
    if "BANKRUPTCY" in t: return "BK"
    return None

def classify_distress(doc_type: str) -> Optional[str]:
    return {
        "LP":"lis_pendens","RELLP":"lis_pendens","NOFC":"foreclosure",
        "TAXDEED":"tax_delinquent","JUD":"judgment","CCJ":"judgment",
        "LN":"lien","LNHOA":"lien","LNFED":"lien","LNIRS":"lien",
        "LNCORPTX":"lien","MEDLN":"lien","LNMECH":"mechanic_lien",
        "NOC":"mechanic_lien","TAX":"tax_delinquent","PRO":"probate",
        "SHERIFF":"sheriff_sale","CODEVIOLATION":"code_violation",
        "DIVORCE":"divorce","EVICTION":"eviction","BK":"bankruptcy",
    }.get(clean(doc_type).upper())

def cat_flags(doc_type: str, owner: str = "") -> List[str]:
    flags = []; dt = clean(doc_type).upper()
    if dt=="LP": flags.append("Lis pendens")
    if dt=="NOFC": flags.append("Pre-foreclosure")
    if dt in {"JUD","CCJ","DRJUD"}: flags.append("Judgment lien")
    if dt in {"TAXDEED","LNCORPTX","LNIRS","LNFED","TAX"}: flags.append("Tax lien")
    if dt in {"LNMECH","NOC"}: flags.append("Mechanic lien")
    if dt=="PRO": flags.append("Probate / estate")
    if dt=="SHERIFF": flags.append("Sheriff sale scheduled")
    if dt=="DIVORCE": flags.append("Divorce filing")
    if dt=="EVICTION": flags.append("Eviction filed")
    if dt=="BK": flags.append("Bankruptcy")
    if likely_corp(norm_name(owner)): flags.append("LLC / corp owner")
    return list(dict.fromkeys(flags))

def is_absentee(prop: str, mail: str, mail_state: str = "") -> bool:
    if not prop or not mail: return False
    if re.search(r"\bP\.?\s*O\.?\s*BOX\b", mail.upper()): return True
    s = norm_state(mail_state)
    if s and s != "OH": return True
    pk = norm_addr_key(prop); mk = norm_addr_key(mail)
    if not pk or not mk or pk==mk: return False
    return " ".join(pk.split()[:2]) != " ".join(mk.split()[:2])

def is_oos(mail_state: str) -> bool:
    s = norm_state(mail_state)
    return bool(s and s != "OH")

# ── Sheriff case lookup — get owner name from CPC by case number ───────────
_sheriff_owner_cache: Dict[str, str] = {}

def lookup_sheriff_owner(case_num: str) -> str:
    """
    Look up the property owner/defendant for a sheriff sale by case number.
    Uses Lucas County Common Pleas public case search.
    Returns owner name or empty string.
    """
    if not case_num or case_num in _sheriff_owner_cache:
        return _sheriff_owner_cache.get(case_num, "")
    # Normalize case number
    cn = re.sub(r"[\s\-]","",case_num.upper())
    try:
        # Try CPC case search
        params = {"casenumber": case_num, "court": "CV"}
        r = requests.get(LC_CPC_CASE, params=params, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")
            text = soup.get_text(" ")
            # Look for defendant name
            for pat in [
                r"(?:defendant|mortgagor|owner)[:\s]+([A-Z][A-Za-z\s,\.]{3,40}?)(?:,|\.|$|\n)",
                r"vs\.?\s+([A-Z][A-Za-z\s,\.]{3,40}?)(?:\s+whose|\s+last|,|\.|$)",
            ]:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    name = clean(m.group(1)).title()
                    if not is_boilerplate(name) and len(name) >= 4:
                        _sheriff_owner_cache[case_num] = name
                        return name
    except Exception as e:
        logging.debug("Sheriff owner lookup %s: %s", case_num, e)
    _sheriff_owner_cache[case_num] = ""
    return ""

# ── Auditor property search fallback ──────────────────────────────────────
_auditor_cache: Dict[str, Optional[dict]] = {}

def auditor_lookup(owner: str) -> Optional[dict]:
    key = norm_name(owner)
    if key in _auditor_cache: return _auditor_cache[key]
    if not owner or len(owner) < 4 or is_boilerplate(owner):
        _auditor_cache[key] = None; return None
    try:
        r = requests.get(LC_AUDITOR_API,
                         params={"query":owner,"type":"owner","limit":3},
                         headers={**HEADERS,"Accept":"application/json"},
                         timeout=8)
        if r.status_code == 200:
            data = r.json()
            results = data.get("results", data.get("properties", data.get("data",[])))
            if results and isinstance(results, list):
                for item in results:
                    addr = clean(item.get("address","") or item.get("siteAddress","") or "")
                    if is_valid_address(addr):
                        result = {
                            "prop_address": addr.title(),
                            "prop_city": clean(item.get("city","") or "Toledo").title(),
                            "prop_zip":  clean(item.get("zip","") or item.get("zipCode","")),
                            "parcel_id": clean(item.get("parcelId","") or item.get("parcel_id","")),
                        }
                        _auditor_cache[key] = result; return result
    except Exception as e:
        logging.debug("Auditor lookup %s: %s", owner[:25], e)
    _auditor_cache[key] = None; return None

# ── Redfin AVM ─────────────────────────────────────────────────────────────
def get_redfin_value(address: str, city: str = "Toledo",
                     state: str = "OH", zip_code: str = "") -> Optional[dict]:
    global REDFIN_CALLS
    if not is_valid_address(address) or REDFIN_CALLS >= REDFIN_MAX_CALLS: return None
    full = f"{address}, {city}, {state}"
    if zip_code: full += f" {zip_code}"
    ck = norm_addr_key(full)
    if ck in REDFIN_CACHE: return REDFIN_CACHE[ck]
    try:
        r = requests.get("https://www.redfin.com/stingray/do/location-autocomplete",
                         params={"location":full,"v":"2","count":"5"},
                         headers=REDFIN_HEADERS, timeout=12)
        REDFIN_CALLS += 1
        if r.status_code != 200: REDFIN_CACHE[ck]=None; return None
        raw = r.text
        for pfx in ["{}&&","//","while(1);"]:
            if raw.startswith(pfx): raw=raw[len(pfx):]; break
        data = json.loads(raw)
        prop_url = None
        for section in data.get("payload",{}).get("sections",[]):
            for row in section.get("rows",[]):
                u = row.get("url","")
                if u and "/home/" in u: prop_url=u; break
            if prop_url: break
        if not prop_url: REDFIN_CACHE[ck]=None; return None
        time.sleep(1.5 + random.uniform(0,0.5))
        r2 = requests.get("https://www.redfin.com/stingray/api/home/details/initialInfo",
                          params={"path":prop_url,"accessLevel":"1"},
                          headers=REDFIN_HEADERS, timeout=12)
        REDFIN_CALLS += 1
        if r2.status_code != 200: REDFIN_CACHE[ck]=None; return None
        raw2 = r2.text
        for pfx in ["{}&&","//","while(1);"]:
            if raw2.startswith(pfx): raw2=raw2[len(pfx):]; break
        detail = json.loads(raw2)
        p2 = detail.get("payload",{})
        hi = p2.get("homeInfo",{})
        avm = (hi.get("avm") or hi.get("priceEstimate") or hi.get("estimate") or
               p2.get("avm") or p2.get("estimatedValue") or
               p2.get("mediaBrowserInfo",{}).get("avm") or
               p2.get("aboveTheFold",{}).get("avm"))
        beds  = hi.get("beds") or hi.get("bedCount")
        baths = hi.get("baths") or hi.get("bathCount")
        sqft  = hi.get("sqFt") or hi.get("sqft") or hi.get("livingArea")
        lsp   = hi.get("lastSoldPrice") or hi.get("price")
        lsd   = hi.get("lastSoldDate","")
        lsy   = None
        if lsd:
            try: lsy = datetime.strptime(str(lsd)[:10],"%Y-%m-%d").year
            except:
                try: lsy = int(str(lsd)[:4])
                except: pass
        if not avm and lsp:
            yrs = max(0, datetime.now().year-(lsy or datetime.now().year))
            avm = round(float(lsp)*((1+OH_APPR_RATE)**yrs))
        if avm and float(avm)>5000:
            result = {"avm":float(avm),"last_sale_price":float(lsp) if lsp else None,
                      "last_sale_year":lsy,"beds":int(beds) if beds else None,
                      "baths":float(baths) if baths else None,
                      "sqft":int(sqft) if sqft else None}
            REDFIN_CACHE[ck]=result
            logging.info("Redfin: %s -> $%,.0f (#%s)", address[:35], avm, REDFIN_CALLS)
            return result
    except json.JSONDecodeError: pass
    except Exception as e: logging.debug("Redfin %s: %s", address[:30], e)
    REDFIN_CACHE[ck]=None; return None

def get_zillow_value(address: str, city: str = "Toledo",
                     state: str = "OH", zip_code: str = "") -> Optional[float]:
    global ZILLOW_CALLS
    if not ZILLOW_API_KEY or not is_valid_address(address): return None
    if ZILLOW_CALLS >= ZILLOW_MAX_CALLS: return None
    full = f"{address}, {city}, {state}"
    if zip_code: full += f" {zip_code}"
    ck = norm_addr_key(full)
    if ck in ZILLOW_CACHE: return ZILLOW_CACHE[ck]
    time.sleep(2.0+random.uniform(0,0.5))
    try:
        hdrs = {"X-RapidAPI-Key":ZILLOW_API_KEY,"X-RapidAPI-Host":ZILLOW_API_HOST}
        r = requests.get(f"https://{ZILLOW_API_HOST}/propertyExtendedSearch",
                         headers=hdrs, params={"location":full}, timeout=10)
        r.raise_for_status(); ZILLOW_CALLS += 1
        props = r.json().get("props",[])
        if props:
            first = props[0]
            z = (first.get("zestimate") or first.get("price") or
                 first.get("hdpData",{}).get("homeInfo",{}).get("zestimate"))
            if z:
                val = float(str(z).replace(",","").replace("$",""))
                if val>1000: ZILLOW_CACHE[ck]=val; return val
    except Exception as e: logging.debug("Zillow %s: %s", address[:30], e)
    ZILLOW_CACHE[ck]=None; return None

def get_best_value(record: "LeadRecord") -> tuple:
    if is_valid_address(record.prop_address):
        rf = get_redfin_value(record.prop_address, record.prop_city or "Toledo",
                              record.prop_state or "OH", record.prop_zip or "")
        if rf and rf.get("avm") and rf["avm"]>5000:
            if rf.get("last_sale_price") and not record.last_sale_price:
                record.last_sale_price = rf["last_sale_price"]
            if rf.get("last_sale_year") and not record.last_sale_year:
                record.last_sale_year = rf["last_sale_year"]
            if rf.get("beds")  and not record.beds:  record.beds  = rf["beds"]
            if rf.get("baths") and not record.baths: record.baths = rf["baths"]
            if rf.get("sqft")  and not record.sqft:  record.sqft  = rf["sqft"]
            record.redfin_value = rf["avm"]
            return rf["avm"], "Redfin AVM"
        if ZILLOW_API_KEY:
            zv = get_zillow_value(record.prop_address, record.prop_city or "Toledo",
                                  record.prop_state or "OH", record.prop_zip or "")
            if zv and zv>5000:
                record.zillow_value = zv; return zv, "Zillow Zestimate"
    if record.assessed_value and record.assessed_value>1000:
        return round(record.assessed_value/0.35,2), "Assessed Value (est)"
    if record.last_sale_price and record.last_sale_price>5000:
        yrs = max(0, datetime.now().year-(record.last_sale_year or datetime.now().year))
        return round(record.last_sale_price*((1+OH_APPR_RATE)**yrs),2), "Last Sale (appreciated)"
    return None, ""

def estimate_financials(record: "LeadRecord") -> "LeadRecord":
    signals=[]; sto=0
    if not record.estimated_value:
        val, source = get_best_value(record)
        if val: record.estimated_value=val; record.value_source=source
    mv = record.estimated_value
    if record.last_sale_price and record.last_sale_year and record.last_sale_price>5000:
        yrs=max(0,min(30,datetime.now().year-record.last_sale_year))
        orig=record.last_sale_price*0.80; mr=0.065/12; n=360; paid=yrs*12
        if paid<n:
            bal=orig*((1+mr)**n-(1+mr)**paid)/((1+mr)**n-1)
            record.est_mortgage_balance=round(max(0,bal),2)
        else: record.est_mortgage_balance=0.0
    if mv and record.est_mortgage_balance is not None:
        record.est_equity=round(mv-record.est_mortgage_balance,2)
    elif mv and not record.last_sale_price:
        record.est_mortgage_balance=round(mv*0.50,2)
        record.est_equity=round(mv*0.50,2)
        record.est_payoff=record.est_mortgage_balance
    if record.doc_type in {"LP","NOFC","TAXDEED","SHERIFF"} and record.amount and record.amount>0:
        record.est_arrears=record.amount
        record.est_payoff=record.est_mortgage_balance or record.amount
        signals.append(f"Arrears ~${record.est_arrears:,.0f}")
    if "Tax lien" in record.flags and record.amount and record.amount>0:
        record.est_arrears=(record.est_arrears or 0)+record.amount
        signals.append(f"Tax owed ~${record.amount:,.0f}")
    if record.est_equity is not None:
        if   record.est_equity>50000: sto+=30; signals.append("High equity")
        elif record.est_equity>20000: sto+=20; signals.append("Moderate equity")
        elif record.est_equity>0:     sto+=10
        else: signals.append("Underwater")
    if record.doc_type in {"LP","NOFC","SHERIFF"}: sto+=25; signals.append("Active foreclosure")
    if record.doc_type=="PRO":    sto+=20; signals.append("Estate / probate")
    if record.is_absentee:         sto+=15; signals.append("Absentee owner")
    if record.is_out_of_state:     sto+=10; signals.append("Out-of-state owner")
    if record.is_inherited:        sto+=20; signals.append("Inherited property")
    if "Tax lien" in record.flags: sto+=15
    if record.est_mortgage_balance and record.estimated_value and record.estimated_value>0:
        ltv=record.est_mortgage_balance/record.estimated_value
        if   ltv<0.5: sto+=20; signals.append("Low LTV <50%")
        elif ltv<0.7: sto+=10; signals.append("LTV <70%")
        elif ltv>0.95: signals.append("High LTV >95%")
    if sto>=50 and "Subject-To Candidate" not in " ".join(record.flags):
        record.flags.append("Subject-To Candidate")
    if sto>=70 and "Prime Subject-To" not in " ".join(record.flags):
        record.flags.append("Prime Subject-To")
    record.subject_to_score=min(sto,100); record.mortgage_signals=signals
    return record

def score_record(record: "LeadRecord") -> int:
    score=30; lf={f.lower() for f in record.flags}; fs=0
    if "lis pendens"         in lf: fs+=20
    if "pre-foreclosure"     in lf: fs+=20
    if "judgment lien"       in lf: fs+=15
    if "tax lien"            in lf: fs+=15
    if "mechanic lien"       in lf: fs+=10
    if "probate / estate"    in lf: fs+=15
    if "sheriff sale scheduled" in lf: fs+=35
    if "code violation"      in lf: fs+=20
    if "eviction filed"      in lf: fs+=18
    if "divorce filing"      in lf: fs+=15
    if "absentee owner"      in lf: fs+=10
    if "out-of-state owner"  in lf: fs+=12
    if "bankruptcy"          in lf: fs+=12
    if "inherited property"  in lf: fs+=15
    if "subject-to candidate" in lf: fs+=15
    if "prime subject-to"    in lf: fs+=20
    score+=min(fs,70)
    if "lis pendens" in lf and "pre-foreclosure" in lf: score+=20
    if record.amount is not None:
        score+=15 if record.amount>100000 else (10 if record.amount>50000 else 5)
    if record.estimated_value or record.redfin_value: score+=5
    if record.filed:
        try:
            if datetime.fromisoformat(record.filed).date()>=(datetime.now().date()-timedelta(days=7)):
                if "New this week" not in record.flags: record.flags.append("New this week")
                score+=5
        except: pass
    if is_valid_address(record.prop_address): score+=5
    if record.mail_address: score+=3
    dc=len(set(record.distress_sources)); record.distress_count=dc
    bk=min(dc,4)
    if bk>=2:
        score+=STACK_BONUS.get(bk,STACK_BONUS[4]); record.hot_stack=True
        if "Hot Stack" not in " ".join(record.flags): record.flags.append("Hot Stack")
    return min(score,100)

# ── Parcel data loader ─────────────────────────────────────────────────────
def _parse_prop_addr(raw: str) -> tuple:
    if not raw: return "","Toledo","OH",""
    raw=raw.strip()
    m=re.match(r"^(\d+\s+.+?),\s*([A-Za-z\s]+?)\s+([A-Z]{2})\s+(\d{5})?$",raw)
    if m: return clean(m.group(1)).title(),clean(m.group(2)).title(),m.group(3).upper(),m.group(4) or ""
    m2=re.match(r"^(\d+\s+.+?),\s*([A-Za-z\s]+?)\s+([A-Z]{2})$",raw)
    if m2: return clean(m2.group(1)).title(),clean(m2.group(2)).title(),m2.group(3).upper(),""
    return raw.title(),"Toledo","OH",""

def _parse_mail_addr(raw: str) -> tuple:
    if not raw: return "","","",""
    raw=raw.strip()
    m=re.match(r"^(.+?),\s*([A-Za-z\s\.]+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)?$",raw)
    if m: return clean(m.group(1)).title(),clean(m.group(2)).title(),m.group(3).upper(),(m.group(4) or "")[:5]
    m2=re.match(r"^(.+?),\s*([A-Za-z\s]+?)\s+([A-Z]{2})$",raw)
    if m2: return clean(m2.group(1)).title(),clean(m2.group(2)).title(),m2.group(3).upper(),""
    return raw.title(),"","",""

def _build_addr_from_parts(R: dict) -> str:
    parts = [R.get("ADRNO",""), R.get("ADRDIR",""), R.get("ADRSTR",""),
             R.get("ADRSUF",""), R.get("ADRSUF2","")]
    return " ".join(p for p in parts if p and p!="None").strip().title()

def load_parcel_data() -> Dict[str, dict]:
    parcels: Dict[str, dict] = {}

    # Try DBF first
    if DBF_PARCELS_ADDRESS.exists():
        try:
            from dbfread import DBF as DbfReader
            logging.info("Loading ParcelsAddress.dbf ...")
            count = 0
            for row in DbfReader(str(DBF_PARCELS_ADDRESS), load=False, encoding="latin-1"):
                try:
                    R = {k.upper(): clean(v) for k,v in dict(row).items()}
                    owner_raw = R.get("OWNER","") or R.get("OWN","") or R.get("NAME","")
                    prop_raw  = R.get("PROPERTY_A","")
                    mail_raw  = R.get("MAILING_AD","")
                    parid     = R.get("PARID",""); luc = R.get("LUC","")
                    if not owner_raw and not prop_raw: continue
                    prop_street,prop_city,prop_state,prop_zip = _parse_prop_addr(prop_raw)
                    if not prop_street:
                        prop_street = _build_addr_from_parts(R)
                        prop_city   = R.get("CITY","Toledo").title()
                        prop_zip    = R.get("ZIP","") or R.get("ZIP_CODE","")
                    mail_street,mail_city,mail_state,mail_zip = _parse_mail_addr(mail_raw)
                    if not prop_street: continue
                    rec = {"parcel_id":parid,"owner":owner_raw.title(),
                           "prop_address":prop_street,"prop_city":prop_city or "Toledo",
                           "prop_state":prop_state or "OH","prop_zip":prop_zip,
                           "mail_address":mail_street,"mail_city":mail_city,
                           "mail_state":mail_state or "OH","mail_zip":mail_zip,"luc":luc}
                    ak = norm_addr_key(prop_street)
                    if ak: parcels[ak]=rec
                    if owner_raw:
                        for v in name_variants(owner_raw):
                            if f"OWNER:{v}" not in parcels: parcels[f"OWNER:{v}"]=rec
                    count+=1
                except: continue
            logging.info("DBF: %s parcels loaded",count)
            if count>0: return parcels
        except ImportError: logging.warning("dbfread not installed")
        except Exception as e: logging.error("DBF error: %s",e)

    # Try CSV (from download_parcels.py)
    if CSV_PARCELS_ADDRESS.exists():
        logging.info("Loading ParcelsAddress.csv ...")
        count=0
        try:
            with CSV_PARCELS_ADDRESS.open(encoding="utf-8",errors="ignore") as f:
                for row in csv.DictReader(f):
                    try:
                        R = {k.upper(): clean(v) for k,v in row.items()}
                        owner_raw = R.get("OWNER","") or R.get("OWN","") or R.get("NAME","")
                        parid     = R.get("PARID",""); luc=R.get("LUC","")
                        prop_raw  = R.get("PROPERTY_A","")
                        if prop_raw:
                            prop_street,prop_city,prop_state,prop_zip = _parse_prop_addr(prop_raw)
                        else:
                            prop_street = _build_addr_from_parts(R)
                            prop_city   = R.get("CITY","Toledo").title()
                            prop_state  = "OH"
                            prop_zip    = R.get("ZIP","") or R.get("ZIP_CODE","")
                        mail_raw = R.get("MAILING_AD","")
                        mail_street,mail_city,mail_state,mail_zip = _parse_mail_addr(mail_raw)
                        if not prop_street and not owner_raw: continue
                        rec = {"parcel_id":parid,"owner":owner_raw.title(),
                               "prop_address":prop_street,"prop_city":prop_city or "Toledo",
                               "prop_state":prop_state or "OH","prop_zip":prop_zip,
                               "mail_address":mail_street,"mail_city":mail_city,
                               "mail_state":mail_state or "OH","mail_zip":mail_zip,"luc":luc}
                        if prop_street:
                            ak=norm_addr_key(prop_street)
                            if ak: parcels[ak]=rec
                        if owner_raw:
                            for v in name_variants(owner_raw):
                                if f"OWNER:{v}" not in parcels: parcels[f"OWNER:{v}"]=rec
                        count+=1
                    except: continue
            logging.info("CSV: %s parcels loaded | %s addr keys | %s owner keys",
                         count,
                         sum(1 for k in parcels if not k.startswith("OWNER:")),
                         sum(1 for k in parcels if k.startswith("OWNER:")))
        except Exception as e: logging.error("CSV error: %s",e)

    if not parcels:
        logging.warning("No parcel data loaded — using per-record auditor API fallback")
    return parcels

def match_parcel(owner: str, prop_address: str, parcels: Dict[str,dict]) -> tuple:
    if not parcels: return None,"unmatched"
    if prop_address and is_valid_address(prop_address):
        k=norm_addr_key(prop_address)
        if k and k in parcels: return parcels[k],"address_exact"
    if owner:
        for v in name_variants(norm_name(owner)):
            k=f"OWNER:{v}"
            if k in parcels: return parcels[k],"name_exact"
        ts=" ".join(sorted(norm_name(owner).split()))
        if f"OWNER:{ts}" in parcels: return parcels[f"OWNER:{ts}"],"name_token_sorted"
        if not likely_corp(owner):
            for part in [p for p in re.split(r"[\s,]+",norm_name(owner)) if len(p)>2]:
                if f"OWNER:{part}" in parcels: return parcels[f"OWNER:{part}"],"name_lastname"
    return None,"unmatched"

def enrich(record: "LeadRecord", parcels: Dict[str,dict]) -> "LeadRecord":
    matched,method = match_parcel(record.owner, record.prop_address, parcels)
    if matched is None and record.doc_type=="PRO" and record.decedent_name:
        matched,method = match_parcel(record.decedent_name,"",parcels)
        if matched: method=f"probate_{method}"
    if matched is None and record.owner and not is_boilerplate(record.owner):
        au = auditor_lookup(record.owner)
        if au: matched=au; method="auditor_api"

    if matched:
        if not is_valid_address(record.prop_address):
            record.prop_address=matched.get("prop_address","")
        if not record.prop_city:   record.prop_city  =matched.get("prop_city","") or "Toledo"
        if not record.prop_zip:    record.prop_zip   =matched.get("prop_zip","")
        if not record.mail_address:record.mail_address=matched.get("mail_address","")
        if not record.mail_city:   record.mail_city  =matched.get("mail_city","")
        if not record.mail_state:  record.mail_state =matched.get("mail_state","OH")
        if not record.mail_zip:    record.mail_zip   =matched.get("mail_zip","")
        if not record.parcel_id:   record.parcel_id  =matched.get("parcel_id","")
        if not record.luc:         record.luc        =matched.get("luc","")
        if not record.assessed_value:  record.assessed_value  =matched.get("assessed_value")
        if not record.estimated_value: record.estimated_value =matched.get("est_market_value")
        record.match_method=method
        record.match_score={"address_exact":1.0,"name_exact":0.92,
                            "name_token_sorted":0.85,"name_lastname":0.65,
                            "auditor_api":0.80}.get(method.replace("probate_",""),0.75)

    if not record.prop_city:  record.prop_city ="Toledo"
    if not record.prop_state: record.prop_state="OH"
    if record.prop_address and not is_valid_address(record.prop_address):
        record.prop_address=""

    record.with_address    =1 if is_valid_address(record.prop_address) else 0
    record.is_absentee     =is_absentee(record.prop_address,record.mail_address,record.mail_state)
    record.is_out_of_state =is_oos(record.mail_state)

    if record.is_absentee     and "Absentee owner"     not in record.flags: record.flags.append("Absentee owner")
    if record.is_out_of_state and "Out-of-state owner" not in record.flags: record.flags.append("Out-of-state owner")

    record.flags=list(dict.fromkeys(record.flags+cat_flags(record.doc_type,record.owner)))
    record=estimate_financials(record); record.score=score_record(record)
    return record

# ── SCRAPER 1: TLN Common Pleas ───────────────────────────────────────────
async def scrape_tln_common_pleas() -> List[LeadRecord]:
    records: List[LeadRecord]=[]; seen: set=set()
    logging.info("Scraping TLN Common Pleas...")
    html=await pw_fetch(TLN_COMMON_PLEAS_URL, wait_ms=3000)
    if not html: return records
    soup=BeautifulSoup(html,"lxml")
    save_debug("tln_cp_index.html",html[:5000])

    article_links=[]
    for a in soup.select("a[href]"):
        href=clean(a.get("href",""))
        if not href or is_noise(href): continue
        if href.startswith("mailto:") or "wa.me" in href or "facebook.com" in href: continue
        if "article_" in href or "filings-received" in href:
            full=href if href.startswith("http") else urljoin(TLN_BASE,href)
            if "toledolegalnews.com" in full and full not in article_links:
                article_links.append(full)
    for days_back in range(0,15):
        d=(datetime.now()-timedelta(days=days_back)).strftime("%B-%-d-%Y").lower()
        url=f"{TLN_BASE}/courts/common_pleas/common-pleas-filings-received-on-{d}/"
        if url not in article_links: article_links.append(url)
    logging.info("TLN CP: %s URLs", len(article_links))

    for url in article_links[:20]:
        try:
            art_html=await pw_fetch(url,wait_ms=2000)
            if not art_html or len(art_html)<500: continue
            text=BeautifulSoup(art_html,"lxml").get_text(" ")
            if "404" in text[:300] or "not found" in text[:300].lower(): continue

            # Foreclosure with address
            fc_pat=re.compile(
                r"(CI[0-9]{4}[0-9]+)\s+(.{5,80}?)\s+vs\.?\s+(.{5,80}?)\.\s+"
                r".*?(?:foreclosure of mtg on|property located at|premises known as|real estate located at)\s+"
                r"([0-9]{1,5}\s+[A-Za-z][A-Za-z0-9\s\.]{3,35}),"
                r"\s*([A-Za-z\s]+),\s*Ohio\s*([0-9]{5})?",
                re.IGNORECASE|re.DOTALL
            )
            for m in fc_pat.finditer(text):
                doc_num=clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                defendant=clean(m.group(3)).title()
                if is_boilerplate(defendant): defendant=""
                prop_address=clean(m.group(4)).title()
                prop_city=clean(m.group(5)).title()
                prop_zip=clean(m.group(6)) if m.group(6) else ""
                amt_m=re.search(r"\$\s*([\d,]+(?:\.\d{2})?)",text[m.start():m.start()+300])
                filed=try_parse_date(text[max(0,m.start()-200):m.start()+50]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                records.append(LeadRecord(
                    doc_num=doc_num,doc_type="NOFC",filed=filed,
                    cat="NOFC",cat_label="Pre-foreclosure",
                    owner=defendant,grantee=clean(m.group(2)),
                    amount=parse_amount(amt_m.group(1)) if amt_m else None,
                    prop_address=prop_address,prop_city=prop_city,
                    prop_state="OH",prop_zip=prop_zip,clerk_url=url,
                    flags=["Pre-foreclosure","Lis pendens"],
                    distress_sources=["foreclosure","lis_pendens"],
                ))

            # Divorce
            dr_pat=re.compile(
                r"(DR[0-9]{4}[0-9\-]+|DM[0-9]+)[;,\s]+([A-Z][A-Za-z\s,\.]{3,40}?)\s+vs\.?\s+([A-Z][A-Za-z\s,\.]{3,40}?)(?:[;,\.]|\s{2}|$)",
                re.IGNORECASE
            )
            for m in dr_pat.finditer(text):
                doc_num=clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                plaintiff=clean(m.group(2)).title()
                if is_boilerplate(plaintiff): continue
                filed=try_parse_date(text[max(0,m.start()-100):m.start()+50]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                records.append(LeadRecord(
                    doc_num=doc_num,doc_type="DIVORCE",filed=filed,
                    cat="DIVORCE",cat_label="Divorce Filing",
                    owner=plaintiff,grantee=clean(m.group(3)).title(),
                    clerk_url=url,flags=["Divorce filing"],
                    distress_sources=["divorce"],
                ))

            # Lien vs
            ln_pat=re.compile(
                r"(LN[0-9]{4}[0-9\-]+)[;,\s]+([^;,\n]{3,80}?)\s+vs\.?\s+([^;,\n\.]{3,60}?)(?:[;,\.]|\s{2}|$)",
                re.IGNORECASE
            )
            for m in ln_pat.finditer(text):
                doc_num=clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                owner=clean(m.group(3)).title()
                if not owner or len(owner)<3 or is_boilerplate(owner): continue
                filed=try_parse_date(text[max(0,m.start()-100):m.start()+50]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                dt=infer_doc_type(clean(m.group(2))) or "LNFED"
                amt_m=re.search(r"\$\s*([\d,]+(?:\.\d{2})?)",text[m.start():m.start()+200])
                records.append(LeadRecord(
                    doc_num=doc_num,doc_type=dt,filed=filed,
                    cat=dt,cat_label=LEAD_TYPE_MAP.get(dt,dt),
                    owner=owner,grantee=clean(m.group(2)),
                    amount=parse_amount(amt_m.group(1)) if amt_m else None,
                    clerk_url=url,flags=cat_flags(dt,owner),
                    distress_sources=[s for s in [classify_distress(dt)] if s],
                ))

            # Semicolon lien list
            sc_pat=re.compile(
                r"(LN[0-9]{4}[0-9\-]+)[;, ]+\$?([\d,\.]+)[;, ]+([^;\n]{3,60})[;, ]+([^;\n]{3,60})[;, ]+([^;\n]{3,80})",
                re.IGNORECASE
            )
            for m in sc_pat.finditer(text):
                doc_num=clean(m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)
                try: amt=float(m.group(2).replace(",",""))
                except: amt=None
                if amt and amt>10_000_000: continue
                owner=clean(m.group(4)).title(); addr_raw=clean(m.group(5))
                if not owner or len(owner)<3 or is_boilerplate(owner): continue
                dt=infer_doc_type(clean(m.group(3))) or "LN"
                filed=try_parse_date(text[max(0,m.start()-200):m.start()+100]) or datetime.now().date().isoformat()
                if not is_recent(filed): continue
                addr_m=re.search(r"(\d{2,5}\s+[A-Z][A-Za-z\s\.]{3,30}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL|WAY|TER|CIR)\.?)",addr_raw,re.IGNORECASE)
                city_m=re.search(r"(TOLEDO|MAUMEE|SYLVANIA|OREGON|PERRYSBURG|WATERVILLE|WHITEHOUSE|HOLLAND|SWANTON)",addr_raw,re.IGNORECASE)
                zip_m=re.search(r"(43\d{3})",addr_raw)
                records.append(LeadRecord(
                    doc_num=doc_num,doc_type=dt,filed=filed,
                    cat=dt,cat_label=LEAD_TYPE_MAP.get(dt,dt),
                    owner=owner,grantee=clean(m.group(3)),amount=amt,
                    prop_address=clean(addr_m.group(1)).title() if addr_m else "",
                    prop_city=clean(city_m.group(0)).title() if city_m else "Toledo",
                    prop_state="OH",prop_zip=zip_m.group(1) if zip_m else "",
                    clerk_url=url,flags=cat_flags(dt,owner),
                    distress_sources=[s for s in [classify_distress(dt)] if s],
                ))
            await asyncio.sleep(1.5+random.uniform(0,1))
        except Exception as e:
            logging.warning("TLN CP %s: %s",url[-50:],e)
    logging.info("TLN Common Pleas: %s records",len(records))
    return records

# ── SCRAPER 2: TLN Foreclosure Notices ────────────────────────────────────
async def scrape_tln_foreclosure_notices() -> List[LeadRecord]:
    records: List[LeadRecord]=[]; seen: set=set()
    logging.info("Scraping TLN Foreclosure Notices...")
    ADDRESS_PAT=re.compile(
        r'(\d{1,5}\s+[A-Za-z][A-Za-z0-9\s\.,#\-]{3,50}),\s+'
        r'(?:Toledo|Maumee|Perrysburg|Sylvania|Oregon|Waterville|Whitehouse|Holland|Swanton),?\s+'
        r'OH\s+(\d{5})',re.IGNORECASE
    )
    OWNER_PATS=[
        re.compile(r'(?:defendant|owner|mortgagor)[s]?[:\s]+([A-Z][A-Za-z][A-Za-z\s,\.]{2,50}?)(?:,|\.|whose|last known|and\s+Jane|and\s+John)',re.IGNORECASE),
        re.compile(r'vs\.?\s+([A-Z][A-Za-z][A-Za-z\s,\.]{2,40}?)(?:\s+whose|\s+last|\s+an\s+individual|,|\.|$)',re.IGNORECASE),
    ]
    html=await pw_fetch(TLN_FORECLOSURES_URL,wait_ms=3000)
    if not html: return records
    soup=BeautifulSoup(html,"lxml")
    case_links=[]
    for a in soup.select("a[href]"):
        href=clean(a.get("href","")); text=clean(a.get_text())
        if not href or is_noise(text) or is_noise(href): continue
        if "/legal_notices/foreclosures/" in href and "article_" in href:
            full=href if href.startswith("http") else urljoin(TLN_BASE,href)
            if full not in case_links: case_links.append((text,full))
    logging.info("TLN Foreclosure: %s links",len(case_links))
    for link_text,url in case_links[:60]:
        try:
            case_m=re.search(r"(CI[0-9]{4}[0-9\-]+|TF[0-9]+)",url+" "+link_text,re.IGNORECASE)
            doc_num=clean(case_m.group(1)).upper() if case_m else f"FC-{len(records)+1:04d}"
            if doc_num in seen: continue
            seen.add(doc_num)
            if is_noise(link_text): continue
            art_html=await pw_fetch(url,wait_ms=2000)
            if not art_html or len(art_html)<300: continue
            text=BeautifulSoup(art_html,"lxml").get_text(" ")
            addr_m=ADDRESS_PAT.search(text)
            prop_address=clean(addr_m.group(1)).title() if addr_m else ""
            prop_zip=addr_m.group(2) if addr_m else ""
            city_m=re.search(r"(Toledo|Maumee|Perrysburg|Sylvania|Oregon|Waterville|Whitehouse|Holland|Swanton)",text,re.IGNORECASE)
            prop_city=clean(city_m.group(0)).title() if city_m else "Toledo"
            owner=""
            for pat in OWNER_PATS:
                om=pat.search(text)
                if om:
                    cand=clean(om.group(1)).title()
                    cand=re.sub(r"\s+(?:Case|CI|No\.?)\s+.*$","",cand,flags=re.IGNORECASE).strip()
                    cand=re.sub(r"\s{2,}.*$","",cand).strip()
                    if not is_boilerplate(cand) and len(cand)>=4: owner=cand; break
            if not owner:
                lt=re.sub(r"case\s+no\.?\s+CI[\d\-]+","",link_text,flags=re.IGNORECASE).strip()
                if lt and not is_boilerplate(lt) and len(lt)>=4: owner=lt.title()
            amt_m=re.search(r'\$([\d,]+(?:\.\d{2})?)',text)
            is_tax="tax" in url.lower() or "tax" in link_text.lower()
            dt="TAX" if is_tax else "NOFC"
            filed=try_parse_date(text[:500]) or datetime.now().date().isoformat()
            records.append(LeadRecord(
                doc_num=doc_num,doc_type=dt,filed=filed,
                cat=dt,cat_label=LEAD_TYPE_MAP.get(dt,dt),
                owner=owner,amount=parse_amount(amt_m.group(1)) if amt_m else None,
                prop_address=prop_address,prop_city=prop_city,
                prop_state="OH",prop_zip=prop_zip,clerk_url=url,
                flags=["Pre-foreclosure"]+(["Tax lien"] if is_tax else []),
                distress_sources=["foreclosure"]+(["tax_delinquent"] if is_tax else []),
            ))
            await asyncio.sleep(1+random.uniform(0,0.5))
        except Exception as e:
            logging.warning("TLN FC %s: %s",url[-50:],e)
    logging.info("TLN Foreclosure Notices: %s records",len(records))
    return records

# ── SCRAPER 3: Sheriff Sales ───────────────────────────────────────────────
async def scrape_sheriff_sales() -> List[LeadRecord]:
    """
    RealForeclose platform — owner data is in the individual case detail pages.
    Each case links to a detail page that has the defendant name in static HTML.
    We also cross-reference with the LC CPC case search for owner names.
    """
    records: List[LeadRecord]=[]; seen=set()
    logging.info("Scraping sheriff sales...")

    for days_ahead in range(0,45):
        sale_date=(datetime.now()+timedelta(days=days_ahead)).strftime("%m/%d/%Y")
        url=f"{SHERIFF_BASE}/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE={quote(sale_date)}"
        try:
            html=await pw_fetch(url,wait_ms=5000)  # extra wait for JS
            if not html or len(html)<500: continue
            soup=BeautifulSoup(html,"lxml")
            text=soup.get_text(" ")
            if "no auction" in text.lower() or "no properties" in text.lower(): continue
            save_debug("sheriff_page.html",html[:5000])

            # Try to get individual case detail links
            for a in soup.select("a[href]"):
                href=clean(a.get("href",""))
                if "zaction=AUCTION" in href and ("VIEW" in href.upper() or "DETAIL" in href.upper()):
                    full=href if href.startswith("http") else f"{SHERIFF_BASE}{href}"
                    case_m=re.search(r"CASENUM=([^&]+)|zcase=([^&]+)",href,re.IGNORECASE)
                    if case_m:
                        doc_num=clean(case_m.group(1) or case_m.group(2))
                        if doc_num not in seen:
                            # Queue for detail fetch
                            pass

            for row in soup.select("tr,.AUCTION_ITEM,.property-item"):
                item_text=clean(row.get_text(" "))
                if len(item_text)<20: continue
                case_m=re.search(r"(CI[0-9]{4}[0-9\-]+|TF[0-9]+|[0-9]{4}CV[0-9]+)",item_text,re.IGNORECASE)
                if not case_m: continue
                doc_num=clean(case_m.group(1))
                if doc_num in seen: continue
                seen.add(doc_num)

                addr_m=re.search(
                    r"(\d{2,5}\s+[A-Z][A-Za-z\s\.]{3,35}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL|WAY|TER|CIR|PKWY)\.?)",
                    item_text,re.IGNORECASE
                )
                prop_address=clean(addr_m.group(1)).title() if addr_m else ""
                if not is_valid_address(prop_address): prop_address=""

                amt_m=re.search(r"(?:Appraised|Value|Bid)[:\s]*\$?([\d,]+)",item_text,re.IGNORECASE)
                if not amt_m: amt_m=re.search(r"\$([\d,]+(?:\.\d{2})?)",item_text)
                amt=parse_amount(amt_m.group(1)) if amt_m else None

                city_m=re.search(r"(Toledo|Maumee|Sylvania|Oregon|Perrysburg|Waterville|Whitehouse|Holland)",item_text,re.IGNORECASE)
                zip_m=re.search(r"(43\d{3})",item_text)

                # Try to get owner from case table cell
                owner=""
                owner_m=re.search(
                    r"(?:defendant|titled|owner)[:\s]+([A-Z][A-Za-z\s]{3,40}?)(?:,|\.|$|\n)",
                    item_text,re.IGNORECASE
                )
                if owner_m and not is_boilerplate(owner_m.group(1)):
                    owner=clean(owner_m.group(1)).title()

                # If still no owner and we have a case number, try CPC lookup
                # (async-safe: do it synchronously since it's a quick GET)
                if not owner and doc_num:
                    owner=lookup_sheriff_owner(doc_num)

                try: filed=datetime.strptime(sale_date,"%m/%d/%Y").date().isoformat()
                except: filed=datetime.now().date().isoformat()

                records.append(LeadRecord(
                    doc_num=doc_num,doc_type="SHERIFF",filed=filed,
                    cat="SHERIFF",cat_label="Sheriff Sale",
                    owner=owner,amount=amt,appraised_value=amt,
                    prop_address=prop_address,
                    prop_city=clean(city_m.group(0)).title() if city_m else "Toledo",
                    prop_state="OH",prop_zip=zip_m.group(1) if zip_m else "",
                    sheriff_sale_date=sale_date,clerk_url=url,
                    flags=["Sheriff sale scheduled","Pre-foreclosure","Hot Stack"],
                    distress_sources=["sheriff_sale","foreclosure"],
                    distress_count=2,hot_stack=True,
                    with_address=1 if is_valid_address(prop_address) else 0,
                ))
            await asyncio.sleep(1.5)
        except Exception as e:
            logging.warning("Sheriff %s: %s",sale_date,e)

    logging.info("Sheriff sales: %s",len(records))
    return records

# ── SCRAPER 4: TLN Probate ────────────────────────────────────────────────
async def scrape_tln_probate() -> List[LeadRecord]:
    records: List[LeadRecord]=[]
    logging.info("Scraping probate...")
    try:
        html=await pw_fetch(TLN_PROBATE_URL,wait_ms=3000)
        if not html: return records
        soup=BeautifulSoup(html,"lxml"); all_text=soup.get_text(" ")
        links=[]
        for a in soup.select("a[href]"):
            href=clean(a.get("href",""))
            if not href or href.startswith("mailto:") or "wa.me" in href or "facebook.com" in href: continue
            if "article_" in href or "probate" in href:
                full=href if href.startswith("http") else urljoin(TLN_BASE,href)
                if "toledolegalnews.com" in full: links.append(full)
        for link in links[:10]:
            try:
                ah=await pw_fetch(link,wait_ms=2000)
                if ah: all_text+=" "+BeautifulSoup(ah,"lxml").get_text(" ")
                await asyncio.sleep(1)
            except: pass
        estate_pat=re.compile(
            r"(?:Estate\s+of|In\s+re\s+Estate\s+of)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})"
            r"(?:,\s*(?:deceased|Deceased|DECEASED))?",re.IGNORECASE
        )
        seen=set()
        for m in estate_pat.finditer(all_text):
            name=clean(m.group(1))
            if name in seen or len(name)<5: continue
            seen.add(name)
            surrounding=all_text[max(0,m.start()-50):m.end()+400]
            filed=try_parse_date(surrounding) or datetime.now().date().isoformat()
            if not is_recent(filed): continue
            exec_m=re.search(r"(?:executor|administrator)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)",surrounding,re.IGNORECASE)
            executor=clean(exec_m.group(1)) if exec_m else ""
            records.append(LeadRecord(
                doc_num=f"PRO-{name.replace(' ','-')}-{len(records)+1}",
                doc_type="PRO",filed=filed,cat="PRO",cat_label="Probate / Estate",
                owner=name.title(),decedent_name=name.title(),
                executor_name=executor.title(),is_inherited=True,
                flags=["Probate / estate","Inherited property"],
                distress_sources=["probate"],distress_count=1,
                clerk_url=TLN_PROBATE_URL,match_method="probate_name",
            ))
    except Exception as e:
        logging.warning("Probate failed: %s",e)
    logging.info("Probate: %s",len(records))
    return records

# ── SCRAPER 5: Divorces ────────────────────────────────────────────────────
async def scrape_divorces() -> List[LeadRecord]:
    records: List[LeadRecord]=[]; seen: set=set()
    logging.info("Scraping divorces...")
    try:
        html=await pw_fetch(TLN_DOMESTIC_URL,wait_ms=3000)
        if html:
            soup=BeautifulSoup(html,"lxml"); texts=[soup.get_text(" ")]
            links=[]
            for a in soup.select("a[href]"):
                href=clean(a.get("href",""))
                if not href or href.startswith("mailto:") or "wa.me" in href: continue
                if "article_" in href or "domestic" in href or "filings" in href:
                    full=href if href.startswith("http") else urljoin(TLN_BASE,href)
                    if "toledolegalnews.com" in full: links.append(full)
            for link in links[:8]:
                try:
                    ah=await pw_fetch(link,wait_ms=1500)
                    if ah: texts.append(BeautifulSoup(ah,"lxml").get_text(" "))
                    await asyncio.sleep(1)
                except: pass
            dr_pat=re.compile(
                r"(DR[0-9]{4}[0-9\-]+|DM[0-9]+)[;,\s]+([A-Z][A-Za-z\s,\.]{3,40}?)\s+vs\.?\s+([A-Z][A-Za-z\s,\.]{3,40}?)(?:[;,\.]|\s{2}|$)",
                re.IGNORECASE
            )
            for text in texts:
                for m in dr_pat.finditer(text):
                    doc_num=clean(m.group(1))
                    if doc_num in seen: continue
                    seen.add(doc_num)
                    plaintiff=clean(m.group(2)).title()
                    if is_boilerplate(plaintiff): continue
                    filed=try_parse_date(text[max(0,m.start()-100):m.start()+50]) or datetime.now().date().isoformat()
                    if not is_recent(filed): continue
                    records.append(LeadRecord(
                        doc_num=doc_num,doc_type="DIVORCE",filed=filed,
                        cat="DIVORCE",cat_label="Divorce Filing",
                        owner=plaintiff,grantee=clean(m.group(3)).title(),
                        clerk_url=TLN_DOMESTIC_URL,
                        flags=["Divorce filing"],distress_sources=["divorce"],
                    ))
    except Exception as e:
        logging.warning("Divorce TLN: %s",e)

    # CPC public search
    try:
        for days_back in range(0,14):
            d=(datetime.now()-timedelta(days=days_back)).strftime("%m/%d/%Y")
            url=f"http://lcapps.co.lucas.oh.us/CPC/CaseSearch.aspx?court=DR&fromdate={d}&todate={d}"
            r=requests.get(url,headers=HEADERS,timeout=10)
            if r.status_code!=200: continue
            text=BeautifulSoup(r.text,"lxml").get_text(" ")
            dr_pat2=re.compile(
                r"(DR\s*\d{4}\s*\d{4,}|DM\s*\d{4}\s*\d{4,})\s+([A-Z][A-Za-z\s,]{3,40}?)\s+vs\.?\s+([A-Z][A-Za-z\s,]{3,40}?)(?:\s|$|,|\.)",
                re.IGNORECASE
            )
            for m in dr_pat2.finditer(text):
                doc_num=re.sub(r"\s+","",clean(m.group(1)))
                if doc_num in seen: continue
                seen.add(doc_num)
                plaintiff=clean(m.group(2)).title()
                if is_boilerplate(plaintiff): continue
                records.append(LeadRecord(
                    doc_num=doc_num,doc_type="DIVORCE",
                    filed=datetime.now().date().isoformat(),
                    cat="DIVORCE",cat_label="Divorce Filing",
                    owner=plaintiff,grantee=clean(m.group(3)).title(),
                    clerk_url=url,flags=["Divorce filing"],
                    distress_sources=["divorce"],
                ))
            await asyncio.sleep(0.5)
    except Exception as e:
        logging.debug("CPC divorce: %s",e)

    logging.info("Divorces: %s",len(records))
    return records

# ── SCRAPER 6: Tax Delinquent ──────────────────────────────────────────────
async def scrape_tax_delinquent() -> List[LeadRecord]:
    records: List[LeadRecord]=[]; seen=set()
    logging.info("Scraping tax delinquent...")
    try:
        html=await pw_fetch(TLN_FORECLOSURES_URL,wait_ms=3000)
        if html:
            text=BeautifulSoup(html,"lxml").get_text(" ")
            tf_pat=re.compile(
                r"(TF[0-9]{4}[0-9\-]+|TF\s*[0-9]{6,})\s+(.{10,80}?)\s+\$?([\d,]+(?:\.\d{2})?)?",
                re.IGNORECASE
            )
            for m in tf_pat.finditer(text):
                doc_num=re.sub(r"\s+","",clean(m.group(1)))
                if doc_num in seen: continue
                seen.add(doc_num)
                raw=clean(m.group(2))
                try: amt=float(m.group(3).replace(",","")) if m.group(3) else None
                except: amt=None
                addr_m=re.search(r"(\d{2,5}\s+[A-Za-z][A-Za-z\s\.]{3,25}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL)\.?)",raw,re.IGNORECASE)
                prop_address=clean(addr_m.group(1)).title() if addr_m else ""
                records.append(LeadRecord(
                    doc_num=doc_num,doc_type="TAX",
                    filed=datetime.now().date().isoformat(),
                    cat="TAX",cat_label="Tax Delinquent",
                    amount=amt,prop_address=prop_address,
                    prop_city="Toledo",prop_state="OH",
                    clerk_url=TLN_FORECLOSURES_URL,
                    flags=["Tax delinquent","Tax lien"],
                    distress_sources=["tax_delinquent"],
                ))
    except Exception as e:
        logging.warning("Tax delin: %s",e)
    logging.info("Tax delinquent: %s",len(records))
    return records

# ── Cross-stack + dedupe ───────────────────────────────────────────────────
def cross_stack(records: List[LeadRecord]) -> List[LeadRecord]:
    addr_map: Dict[str,List[int]]=defaultdict(list)
    for i,r in enumerate(records):
        if is_valid_address(r.prop_address):
            k=norm_addr_key(r.prop_address)
            if k: addr_map[k].append(i)
    stacked=0
    for key,idxs in addr_map.items():
        if len(idxs)<2: continue
        all_sources: set=set()
        for i in idxs: all_sources.update(records[i].distress_sources or [])
        if len(all_sources)<2: continue
        for i in idxs:
            r=records[i]
            r.distress_sources=list(set(list(r.distress_sources or [])+list(all_sources)))
            r.distress_count=len(r.distress_sources); r.hot_stack=True
            if "Hot Stack"        not in " ".join(r.flags): r.flags.append("Hot Stack")
            if "Cross-List Match" not in " ".join(r.flags): r.flags.append("Cross-List Match")
            r=estimate_financials(r); r.score=score_record(r); records[i]=r
        stacked+=1
    logging.info("Cross-stacked %s property groups",stacked)
    return records

def dedupe(records: List[LeadRecord]) -> List[LeadRecord]:
    final,seen=[],set()
    for r in records:
        nd=re.sub(r"^(PCF1|PCF2)-","",clean(r.doc_num).upper())
        key=(nd,clean(r.doc_type).upper(),clean(r.owner)[:20].upper(),clean(r.filed))
        if key in seen: continue
        seen.add(key); final.append(r)
    return final

# ── Output ─────────────────────────────────────────────────────────────────
def split_name(n: str):
    parts=clean(n).split()
    if not parts: return "",""
    return parts[0]," ".join(parts[1:]) if len(parts)>1 else ""

def write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(payload,indent=2),encoding="utf-8")

def build_payload(records: List[LeadRecord]) -> dict:
    return {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       SOURCE_NAME,
        "date_range": {
            "from":(datetime.now()-timedelta(days=LOOKBACK_DAYS)).date().isoformat(),
            "to":  datetime.now().date().isoformat(),
        },
        "total":                len(records),
        "with_address":         sum(1 for r in records if is_valid_address(r.prop_address)),
        "hot_stack_count":      sum(1 for r in records if r.hot_stack),
        "sheriff_sale_count":   sum(1 for r in records if r.doc_type=="SHERIFF"),
        "probate_count":        sum(1 for r in records if r.doc_type=="PRO"),
        "inherited_count":      sum(1 for r in records if r.is_inherited),
        "tax_delinquent_count": sum(1 for r in records if r.doc_type=="TAX"),
        "foreclosure_count":    sum(1 for r in records if r.doc_type in {"NOFC","LP"}),
        "lien_count":           sum(1 for r in records if r.doc_type in {"LN","LNMECH","LNFED","LNIRS","LNCORPTX"}),
        "absentee_count":       sum(1 for r in records if r.is_absentee),
        "out_of_state_count":   sum(1 for r in records if r.is_out_of_state),
        "subject_to_count":     sum(1 for r in records if r.subject_to_score>=50),
        "divorce_count":        sum(1 for r in records if r.doc_type=="DIVORCE"),
        "redfin_enriched_count":sum(1 for r in records if r.redfin_value),
        "zillow_enriched_count":sum(1 for r in records if r.zillow_value),
        "redfin_calls":         REDFIN_CALLS,
        "zillow_api_calls":     ZILLOW_CALLS,
        "parcel_matched_count": sum(1 for r in records if r.match_method!="unmatched"),
        "records":              [asdict(r) for r in records],
    }

def write_category_json(records: List[LeadRecord]):
    categories={
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
        "code_violations": [r for r in records if r.doc_type=="CODEVIOLATION"],
        "vacant_homes":    [r for r in records if r.is_vacant_home],
        "vacant_land":     [r for r in records if r.is_vacant_land],
        "evictions":       [r for r in records if r.doc_type=="EVICTION"],
    }
    descs={
        "hot_stack":"2+ distress signals","sheriff_sales":"Sheriff auction",
        "probate":"Estate / probate","inherited":"Inherited properties",
        "tax_delinquent":"Tax delinquent","foreclosure":"Active foreclosures",
        "pre_foreclosure":"Pre-foreclosure & lis pendens","liens":"Liens",
        "absentee":"Absentee owners","out_of_state":"Out-of-state owners",
        "subject_to":"Subject-To candidates","divorces":"Divorce filings",
        "code_violations":"Code violations","vacant_homes":"Vacant homes",
        "vacant_land":"Vacant land","evictions":"Eviction filings",
    }
    for cat,recs in categories.items():
        recs_s=sorted(recs,key=lambda r:(r.hot_stack,r.distress_count,r.subject_to_score,r.score),reverse=True)
        payload={"fetched_at":datetime.now(timezone.utc).isoformat(),"source":SOURCE_NAME,
                 "category":cat,"description":descs.get(cat,""),"total":len(recs_s),
                 "records":[asdict(r) for r in recs_s]}
        for path in [DATA_DIR/f"{cat}.json",DASHBOARD_DIR/f"{cat}.json"]:
            write_json(path,payload)
        logging.info("Wrote %s: %s records",cat,len(recs_s))

def write_csv(records: List[LeadRecord], csv_path: Path):
    csv_path.parent.mkdir(parents=True,exist_ok=True)
    fieldnames=[
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Subject-To Score","Motivated Seller Flags","Distress Sources","Distress Count",
        "Hot Stack","Absentee Owner","Out-of-State Owner","Inherited",
        "Assessed Value","Est Market Value","Redfin AVM","Zillow Value","Value Source",
        "Beds","Baths","Sq Ft","Last Sale Price","Last Sale Year",
        "Est Equity","Est Arrears","Est Payoff","Mortgage Signals",
        "Parcel ID","LUC Code","Match Method","Match Score","Source","Public Records URL",
    ]
    with csv_path.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fieldnames)
        w.writeheader()
        for r in records:
            fn,ln=split_name(r.owner)
            w.writerow({
                "First Name":fn,"Last Name":ln,
                "Mailing Address":r.mail_address,"Mailing City":r.mail_city,
                "Mailing State":r.mail_state,"Mailing Zip":r.mail_zip,
                "Property Address":r.prop_address,"Property City":r.prop_city,
                "Property State":r.prop_state,"Property Zip":r.prop_zip,
                "Lead Type":r.cat_label,"Document Type":r.doc_type,
                "Date Filed":r.filed,"Document Number":r.doc_num,
                "Amount/Debt Owed":f"${r.amount:,.2f}" if r.amount else "",
                "Seller Score":r.score,"Subject-To Score":r.subject_to_score,
                "Motivated Seller Flags":"; ".join(r.flags),
                "Distress Sources":"; ".join(r.distress_sources),
                "Distress Count":r.distress_count,
                "Hot Stack":"YES" if r.hot_stack else "",
                "Absentee Owner":"YES" if r.is_absentee else "",
                "Out-of-State Owner":"YES" if r.is_out_of_state else "",
                "Inherited":"YES" if r.is_inherited else "",
                "Assessed Value":f"${r.assessed_value:,.0f}" if r.assessed_value else "",
                "Est Market Value":f"${r.estimated_value:,.0f}" if r.estimated_value else "",
                "Redfin AVM":f"${r.redfin_value:,.0f}" if r.redfin_value else "",
                "Zillow Value":f"${r.zillow_value:,.0f}" if r.zillow_value else "",
                "Value Source":r.value_source,
                "Beds":r.beds or "","Baths":r.baths or "","Sq Ft":r.sqft or "",
                "Last Sale Price":f"${r.last_sale_price:,.0f}" if r.last_sale_price else "",
                "Last Sale Year":r.last_sale_year or "",
                "Est Equity":f"${r.est_equity:,.0f}" if r.est_equity is not None else "",
                "Est Arrears":f"${r.est_arrears:,.0f}" if r.est_arrears else "",
                "Est Payoff":f"${r.est_payoff:,.0f}" if r.est_payoff else "",
                "Mortgage Signals":"; ".join(r.mortgage_signals),
                "Parcel ID":r.parcel_id,"LUC Code":r.luc,
                "Match Method":r.match_method,"Match Score":f"{r.match_score:.2f}",
                "Source":SOURCE_NAME,"Public Records URL":r.clerk_url,
            })
    logging.info("Wrote CSV: %s (%s rows)",csv_path,len(records))

# ── Main ───────────────────────────────────────────────────────────────────
async def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--out-csv",     default=str(DEFAULT_ENRICHED_CSV_PATH))
    ap.add_argument("--dbf-address", default="")
    args=ap.parse_args()

    global DBF_PARCELS_ADDRESS
    if args.dbf_address: DBF_PARCELS_ADDRESS=Path(args.dbf_address)

    ensure_dirs(); log_setup()
    logging.info("=== Toledo / Lucas County Motivated Seller Intelligence v3 ===")
    logging.info("Zillow   : %s","ENABLED" if ZILLOW_API_KEY else "DISABLED")
    logging.info("Redfin   : ENABLED (free)")
    logging.info("DBF      : %s (exists=%s)",DBF_PARCELS_ADDRESS,DBF_PARCELS_ADDRESS.exists())
    logging.info("CSV      : %s (exists=%s)",CSV_PARCELS_ADDRESS,CSV_PARCELS_ADDRESS.exists())

    parcels=load_parcel_data()

    cp_recs,fc_recs,sh_recs,pr_recs,dv_recs,tx_recs = await asyncio.gather(
        scrape_tln_common_pleas(),
        scrape_tln_foreclosure_notices(),
        scrape_sheriff_sales(),
        scrape_tln_probate(),
        scrape_divorces(),
        scrape_tax_delinquent(),
    )
    all_records=cp_recs+fc_recs+sh_recs+pr_recs+dv_recs+tx_recs
    logging.info("Total before enrich: %s",len(all_records))

    enriched=[]
    for r in all_records:
        try: enriched.append(enrich(r,parcels))
        except Exception as e:
            logging.warning("Enrich %s: %s",r.doc_num,e)
            enriched.append(r)
    all_records=enriched

    all_records=cross_stack(all_records)
    all_records=dedupe(all_records)
    all_records.sort(
        key=lambda r:(r.doc_type=="SHERIFF",r.hot_stack,r.distress_count,
                      r.subject_to_score,r.score,r.filed),
        reverse=True
    )

    parcel_matched=sum(1 for r in all_records if r.match_method!="unmatched")
    logging.info("Final: %s | Matched: %s | Redfin: %s | Zillow: %s/%s",
                 len(all_records),parcel_matched,REDFIN_CALLS,ZILLOW_CALLS,ZILLOW_MAX_CALLS)

    payload=build_payload(all_records)
    for path in DEFAULT_OUTPUT_JSON_PATHS:
        write_json(path,payload)
        logging.info("Wrote %s (%s records)",path,len(all_records))
    write_category_json(all_records)
    write_csv(all_records,DEFAULT_OUTPUT_CSV_PATH)
    if Path(args.out_csv)!=DEFAULT_OUTPUT_CSV_PATH:
        write_csv(all_records,Path(args.out_csv))

    logging.info(
        "=== DONE === Total:%s | Sheriff:%s | HotStack:%s | Probate:%s | "
        "PreFC:%s | Liens:%s | Tax:%s | Absentee:%s | OOS:%s | "
        "SubTo:%s | Inherited:%s | Divorce:%s | Redfin:%s | Zillow:%s | Matched:%s",
        len(all_records),
        sum(1 for r in all_records if r.doc_type=="SHERIFF"),
        sum(1 for r in all_records if r.hot_stack),
        sum(1 for r in all_records if r.doc_type=="PRO"),
        sum(1 for r in all_records if r.doc_type in {"NOFC","LP"}),
        sum(1 for r in all_records if r.doc_type in {"LN","LNMECH","LNFED"}),
        sum(1 for r in all_records if r.doc_type=="TAX"),
        sum(1 for r in all_records if r.is_absentee),
        sum(1 for r in all_records if r.is_out_of_state),
        sum(1 for r in all_records if r.subject_to_score>=50),
        sum(1 for r in all_records if r.is_inherited),
        sum(1 for r in all_records if r.doc_type=="DIVORCE"),
        sum(1 for r in all_records if r.redfin_value),
        sum(1 for r in all_records if r.zillow_value),
        parcel_matched,
    )

if __name__=="__main__":
    asyncio.run(main())
