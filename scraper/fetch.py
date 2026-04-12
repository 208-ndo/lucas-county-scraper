"""
Toledo / Lucas County — Motivated Seller Intelligence
"""
from __future__ import annotations
import asyncio, json, logging, re, time, csv
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

LOOKBACK_DAYS   = 90
SOURCE_NAME     = "Lucas County / Toledo"
DATA_DIR        = Path("data")
DASHBOARD_DIR   = Path("dashboard")

TLN_BASE        = "https://www.toledolegalnews.com"
TLN_CP          = f"{TLN_BASE}/courts/common_pleas/"
TLN_PROBATE     = f"{TLN_BASE}/courts/probate/"
TLN_FORECLOSURE = f"{TLN_BASE}/legal_notices/foreclosures/"
SHERIFF_CAL     = "https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=USER&zmethod=CALENDAR"
TREASURER_URL   = "https://www.lucascountytreasurer.org/delinquent-taxes"
AREIS_BASE      = "https://icare.co.lucas.oh.us/lucascare"
AREIS_OWNER_URL = f"{AREIS_BASE}/search/commonsearch.aspx?mode=owner"
AREIS_ADDR_URL  = f"{AREIS_BASE}/search/commonsearch.aspx?mode=address"

HOT_STACK_MIN_SOURCES = 2

VACANT_LUCS = {
    "400","401","402","403","404","405","406","407","408","409",
    "500","501","502","503","504","505","506","510","511","512",
    "550","551","552","553","554","555",
    "700","701","702","703","704","705",
    "800","801","802","803","880","881",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

@dataclass
class LeadRecord:
    owner:            str = ""
    prop_address:     str = ""
    prop_city:        str = ""
    prop_zip:         str = ""
    mail_address:     str = ""
    mail_city:        str = ""
    mail_state:       str = ""
    mail_zip:         str = ""
    doc_type:         str = ""
    filed:            str = ""
    amount:           Optional[float] = None
    doc_num:          str = ""
    clerk_url:        str = ""
    parcel_id:        str = ""
    luc:              str = ""
    acres:            str = ""
    est_market_value: Optional[float] = None
    assessed_value:   Optional[float] = None
    last_sale_price:  Optional[float] = None
    score:            int = 0
    flags:            list = field(default_factory=list)
    distress_sources: list = field(default_factory=list)
    distress_count:   int = 0
    hot_stack:        bool = False
    is_absentee:      bool = False
    is_out_of_state:  bool = False
    is_vacant_land:   bool = False
    is_inherited:     bool = False
    phone:            str = ""
    tags:             list = field(default_factory=list)

def clean(s) -> str:
    if s is None: return ""
    return " ".join(str(s).split()).strip()

def normalize_state(s: str) -> str:
    s = clean(s).upper()
    if len(s) == 2 and s.isalpha(): return s
    m = {"OHIO":"OH","MICHIGAN":"MI","INDIANA":"IN","FLORIDA":"FL","TEXAS":"TX"}
    return m.get(s, "")

def parse_amount(s: str) -> Optional[float]:
    if not s: return None
    s = re.sub(r"[^\d.]", "", s)
    try: return float(s) if s else None
    except: return None

def parse_date(s: str) -> str:
    if not s: return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
        try: return datetime.strptime(clean(s), fmt).strftime("%Y-%m-%d")
        except: pass
    return clean(s)

def is_within_lookback(date_str: str) -> bool:
    if not date_str: return True
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return d >= datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    except: return True

def ensure_dirs():
    DATA_DIR.mkdir(exist_ok=True)
    DASHBOARD_DIR.mkdir(exist_ok=True)

def log_setup():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def write_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")

def score_record(r: LeadRecord) -> int:
    s = 0
    dt = r.doc_type.lower()
    if "sheriff" in dt:                                s += 50
    if "pre-foreclosure" in dt or "lis pendens" in dt: s += 40
    if "foreclosure" in dt:                            s += 40
    if "tax" in dt:                                    s += 35
    if "probate" in dt:                                s += 30
    if "lien" in dt:                                   s += 20
    if r.is_absentee:                                  s += 15
    if r.is_out_of_state:                              s += 20
    if r.is_vacant_land:                               s += 10
    if r.is_inherited:                                 s += 25
    if r.distress_count >= 2:                          s += 20
    if r.amount and r.amount > 0:                      s += 5
    return min(s, 100)

def parse_street(addr: str):
    addr = clean(addr)
    m = re.match(r"^(\d+[A-Za-z]?)\s+(.*)", addr)
    if m: return m.group(1), m.group(2).strip()
    return "", addr

async def pw_fetch(url: str, timeout: int = 30000) -> str:
    if not HAS_PLAYWRIGHT:
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            return r.text
        except Exception as e:
            logging.warning("requests fetch failed %s: %s", url[:80], e)
            return ""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            content = await page.content()
            await browser.close()
            logging.info("pw_fetch %d chars from %s", len(content), url[:80])
            return content
    except Exception as e:
        logging.warning("pw_fetch failed %s: %s", url[:80], e)
        return ""

_areis_session: Optional[requests.Session] = None

def get_areis_session() -> requests.Session:
    global _areis_session
    if _areis_session is None:
        _areis_session = requests.Session()
        _areis_session.headers.update(HEADERS)
        try:
            _areis_session.get(f"{AREIS_BASE}/main/homepage.aspx", timeout=15)
        except: pass
    return _areis_session

def check_areis_available() -> bool:
    try:
        sess = get_areis_session()
        r = sess.get(f"{AREIS_BASE}/main/homepage.aspx", timeout=10)
        if "maintenance" in r.text.lower() or len(r.text) < 200:
            logging.warning("AREIS in maintenance — skipping enrichment")
            return False
        logging.info("AREIS available")
        return True
    except Exception as e:
        logging.warning("AREIS not reachable: %s", e)
        return False

def is_absentee(prop_addr: str, mail_addr: str, mail_city: str, mail_state: str) -> bool:
    if not mail_addr: return False
    mu = clean(mail_addr).upper()
    pu = clean(prop_addr).upper()
    if mu.startswith("PO BOX") or mu.startswith("P O BOX"): return True
    if mail_state and mail_state not in ("OH", "", "0", "3"): return True
    local = {"TOLEDO","MAUMEE","SYLVANIA","OREGON","PERRYSBURG","WATERVILLE"}
    if mail_city and mail_city.upper() not in local and mail_city.upper() != "": return True
    m1 = re.match(r"^(\d+)\s", pu)
    m2 = re.match(r"^(\d+)\s", mu)
    if m1 and m2 and m1.group(1) != m2.group(1): return True
    return False

def enrich_with_areis(records: list) -> list:
    logging.info("Starting AREIS enrichment for %d records...", len(records))
    if not check_areis_available():
        logging.warning("AREIS down — keeping all records as-is")
        return records

    sess = get_areis_session()
    cache: dict = {}
    enriched = absentee_ct = oos_ct = vacant_ct = 0

    for i, rec in enumerate(records):
        if i % 100 == 0 and i > 0:
            logging.info("AREIS enrichment: %d/%d | absentee=%d oos=%d vacant=%d",
                         i, len(records), absentee_ct, oos_ct, vacant_ct)

        prop_addr    = clean(rec.prop_address)
        owner        = clean(rec.owner)
        stno, stname = parse_street(prop_addr)

        if stno and len(stname) > 2:
            cache_key = f"addr|{stno}|{stname[:20].upper()}"
        elif owner and len(owner) > 3:
            cache_key = f"own|{owner[:30].upper()}"
        else:
            continue

        if cache_key not in cache:
            parcel_id = ""
            detail    = {}

            if stno and len(stname) > 2:
                try:
                    vs = {}
                    r0 = sess.get(AREIS_ADDR_URL, timeout=15)
                    soup0 = BeautifulSoup(r0.text, "lxml")
                    for inp in soup0.find_all("input", {"type": "hidden"}):
                        if inp.get("name"): vs[inp["name"]] = inp.get("value","")
                    post = {"__EVENTTARGET":"","__EVENTARGUMENT":"","hdMode":"address",
                            "stno":stno,"stdir":"","stname":stname,"stsuf":"","unit":"",
                            "searchType":"address"}
                    post.update(vs)
                    r1 = sess.post(AREIS_ADDR_URL, data=post, timeout=20)
                    soup1 = BeautifulSoup(r1.text, "lxml")
                    for row in soup1.select("table tr"):
                        cells = row.find_all(["td","th"])
                        texts = [clean(c.get_text()) for c in cells]
                        for t in texts:
                            if re.match(r"^\d{2}-\d{6}-\d{3}-\d{3}$", t):
                                parcel_id = t; break
                        if parcel_id: break
                    time.sleep(0.3)
                except Exception as e:
                    logging.debug("AREIS addr search error: %s", e)

            if parcel_id:
                try:
                    det_url = f"{AREIS_BASE}/search/commonsearch.aspx?mode=detail&parid={parcel_id}"
                    rd = sess.get(det_url, timeout=20)
                    soup_d = BeautifulSoup(rd.text, "lxml")
                    lmap = {
                        "owner name":"owner_name","owner":"owner_name",
                        "mailing address":"mail_address","mail address":"mail_address",
                        "mail city":"mail_city","city":"mail_city",
                        "mail state":"mail_state","state":"mail_state",
                        "mail zip":"mail_zip","zip":"mail_zip",
                        "property address":"prop_address","location address":"prop_address",
                        "land use":"luc","acreage":"acres",
                    }
                    detail = {"owner_name":"","mail_address":"","mail_city":"",
                              "mail_state":"OH","mail_zip":"","prop_address":"",
                              "prop_city":"Toledo","luc":"","acres":"",
                              "est_market_value":None,"assessed_value":None}
                    for row in soup_d.find_all("tr"):
                        cells = row.find_all(["td","th"])
                        if len(cells) >= 2:
                            label = clean(cells[0].get_text()).lower().rstrip(":").strip()
                            value = clean(cells[1].get_text())
                            if label in lmap and value:
                                detail[lmap[label]] = value
                    text = soup_d.get_text(" ")
                    for pat, key in [
                        (r"(?:Est\.?\s*Market\s*Value|Market Value)[:\s]+\$?([\d,]+)", "est_market_value"),
                        (r"(?:Total\s*Assessed|Assessed\s*Value)[:\s]+\$?([\d,]+)", "assessed_value"),
                    ]:
                        m = re.search(pat, text, re.IGNORECASE)
                        if m:
                            try:
                                v = float(m.group(1).replace(",",""))
                                if v > 0: detail[key] = v
                            except: pass
                    if detail.get("assessed_value") and not detail.get("est_market_value"):
                        detail["est_market_value"] = round(detail["assessed_value"] / 0.35)
                    detail["mail_state"] = normalize_state(detail.get("mail_state","")) or "OH"
                    detail["parcel_id"] = parcel_id
                    time.sleep(0.3)
                except Exception as e:
                    logging.debug("AREIS detail error: %s", e)

            cache[cache_key] = detail

        detail = cache.get(cache_key, {})
        if not detail: continue

        if detail.get("owner_name") and not rec.owner:  rec.owner = detail["owner_name"]
        if detail.get("prop_address") and not rec.prop_address: rec.prop_address = detail["prop_address"]
        if detail.get("prop_city"):    rec.prop_city    = detail["prop_city"]
        if detail.get("mail_address"):
            rec.mail_address = detail["mail_address"]
            rec.mail_city    = detail.get("mail_city","")
            rec.mail_state   = detail.get("mail_state","OH")
            rec.mail_zip     = detail.get("mail_zip","")
        if detail.get("luc"):              rec.luc              = detail["luc"]
        if detail.get("acres"):            rec.acres            = detail["acres"]
        if detail.get("est_market_value"): rec.est_market_value = detail["est_market_value"]
        if detail.get("assessed_value"):   rec.assessed_value   = detail["assessed_value"]
        if detail.get("parcel_id"):        rec.parcel_id        = detail["parcel_id"]
        enriched += 1

        if is_absentee(prop_addr, rec.mail_address, rec.mail_city, rec.mail_state):
            rec.is_absentee = True
            if "Absentee owner" not in rec.flags: rec.flags.append("Absentee owner")
            absentee_ct += 1

        if rec.mail_state and rec.mail_state not in ("OH","","0","3"):
            rec.is_out_of_state = True
            if "Out of state owner" not in rec.flags: rec.flags.append("Out of state owner")
            oos_ct += 1

        luc_code = rec.luc.split("-")[0].strip() if rec.luc else ""
        if luc_code in VACANT_LUCS or (luc_code.isdigit() and luc_code[:1] in ("4","5","7","8")):
            rec.is_vacant_land = True
            vacant_ct += 1

    logging.info("AREIS enrichment done: %d/%d enriched | absentee=%d oos=%d vacant=%d",
                 enriched, len(records), absentee_ct, oos_ct, vacant_ct)
    return records

async def scrape_tln_common_pleas() -> list:
    logging.info("Scraping TLN Common Pleas...")
    records = []

    # Reject fake addresses (social share text, case listings, etc.)
    BAD_ADDR = re.compile(
        r"facebook|twitter|whatsapp|email|print|copy|save|"
        r"vs\s+[A-Z]|jaffee|christ|west$|direct$|shelter$|fifth\s+third|"
        r"^\d{5}\s+\w", re.I)

    try:
        html = await pw_fetch(TLN_CP)
        soup = BeautifulSoup(html, "lxml")
        urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "common_pleas" in href and ("filing" in href or "received" in href):
                full = href if href.startswith("http") else TLN_BASE + href
                if full not in urls: urls.append(full)
        logging.info("TLN CP: %d URLs to scrape", len(urls))

        for url in urls[:80]:
            try:
                page_html = await pw_fetch(url)
                if len(page_html) < 300: continue
                page_soup = BeautifulSoup(page_html, "lxml")

                # Get article/body content only
                article = page_soup.find("div", class_=re.compile(r"article|content|body", re.I))
                if not article:
                    article = page_soup.find("article") or page_soup.find("main") or page_soup
                text = article.get_text(" ")

                # Split on case entries — each line or paragraph is a case
                lines = [l.strip() for l in text.split("\n") if l.strip()]

                for line in lines:
                    line = clean(line)
                    if len(line) < 15: continue
                    # Must have a case number to be valid
                    case_m = re.search(r"(CI\s*\d{4}[-\s]?\d{4,6})", line, re.I)
                    if not case_m: continue
                    doc_num = re.sub(r"[-\s]", "", clean(case_m.group(1))).upper()

                    # Amount
                    amt_m = re.search(r"\$\s*([\d,]+\.?\d{0,2})", line)
                    amount = parse_amount(amt_m.group(1)) if amt_m else None

                    # Date
                    date_m = re.search(r"(\d{1,2}/\d{1,2}/202[3-9])", line)
                    filed = parse_date(date_m.group(1)) if date_m else ""

                    # Property address — only accept real street addresses
                    addr_m = re.search(
                        r"(\d{2,5}\s+[A-Z][A-Za-z\s]+(?:Blvd|Ave|St|Dr|Rd|Ln|Pl|Ct|Way|Terr|Pkwy)\.?)"
                        r"\s*,?\s*(Toledo|Maumee|Sylvania|Oregon|Perrysburg)",
                        line, re.I)
                    prop_address = ""
                    prop_city = "Toledo"
                    if addr_m:
                        prop_address = clean(addr_m.group(1))
                        prop_city = clean(addr_m.group(2)).title()
                        if BAD_ADDR.search(prop_address):
                            prop_address = ""

                    # Doc type
                    doc_type = "Lien"
                    line_l = line.lower()
                    if "judgment" in line_l: doc_type = "Judgment"
                    if "foreclosure" in line_l or "lis pendens" in line_l: doc_type = "Pre-foreclosure"
                    if "lien" in line_l: doc_type = "Lien"

                    rec = LeadRecord(
                        prop_address=prop_address,
                        prop_city=prop_city,
                        doc_type=doc_type,
                        filed=filed,
                        amount=amount,
                        doc_num=doc_num,
                        clerk_url=url,
                        distress_sources=[doc_type],
                        distress_count=1,
                    )
                    if is_within_lookback(filed):
                        records.append(rec)
            except Exception as e:
                logging.debug("TLN CP page error %s: %s", url[:60], e)
    except Exception as e:
        logging.warning("TLN Common Pleas failed: %s", e)
    logging.info("TLN Common Pleas: %d records", len(records))
    return records

async def scrape_sheriff_sales() -> list:
    logging.info("Scraping sheriff sales...")
    records = []

    # Lucas County uses realauction.com — try their JSON API directly
    REALAUCTION_URLS = [
        "https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=AUCTION&zmethod=PREVIEW&AUCTIONDATE=",
        "https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=AUCTION&zmethod=GET_AUCTIONS&StartDate=01/01/2025&EndDate=12/31/2026",
        "https://lucas.sheriffsaleauction.ohio.gov/index.cfm?zaction=USER&zmethod=CALENDAR",
    ]

    try:
        # Try the calendar page which lists upcoming sales
        html = await pw_fetch(REALAUCTION_URLS[2])
        soup = BeautifulSoup(html, "lxml")

        # Find any auction date links
        auction_urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "AUCTION" in href.upper() or "auction" in href.lower() or "sale" in href.lower():
                full = href if href.startswith("http") else "https://lucas.sheriffsaleauction.ohio.gov" + href
                if full not in auction_urls:
                    auction_urls.append(full)

        logging.info("Sheriff auction URLs found: %d", len(auction_urls))

        # Also try scraping individual auction listing pages
        for url in auction_urls[:10]:
            try:
                page_html = await pw_fetch(url)
                if len(page_html) < 300: continue
                page_soup = BeautifulSoup(page_html, "lxml")
                page_text = page_soup.get_text(" ")

                # Find all property addresses on auction page
                addr_matches = re.findall(
                    r"(\d{2,5}\s+[A-Z][A-Za-z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way)\.?)"
                    r"\s*,?\s*(Toledo|Maumee|Sylvania|Oregon|Perrysburg|Lucas)",
                    page_text, re.I)

                sale_date_m = re.search(r"(\d{1,2}/\d{1,2}/202[3-9])", page_text)
                sale_date = parse_date(sale_date_m.group(1)) if sale_date_m else ""

                for addr, city in addr_matches:
                    amt_m = re.search(r"\$\s*([\d,]+)", page_text)
                    case_m = re.search(r"(CI[-\s]?\d{4}[-\s]\d{4,6}|TF\d{6,})", page_text, re.I)
                    rec = LeadRecord(
                        prop_address=clean(addr),
                        prop_city=clean(city).title(),
                        doc_type="Sheriff Sale",
                        filed=sale_date,
                        amount=parse_amount(amt_m.group(1)) if amt_m else None,
                        doc_num=clean(case_m.group(1)).replace(" ","").upper() if case_m else "",
                        clerk_url=url,
                        score=100,
                        flags=["Sheriff sale scheduled", "Pre-foreclosure", "Hot Stack", "New this week"],
                        hot_stack=True,
                        distress_sources=["Sheriff Sale"],
                        distress_count=1,
                    )
                    records.append(rec)
            except Exception as e:
                logging.debug("Sheriff page error: %s", e)

    except Exception as e:
        logging.warning("Sheriff sales failed: %s", e)

    # Fallback: scrape the Ohio Attorney General sheriff sale list for Lucas County
    if not records:
        try:
            logging.info("Trying OhioAG sheriff sale fallback...")
            AG_URL = "https://www.ohioattorneygeneral.gov/Business/Services-for-Business/Charitable-Law/Search-Charitable-Organizations"
            # Actually scrape common pleas for sheriff sale filings
            CP_SHERIFF_URL = f"{TLN_BASE}/courts/common_pleas/"
            html2 = await pw_fetch(CP_SHERIFF_URL)
            soup2 = BeautifulSoup(html2, "lxml")
            sheriff_urls = []
            for a in soup2.find_all("a", href=True):
                href = a["href"]
                if ("sheriff" in href.lower() or "tf20" in href.lower()):
                    full = href if href.startswith("http") else TLN_BASE + href
                    if full not in sheriff_urls: sheriff_urls.append(full)
            logging.info("TLN sheriff URLs: %d", len(sheriff_urls))
            for url in sheriff_urls[:20]:
                try:
                    ph = await pw_fetch(url)
                    if len(ph) < 500: continue
                    ps = BeautifulSoup(ph, "lxml")
                    text = ps.get_text(" ")
                    addr_m = re.search(
                        r"(\d{2,5}\s+[A-Z][A-Za-z\s]+(?:Blvd|Ave|St|Dr|Rd|Ln|Pl|Ct)\.?)"
                        r"\s*,?\s*(Toledo|Maumee|Sylvania|Oregon)", text, re.I)
                    case_m = re.search(r"(TF\s*\d{4}[-\s]?\d{4,6}|CI\s*\d{4}[-\s]?\d{4,6})", text, re.I)
                    amt_m = re.search(r"\$\s*([\d,]+\.?\d*)", text)
                    date_m = re.search(r"(\d{1,2}/\d{1,2}/202[3-9])", text)
                    if not addr_m and not case_m: continue
                    records.append(LeadRecord(
                        prop_address=clean(addr_m.group(1)) if addr_m else "",
                        prop_city=clean(addr_m.group(2)).title() if addr_m else "Toledo",
                        doc_type="Sheriff Sale",
                        filed=parse_date(date_m.group(1)) if date_m else "",
                        amount=parse_amount(amt_m.group(1)) if amt_m else None,
                        doc_num=clean(case_m.group(1)).replace(" ","").upper() if case_m else "",
                        clerk_url=url, score=100,
                        flags=["Sheriff sale scheduled","Pre-foreclosure","Hot Stack"],
                        hot_stack=True,
                        distress_sources=["Sheriff Sale"],
                        distress_count=1,
                    ))
                except Exception as e:
                    logging.debug("Sheriff TLN page error: %s", e)
        except Exception as e:
            logging.warning("Sheriff fallback failed: %s", e)

    logging.info("Sheriff sales: %d", len(records))
    return records

async def scrape_probate() -> list:
    logging.info("Scraping TLN Probate...")
    records = []
    try:
        html = await pw_fetch(TLN_PROBATE)
        soup = BeautifulSoup(html, "lxml")
        urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "probate" in href and ("filing" in href or "court-filing" in href or "received" in href):
                full = href if href.startswith("http") else TLN_BASE + href
                if full not in urls: urls.append(full)

        logging.info("Probate URLs found: %d", len(urls))

        for url in urls[:10]:
            try:
                page_html = await pw_fetch(url)
                if len(page_html) < 500: continue
                page_soup = BeautifulSoup(page_html, "lxml")
                text = page_soup.get_text(" ")

                # Only match Estate of / deceased — NOT guardianship, trusteeship, name changes
                for m in re.finditer(
                    r"(?:Estate\s+of|In\s+Re\s+(?:Estate\s+of)?|In\s+the\s+Matter\s+of\s+(?:the\s+)?Estate\s+of)\s+"
                    r"([A-Z][a-zA-Z]+(?:\s+[A-Za-z]+){1,4})"
                    r"(?:\s*,?\s*deceased|\s*\.\s*Case|\s*,\s*[A-Z]|\s+\w+\s+No\.?)",
                    text, re.I):

                    name = clean(m.group(1))
                    # Skip if it looks like a guardianship, trusteeship, or name change leaked through
                    if re.search(r"guardian|trustee|name\s+change|minor|ward", name, re.I):
                        continue
                    if len(name) < 4 or len(name) > 50:
                        continue
                    # Skip single-word names (probably truncated)
                    if len(name.split()) < 2:
                        continue

                    case_m = re.search(r"(20\d{2}\s*[A-Z]{2,3}\s*\d{4,})", text)
                    records.append(LeadRecord(
                        owner=name,
                        doc_type="Probate",
                        clerk_url=url,
                        flags=["Probate", "Inherited"],
                        is_inherited=True,
                        doc_num=clean(case_m.group(1)) if case_m else "",
                        distress_sources=["Probate"],
                        distress_count=1,
                    ))
            except Exception as e:
                logging.debug("Probate page error: %s", e)
    except Exception as e:
        logging.warning("Probate scrape failed: %s", e)
    logging.info("Probate: %d", len(records))
    return records

async def scrape_foreclosures() -> list:
    logging.info("Scraping foreclosure notices...")
    records = []

    BAD_OWNER = re.compile(
        r"named above|be required|assert any interest|forever barred|"
        r"marshalling|proceeds of said sale|have or may have|"
        r"Judge\s+[A-Z]|Case\s+No|Plaintiff|Defendant|unknown heirs", re.I)

    try:
        html = await pw_fetch(TLN_FORECLOSURE)
        soup = BeautifulSoup(html, "lxml")
        urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "foreclosure" in href and ("case" in href or "ci20" in href.lower()):
                full = href if href.startswith("http") else TLN_BASE + href
                if full not in urls: urls.append(full)

        logging.info("Foreclosure URLs found: %d", len(urls))

        for url in urls[:55]:
            try:
                page_html = await pw_fetch(url)
                # TLN paywall = 218 chars. Skip and just record the case number from URL.
                is_paywalled = len(page_html) < 500

                # Extract case number from URL always
                case_m = re.search(r"(CI[-\s]?\d{4}[-\s]?\d{4,6}|CI\d{7,})", url, re.I)
                doc_num = re.sub(r"[-\s]", "", clean(case_m.group(1))).upper() if case_m else ""

                prop_address = ""
                prop_city = "Toledo"
                owner = ""
                amount = None
                filed = ""

                if not is_paywalled:
                    page_soup = BeautifulSoup(page_html, "lxml")
                    # Try article body first
                    article = (page_soup.find("div", class_=re.compile(r"article|content|body|notice", re.I))
                               or page_soup.find("article") or page_soup.find("main") or page_soup)
                    text = article.get_text(" ")

                    # Property address patterns
                    for pat in [
                        r"(\d{2,5}\s+[A-Z][A-Za-z0-9\s]+(?:Street|Avenue|Drive|Road|Lane|Boulevard|Place|Court|Way|Blvd|Ave|St|Dr|Rd|Ln|Pl|Ct)\.?)\s*,?\s*(Toledo|Maumee|Sylvania|Oregon|Perrysburg)",
                        r"(\d{2,5}\s+[A-Z][A-Za-z0-9\s]+(?:Blvd|Ave|St|Dr|Rd|Ln|Pl|Ct|Way)\.?)\s*,?\s*(?:Ohio|OH|4\d{4})",
                        r"(?:property(?:\s+is)?(?:\s+located)?\s+at|premises(?:\s+known\s+as)?)[,:\s]+(\d{2,5}\s+[A-Za-z0-9\s]+(?:Blvd|Ave|St|Dr|Rd|Ln|Pl|Ct)\.?)",
                    ]:
                        m = re.search(pat, text, re.I)
                        if m:
                            candidate = clean(m.group(1))
                            # Reject courthouse address
                            if not re.match(r"^(247\s+Gradolph|700\s+Adams)", candidate, re.I):
                                prop_address = candidate
                                if len(m.groups()) > 1 and m.group(2):
                                    prop_city = clean(m.group(2)).title()
                                break

                    # Owner
                    for pat in [
                        r"vs\.?\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,3})\s*[,\n]",
                        r"Defendant[s]?[,:\s]+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,3})",
                    ]:
                        m = re.search(pat, text)
                        if m:
                            c = clean(m.group(1))
                            if not BAD_OWNER.search(c) and 4 < len(c) < 60:
                                owner = c; break

                    amt_m = re.search(r"\$\s*([\d,]+\.?\d{0,2})", text)
                    amount = parse_amount(amt_m.group(1)) if amt_m else None
                    date_m = re.search(r"(\d{1,2}/\d{1,2}/202[3-9])", text)
                    filed = parse_date(date_m.group(1)) if date_m else ""

                if not doc_num:
                    continue

                records.append(LeadRecord(
                    owner=owner,
                    prop_address=prop_address,
                    prop_city=prop_city,
                    doc_type="Foreclosure",
                    filed=filed,
                    amount=amount,
                    doc_num=doc_num,
                    clerk_url=url,
                    flags=["Pre-foreclosure", "Lis pendens"],
                    distress_sources=["Foreclosure"],
                    distress_count=1,
                ))
            except Exception as e:
                logging.debug("Foreclosure page error %s: %s", url[-50:], e)
    except Exception as e:
        logging.warning("Foreclosure scrape failed: %s", e)
    logging.info("Foreclosures: %d", len(records))
    return records

async def scrape_tax_delinquent() -> list:
    logging.info("Scraping tax delinquent...")
    records = []
    try:
        page_html = await pw_fetch(TREASURER_URL)
        page_soup = BeautifulSoup(page_html, "lxml")
        page_text = page_soup.get_text(" ")
        addr_matches = re.findall(
            r"(\d{3,5}\s+[A-Z][A-Za-z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd)\.?)"
            r"\s*(?:Toledo|Maumee|Lucas)", page_text, re.I)
        for addr in addr_matches[:50]:
            records.append(LeadRecord(
                prop_address=clean(addr), prop_city="Toledo",
                doc_type="Tax Delinquent",
                flags=["Tax delinquent"], clerk_url=TREASURER_URL,
            ))
    except Exception as e:
        logging.warning("Tax delinquent scrape failed: %s", e)
    logging.info("Tax delinquent: %d", len(records))
    return records

def apply_distress_stacking(records: list) -> list:
    addr_index: dict = {}
    for rec in records:
        key = re.sub(r"\s+", "", clean(rec.prop_address).upper())
        if key and len(key) > 4:
            addr_index.setdefault(key, []).append(rec)
    for key, group in addr_index.items():
        if len(group) < 2: continue
        sources = list({r.doc_type for r in group})
        for rec in group:
            for s in sources:
                if s not in rec.distress_sources: rec.distress_sources.append(s)
            rec.distress_count = len(rec.distress_sources)
            if rec.distress_count >= HOT_STACK_MIN_SOURCES:
                rec.hot_stack = True
                if "Hot Stack" not in rec.flags: rec.flags.append("Hot Stack")
    return records

def dedupe(records: list) -> list:
    seen_doc  = set()
    seen_addr = set()
    out       = []
    for rec in records:
        doc  = re.sub(r"\s+", "", clean(rec.doc_num).upper())
        addr = re.sub(r"\s+", "", clean(rec.prop_address).upper())
        dtype = re.sub(r"\s+", "", clean(rec.doc_type).upper())
        if doc and len(doc) > 3:
            if doc in seen_doc: continue
            seen_doc.add(doc)
            out.append(rec)
        elif addr and len(addr) > 5:
            key = f"{addr}|{dtype}"
            if key in seen_addr: continue
            seen_addr.add(key)
            out.append(rec)
        else:
            out.append(rec)
    return out

def tag_new_this_week(records: list) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    for rec in records:
        if rec.filed and rec.filed >= cutoff:
            if "New this week" not in rec.flags:
                rec.flags.append("New this week")
    return records

def write_outputs(records: list, sheriff: list, probate: list,
                  tax_delinquent: list, foreclosures: list):

    def to_dict(r):
        d = asdict(r)
        d["score"] = score_record(r)
        return d

    all_dicts = [to_dict(r) for r in records]
    hot       = [d for d in all_dicts if d.get("hot_stack")]
    s_dicts   = [to_dict(r) for r in sheriff]
    pr_dicts  = [to_dict(r) for r in probate]
    td_dicts  = [to_dict(r) for r in tax_delinquent]
    fc_dicts  = [to_dict(r) for r in foreclosures]
    absentee  = [d for d in all_dicts if d.get("is_absentee")]
    oos       = [d for d in all_dicts if d.get("is_out_of_state")]
    vacant    = [d for d in all_dicts if d.get("is_vacant_land")]
    inherited = [d for d in all_dicts if d.get("is_inherited")]
    liens     = [d for d in all_dicts if "lien" in d.get("doc_type","").lower()]
    divorces  = [d for d in all_dicts if "divorce" in d.get("doc_type","").lower()]

    # records.json wrapped in metadata so dashboard can read it
    meta = {
        "total":                len(all_dicts),
        "fetched_at":           datetime.now(timezone.utc).isoformat(),
        "hot_stack_count":      len(hot),
        "sheriff_sale_count":   len(s_dicts),
        "probate_count":        len(pr_dicts),
        "tax_delinquent_count": len(td_dicts),
        "foreclosure_count":    len(fc_dicts),
        "absentee_count":       len(absentee),
        "out_of_state_count":   len(oos),
        "vacant_land_count":    len(vacant),
        "inherited_count":      len(inherited),
        "liens_count":          len(liens),
        "code_violation_count": 0,
        "vacant_home_count":    0,
        "subject_to_count":     0,
        "records":              all_dicts,
    }
    for p in [DATA_DIR / "records.json", DASHBOARD_DIR / "records.json"]:
        write_json(p, meta)
    logging.info("Wrote records.json: %d records", len(all_dicts))

    def wrap(data, label=""):
        return {"total": len(data), "records": data, "fetched_at": datetime.now(timezone.utc).isoformat()}

    for fname, data in [
        ("hot_stack.json",        hot),
        ("sheriff_sales.json",    s_dicts),
        ("probate.json",          pr_dicts),
        ("tax_delinquent.json",   td_dicts),
        ("foreclosure.json",      fc_dicts),
        ("absentee.json",         absentee),
        ("out_of_state.json",     oos),
        ("inherited.json",        inherited),
        ("vacant_land.json",      vacant),
        ("liens.json",            liens),
        ("divorces.json",         divorces),
        ("subject_to.json",       []),
        ("bankruptcy.json",       []),
        ("code_violations.json",  []),
        ("vacant_homes.json",     []),
        ("evictions.json",        []),
        ("prime_subject_to.json", []),
    ]:
        for p in [DATA_DIR / fname, DASHBOARD_DIR / fname]:
            write_json(p, wrap(data))

    csv_path = DATA_DIR / "ghl_export.csv"
    fields = [
        "First Name","Last Name","Phone","Email",
        "Property Address","Property City","Property Zip",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Lead Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Distress Sources",
        "Distress Count","Hot Stack","Absentee Owner","Out of State",
        "Vacant Land","Inherited","Equity Est","Parcel ID","LUC","Source",
        "Public Records URL",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for d in all_dicts:
            owner = d.get("owner","") or ""
            parts = owner.split()
            emv   = d.get("est_market_value")
            w.writerow({
                "First Name":  parts[0] if parts else "",
                "Last Name":   " ".join(parts[1:]) if len(parts)>1 else "",
                "Phone": d.get("phone",""), "Email": "",
                "Property Address": d.get("prop_address",""),
                "Property City":    d.get("prop_city",""),
                "Property Zip":     d.get("prop_zip",""),
                "Mailing Address":  d.get("mail_address",""),
                "Mailing City":     d.get("mail_city",""),
                "Mailing State":    d.get("mail_state",""),
                "Mailing Zip":      d.get("mail_zip",""),
                "Lead Type":        d.get("doc_type",""),
                "Date Filed":       d.get("filed",""),
                "Document Number":  d.get("doc_num",""),
                "Amount/Debt Owed": d.get("amount",""),
                "Seller Score":     d.get("score",0),
                "Motivated Seller Flags": "; ".join(d.get("flags") or []),
                "Distress Sources": "; ".join(d.get("distress_sources") or []),
                "Distress Count":   d.get("distress_count",0),
                "Hot Stack":        "YES" if d.get("hot_stack") else "",
                "Absentee Owner":   "YES" if d.get("is_absentee") else "",
                "Out of State":     "YES" if d.get("is_out_of_state") else "",
                "Vacant Land":      "YES" if d.get("is_vacant_land") else "",
                "Inherited":        "YES" if d.get("is_inherited") else "",
                "Equity Est":       f"${emv:,.0f}" if emv else "",
                "Parcel ID":        d.get("parcel_id",""),
                "LUC":              d.get("luc",""),
                "Source":           SOURCE_NAME,
                "Public Records URL": d.get("clerk_url",""),
            })
    logging.info("Wrote CSV: %s (%d rows)", csv_path, len(all_dicts))

    enr_path = DATA_DIR / "records.enriched.csv"
    with open(enr_path, "w", newline="", encoding="utf-8") as f:
        if all_dicts:
            w = csv.DictWriter(f, fieldnames=list(all_dicts[0].keys()))
            w.writeheader()
            w.writerows(all_dicts)
    logging.info("Wrote CSV: %s (%d rows)", enr_path, len(all_dicts))

    logging.info(
        "=== DONE === Total:%d | Sheriff:%d | HotStack:%d | Probate:%d | "
        "Foreclosure:%d | TaxDelin:%d | Absentee:%d | OOS:%d | Vacant:%d | "
        "Inherited:%d | Liens:%d | Divorce:%d | BK:0 | CodeViol:0",
        len(all_dicts), len(s_dicts), len(hot), len(pr_dicts),
        len(fc_dicts), len(td_dicts), len(absentee), len(oos),
        len(vacant), len(inherited), len(liens), len(divorces),
    )

async def main():
    ensure_dirs()
    log_setup()
    logging.info("=== Toledo / Lucas County - Motivated Seller Intelligence ===")
    logging.info("Lookback: %d days | Started: %s",
                 LOOKBACK_DAYS, datetime.now(timezone.utc).isoformat())
    logging.info("Starting all scrapers...")

    cp_task = asyncio.create_task(scrape_tln_common_pleas())
    sh_task = asyncio.create_task(scrape_sheriff_sales())
    pr_task = asyncio.create_task(scrape_probate())
    fc_task = asyncio.create_task(scrape_foreclosures())
    td_task = asyncio.create_task(scrape_tax_delinquent())

    cp_records = await cp_task
    sh_records = await sh_task
    pr_records = await pr_task
    fc_records = await fc_task
    td_records = await td_task

    all_records = cp_records + sh_records + pr_records + fc_records + td_records
    logging.info("Total before enrich: %d", len(all_records))

    all_records = enrich_with_areis(all_records)

    for rec in all_records:
        rec.score = score_record(rec)

    all_records = tag_new_this_week(all_records)
    all_records = apply_distress_stacking(all_records)

    before = len(all_records)
    all_records = dedupe(all_records)
    logging.info("Total after dedupe: %d (was %d)", len(all_records), before)

    all_records.sort(key=lambda r: (r.hot_stack, r.distress_count, r.score), reverse=True)

    write_outputs(all_records, sh_records, pr_records, td_records, fc_records)

if __name__ == "__main__":
    asyncio.run(main())
