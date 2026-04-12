"""
Toledo / Lucas County — Motivated Seller Intelligence
=====================================================
Sources:
  1. Toledo Legal News — Common Pleas (lis pendens, foreclosure, liens)
  2. Toledo Legal News — Probate
  3. Toledo Legal News — Foreclosure notices
  4. Lucas County Sheriff Sale Auction
  5. Lucas County Treasurer — Tax Delinquent
  6. Lucas County AREIS — owner/address enrichment (icare.co.lucas.oh.us)

Output:
  data/records.json, data/hot_stack.json, data/sheriff_sales.json
  data/probate.json, data/tax_delinquent.json, data/foreclosure.json
  data/liens.json, data/absentee.json, data/out_of_state.json
  data/divorces.json, data/bankruptcy.json, data/code_violations.json
  data/ghl_export.csv, data/records.enriched.csv
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

# ── CONFIG ────────────────────────────────────────────────────────────────────
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

# ── DATA MODEL ────────────────────────────────────────────────────────────────
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

# ── HELPERS ───────────────────────────────────────────────────────────────────
def clean(s) -> str:
    if s is None: return ""
    return " ".join(str(s).split()).strip()

def normalize_state(s: str) -> str:
    s = clean(s).upper()
    if len(s) == 2 and s.isalpha(): return s
    m = {"OHIO":"OH","MICHIGAN":"MI","INDIANA":"IN","FLORIDA":"FL",
         "TEXAS":"TX","GEORGIA":"GA","CALIFORNIA":"CA"}
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
    if "divorce" in dt:                                s += 20
    if r.is_absentee:                                  s += 15
    if r.is_out_of_state:                              s += 20
    if r.is_vacant_land:                               s += 10
    if r.is_inherited:                                 s += 25
    if r.distress_count >= 2:                          s += 20
    if r.distress_count >= 3:                          s += 15
    if r.amount and r.amount > 0:                      s += 5
    return min(s, 100)

def parse_street(addr: str):
    addr = clean(addr)
    m = re.match(r"^(\d+[A-Za-z]?)\s+(.*)", addr)
    if m: return m.group(1), m.group(2).strip()
    return "", addr

# ── PLAYWRIGHT FETCH ──────────────────────────────────────────────────────────
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

# ── AREIS ENRICHMENT ──────────────────────────────────────────────────────────
_areis_session: Optional[requests.Session] = None

def get_areis_session() -> requests.Session:
    global _areis_session
    if _areis_session is None:
        _areis_session = requests.Session()
        _areis_session.headers.update(HEADERS)
        try:
            _areis_session.get(f"{AREIS_BASE}/main/homepage.aspx", timeout=15)
            logging.info("AREIS session initialized")
        except Exception as e:
            logging.warning("AREIS session warmup failed: %s", e)
    return _areis_session

def areis_get_viewstate(url: str) -> dict:
    """Fetch ASP.NET hidden form fields needed for POST."""
    sess = get_areis_session()
    fields = {}
    try:
        r = sess.get(url, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        for inp in soup.find_all("input", {"type": "hidden"}):
            name = inp.get("name", "")
            val  = inp.get("value", "")
            if name:
                fields[name] = val
    except Exception as e:
        logging.debug("viewstate fetch failed: %s", e)
    return fields

def areis_parse_results(html: str) -> list:
    """Parse AREIS search results table."""
    results = []
    if not html or len(html) < 300:
        return results
    soup = BeautifulSoup(html, "lxml")

    for row in soup.select("table tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        texts = [clean(c.get_text()) for c in cells]

        # Skip header rows
        if any(t.lower() in ("parcel id", "owner name", "location address",
                              "land use", "total value") for t in texts):
            continue

        # Find Lucas County parcel ID: XX-XXXXXX-XXX-XXX
        parid = ""
        for t in texts:
            if re.match(r"^\d{2}-\d{6}-\d{3}-\d{3}$", t):
                parid = t
                break
            ct = t.replace("-", "").replace(" ", "")
            if re.match(r"^\d{14,17}$", ct):
                parid = t
                break
        if not parid:
            continue

        # Get detail link
        detail_url = ""
        for a in row.find_all("a", href=True):
            href = a["href"]
            if "detail" in href.lower() or "parid" in href.lower():
                detail_url = (href if href.startswith("http")
                              else AREIS_BASE + "/" + href.lstrip("/"))
                break

        results.append({
            "parcel_id":    parid,
            "owner_name":   texts[1] if len(texts) > 1 else "",
            "prop_address": texts[2] if len(texts) > 2 else "",
            "luc":          texts[3] if len(texts) > 3 else "",
            "total_value":  texts[4] if len(texts) > 4 else "",
            "detail_url":   detail_url,
        })
    return results

def areis_search_address(stno: str, stname: str) -> list:
    """Search AREIS by property address — POST with viewstate."""
    if not stno or not stname:
        return []
    sess = get_areis_session()
    vs   = areis_get_viewstate(AREIS_ADDR_URL)
    post_data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "hdMode": "address",
        "stno": stno,
        "stdir": "",
        "stname": stname,
        "stsuf": "",
        "unit": "",
        "searchType": "address",
    }
    post_data.update(vs)
    try:
        r = sess.post(AREIS_ADDR_URL, data=post_data, timeout=20)
        results = areis_parse_results(r.text)
        logging.debug("AREIS addr '%s %s': %d results", stno, stname, len(results))
        return results
    except Exception as e:
        logging.debug("AREIS addr search failed %s %s: %s", stno, stname, e)
        return []

def areis_search_owner(last: str, first: str = "") -> list:
    """Search AREIS by owner last name — POST with viewstate."""
    if not last or len(last) < 2:
        return []
    sess = get_areis_session()
    vs   = areis_get_viewstate(AREIS_OWNER_URL)
    post_data = {
        "__EVENTTARGET": "",
        "__EVENTARGUMENT": "",
        "hdMode": "owner",
        "ownerlast": last.upper(),
        "ownerfirst": first.upper(),
        "searchType": "owner",
    }
    post_data.update(vs)
    try:
        r = sess.post(AREIS_OWNER_URL, data=post_data, timeout=20)
        results = areis_parse_results(r.text)
        logging.debug("AREIS owner '%s %s': %d results", last, first, len(results))
        return results
    except Exception as e:
        logging.debug("AREIS owner search failed %s: %s", last, e)
        return []

def areis_get_detail(parcel_id: str, detail_url: str = "") -> dict:
    """Fetch AREIS detail page for mailing address, values, land use."""
    sess = get_areis_session()
    detail = {
        "owner_name": "", "mail_address": "", "mail_city": "",
        "mail_state": "OH", "mail_zip": "", "prop_address": "",
        "prop_city": "Toledo", "luc": "", "acres": "",
        "est_market_value": None, "assessed_value": None,
    }
    if not detail_url:
        detail_url = (f"{AREIS_BASE}/search/commonsearch.aspx"
                      f"?mode=detail&parid={parcel_id}")
    try:
        r    = sess.get(detail_url, timeout=20)
        html = r.text
        if not html or len(html) < 500:
            return detail
        soup = BeautifulSoup(html, "lxml")

        lmap = {
            "owner name":       "owner_name",
            "owner":            "owner_name",
            "mailing address":  "mail_address",
            "mail address":     "mail_address",
            "address":          "mail_address",
            "mail city":        "mail_city",
            "mailing city":     "mail_city",
            "city":             "mail_city",
            "mail state":       "mail_state",
            "state":            "mail_state",
            "mail zip":         "mail_zip",
            "zip":              "mail_zip",
            "zip code":         "mail_zip",
            "property address": "prop_address",
            "location address": "prop_address",
            "location":         "prop_address",
            "property city":    "prop_city",
            "land use":         "luc",
            "land use code":    "luc",
            "class":            "luc",
            "acreage":          "acres",
            "acres":            "acres",
        }
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                label = clean(cells[0].get_text()).lower().rstrip(":").strip()
                value = clean(cells[1].get_text())
                if label in lmap and value:
                    detail[lmap[label]] = value

        # Scan element IDs for ASP.NET controls
        for el in soup.find_all(["span", "td", "div"]):
            eid = el.get("id", "").lower()
            val = clean(el.get_text())
            if not val: continue
            if "owner" in eid and not detail["owner_name"]:       detail["owner_name"]  = val
            elif "mailadr" in eid or "mailaddr" in eid:           detail["mail_address"] = val
            elif "mailcity" in eid:                               detail["mail_city"]   = val
            elif "mailstate" in eid:                              detail["mail_state"]  = val
            elif "mailzip" in eid:                                detail["mail_zip"]    = val
            elif "propadr" in eid or "locaddr" in eid:            detail["prop_address"] = val
            elif "landuse" in eid or "luc" in eid:               detail["luc"]         = val
            elif "acreage" in eid or "acres" in eid:             detail["acres"]       = val

        # Parse dollar values from page text
        text = soup.get_text(" ")
        for pat, key in [
            (r"(?:Est\.?\s*Market\s*Value|Market Value|Appraised Value)"
             r"[:\s]+\$?([\d,]+)", "est_market_value"),
            (r"(?:Total\s*Assessed|Assessed\s*Value|Total Value)"
             r"[:\s]+\$?([\d,]+)", "assessed_value"),
        ]:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    v = float(m.group(1).replace(",", ""))
                    if v > 0: detail[key] = v
                except: pass

        if detail["assessed_value"] and not detail["est_market_value"]:
            detail["est_market_value"] = round(detail["assessed_value"] / 0.35)

        detail["mail_state"] = normalize_state(detail["mail_state"]) or "OH"

    except Exception as e:
        logging.debug("AREIS detail failed %s: %s", parcel_id, e)
    return detail

def check_areis_available() -> bool:
    """Check if AREIS is up before attempting enrichment."""
    try:
        sess = get_areis_session()
        r = sess.get(f"{AREIS_BASE}/main/homepage.aspx", timeout=10)
        if "maintenance" in r.text.lower() or len(r.text) < 200:
            logging.warning("AREIS appears to be in maintenance — skipping enrichment")
            return False
        logging.info("AREIS is available (%.0f chars)", len(r.text))
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
    local = {"TOLEDO","MAUMEE","SYLVANIA","OREGON","PERRYSBURG","WATERVILLE","MONCLOVA","ROSSFORD"}
    if mail_city and mail_city.upper() not in local and mail_city.upper() != "": return True
    m1 = re.match(r"^(\d+)\s", pu)
    m2 = re.match(r"^(\d+)\s", mu)
    if m1 and m2 and m1.group(1) != m2.group(1): return True
    return False

def enrich_with_areis(records: list) -> list:
    logging.info("Starting AREIS enrichment for %d records...", len(records))

    if not check_areis_available():
        logging.warning("AREIS down — records kept as-is, no enrichment")
        return records

    cache: dict = {}
    enriched = absentee_ct = oos_ct = vacant_ct = 0
    total = len(records)

    for i, rec in enumerate(records):
        if i % 100 == 0 and i > 0:
            logging.info("AREIS enrichment: %d/%d | absentee=%d oos=%d vacant=%d",
                         i, total, absentee_ct, oos_ct, vacant_ct)

        prop_addr        = clean(rec.prop_address)
        owner            = clean(rec.owner)
        stno, stname     = parse_street(prop_addr)

        if stno and len(stname) > 2:
            cache_key = f"addr|{stno}|{stname[:20].upper()}"
        elif owner and len(owner) > 3:
            cache_key = f"own|{owner[:30].upper()}"
        else:
            continue

        if cache_key not in cache:
            parcel_id  = ""
            detail_url = ""
            detail     = {}

            if stno and len(stname) > 2:
                results = areis_search_address(stno, stname)
                if results:
                    parcel_id  = results[0]["parcel_id"]
                    detail_url = results[0].get("detail_url", "")
                time.sleep(0.3)

            if not parcel_id and owner:
                parts = owner.upper().split()
                last  = parts[0] if parts else ""
                first = parts[1] if len(parts) > 1 else ""
                results = areis_search_owner(last, first)
                for r in results:
                    if stno and stno in clean(r.get("prop_address", "")):
                        parcel_id  = r["parcel_id"]
                        detail_url = r.get("detail_url", "")
                        break
                if not parcel_id and results:
                    parcel_id  = results[0]["parcel_id"]
                    detail_url = results[0].get("detail_url", "")
                time.sleep(0.3)

            if parcel_id:
                detail = areis_get_detail(parcel_id, detail_url)
                detail["parcel_id"] = parcel_id
                time.sleep(0.3)

            cache[cache_key] = detail

        detail = cache.get(cache_key, {})
        if not detail:
            continue

        if detail.get("owner_name") and not rec.owner:  rec.owner        = detail["owner_name"]
        if detail.get("prop_address") and not rec.prop_address: rec.prop_address = detail["prop_address"]
        if detail.get("prop_city"):    rec.prop_city    = detail["prop_city"]
        if detail.get("mail_address"):
            rec.mail_address = detail["mail_address"]
            rec.mail_city    = detail.get("mail_city", "")
            rec.mail_state   = detail.get("mail_state", "OH")
            rec.mail_zip     = detail.get("mail_zip", "")
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

        if rec.mail_state and rec.mail_state not in ("OH", "", "0", "3"):
            rec.is_out_of_state = True
            if "Out of state owner" not in rec.flags: rec.flags.append("Out of state owner")
            oos_ct += 1

        luc_code = rec.luc.split("-")[0].strip() if rec.luc else ""
        if luc_code in VACANT_LUCS or (luc_code.isdigit() and luc_code[:1] in ("4","5","7","8")):
            rec.is_vacant_land = True
            vacant_ct += 1

    logging.info("AREIS enrichment done: %d/%d enriched | absentee=%d | oos=%d | vacant=%d",
                 enriched, total, absentee_ct, oos_ct, vacant_ct)
    return records

# ── SCRAPERS ──────────────────────────────────────────────────────────────────
async def scrape_tln_common_pleas() -> list:
    logging.info("Scraping TLN Common Pleas...")
    records = []
    try:
        html = await pw_fetch(TLN_CP)
        soup = BeautifulSoup(html, "lxml")
        urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "common_pleas" in href and "filing" in href:
                full = href if href.startswith("http") else TLN_BASE + href
                if full not in urls: urls.append(full)
        logging.info("TLN CP: %d URLs to scrape", len(urls))

        for url in urls[:80]:
            try:
                page_html = await pw_fetch(url)
                if len(page_html) < 300: continue
                page_soup = BeautifulSoup(page_html, "lxml")
                text      = page_soup.get_text(" ")
                entries   = re.split(r"\n{2,}", text)

                for entry in entries:
                    entry = clean(entry)
                    if not entry or len(entry) < 20: continue

                    case_m   = re.search(r"(CI\s*\d{4}[-\s]?\d{4,6})", entry, re.I)
                    doc_num  = clean(case_m.group(1)).replace(" ","") if case_m else ""
                    amt_m    = re.search(r"\$\s*([\d,]+\.?\d*)", entry)
                    amount   = parse_amount(amt_m.group(1)) if amt_m else None
                    date_m   = re.search(r"(\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})", entry)
                    filed    = parse_date(date_m.group(1)) if date_m else ""
                    owner_m  = re.search(r"^([A-Z][a-zA-Z\s,\.]+?)(?:\s{2,}|\n|,\s*Et\s*Al)", entry)
                    owner    = clean(owner_m.group(1)) if owner_m else ""
                    addr_m   = re.search(
                        r"(\d{2,5}\s+[A-Za-z][A-Za-z\s]+"
                        r"(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way|Ter|Pkwy)\.?)",
                        entry, re.I)
                    prop_address = clean(addr_m.group(1)) if addr_m else ""

                    if not doc_num and not owner: continue

                    doc_type = "Pre-foreclosure"
                    if "judgment" in entry.lower(): doc_type = "Judgment"
                    if "lien" in entry.lower():     doc_type = "Lien"

                    rec = LeadRecord(
                        owner=owner, prop_address=prop_address, prop_city="Toledo",
                        doc_type=doc_type, filed=filed, amount=amount,
                        doc_num=doc_num, clerk_url=url,
                        flags=["Lis pendens"] if "lis pendens" in entry.lower() else [],
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
    try:
        html = await pw_fetch(SHERIFF_CAL)
        soup = BeautifulSoup(html, "lxml")
        auction_urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "sheriffsaleauction" in href or "realauction" in href:
                if href not in auction_urls: auction_urls.append(href)
        logging.info("Sheriff auction URLs: %d", len(auction_urls))

        for url in auction_urls[:15]:
            try:
                page_html = await pw_fetch(url)
                page_soup = BeautifulSoup(page_html, "lxml")
                text      = page_soup.get_text(" ")
                addr_matches = re.findall(
                    r"(\d{2,5}\s+[A-Z][A-Za-z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct)\.?)"
                    r",?\s*(Toledo|Maumee|Sylvania|Oregon)", text, re.I)
                for addr, city in addr_matches:
                    sale_m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
                    case_m = re.search(r"(TF\d{4,}|CI\d{4,})", text)
                    amt_m  = re.search(r"\$\s*([\d,]+)", text)
                    rec = LeadRecord(
                        prop_address=clean(addr), prop_city=clean(city),
                        doc_type="Sheriff Sale",
                        filed=parse_date(sale_m.group(1)) if sale_m else "",
                        amount=parse_amount(amt_m.group(1)) if amt_m else None,
                        doc_num=clean(case_m.group(1)) if case_m else "",
                        clerk_url=url, score=100,
                        flags=["Sheriff sale scheduled","Pre-foreclosure","Hot Stack","New this week"],
                        hot_stack=True, distress_sources=["Sheriff Sale"], distress_count=1,
                    )
                    records.append(rec)
            except Exception as e:
                logging.debug("Sheriff page error: %s", e)
    except Exception as e:
        logging.warning("Sheriff sales failed: %s", e)

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
            if "probate" in href and ("filing" in href or "case" in href):
                full = href if href.startswith("http") else TLN_BASE + href
                if full not in urls: urls.append(full)

        for url in urls[:10]:
            try:
                page_html = await pw_fetch(url)
                page_soup = BeautifulSoup(page_html, "lxml")
                text      = page_soup.get_text(" ")
                for m in re.finditer(
                    r"(?:Estate of|In Re[:\s]+|In the Matter of)\s+"
                    r"([A-Z][a-zA-Z\s,\.]+?)(?:\.|,|\n|deceased)", text, re.I):
                    name = clean(m.group(1))
                    if len(name) < 4: continue
                    case_m = re.search(r"(20\d{2}\s*[A-Z]{2,3}\s*\d{4,})", text)
                    records.append(LeadRecord(
                        owner=name, doc_type="Probate", clerk_url=url,
                        flags=["Probate","Inherited"], is_inherited=True,
                        doc_num=clean(case_m.group(1)) if case_m else "",
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
    try:
        html = await pw_fetch(TLN_FORECLOSURE)
        soup = BeautifulSoup(html, "lxml")
        urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "foreclosure" in href and ("case" in href or "ci20" in href.lower()):
                full = href if href.startswith("http") else TLN_BASE + href
                if full not in urls: urls.append(full)

        for url in urls[:55]:
            try:
                page_html = await pw_fetch(url)
                if len(page_html) < 500: continue
                page_soup = BeautifulSoup(page_html, "lxml")
                text      = page_soup.get_text(" ")

                case_m   = re.search(r"Case\s*No\.?\s*(CI\s*\d{4}[-\s]?\d{4,6})", text, re.I)
                doc_num  = clean(case_m.group(1)).replace(" ","") if case_m else ""
                addr_m   = re.search(
                    r"(?:known as|located at|premises|property)[:\s]*"
                    r"(\d{2,5}\s+[A-Za-z][A-Za-z\s]+(?:St|Ave|Dr|Rd|Ln|Blvd|Pl|Ct|Way)\.?)",
                    text, re.I)
                prop_address = clean(addr_m.group(1)) if addr_m else ""
                owner_m  = re.search(
                    r"(?:Defendant[s]?|Owner)[:\s]+([A-Z][A-Za-z\s,\.]+?)"
                    r"(?:\.|,\s*Et\s*Al|;|\n)", text, re.I)
                owner    = clean(owner_m.group(1)) if owner_m else ""
                amt_m    = re.search(r"\$\s*([\d,]+\.?\d*)", text)
                amount   = parse_amount(amt_m.group(1)) if amt_m else None
                date_m   = re.search(
                    r"filed[:\s]*(\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})", text, re.I)
                if not date_m: date_m = re.search(r"(\d{4}-\d{2}-\d{2})", url)
                filed    = parse_date(date_m.group(1)) if date_m else ""

                if not doc_num and not prop_address: continue

                records.append(LeadRecord(
                    owner=owner, prop_address=prop_address, prop_city="Toledo",
                    doc_type="Foreclosure", filed=filed, amount=amount,
                    doc_num=doc_num, clerk_url=url,
                    flags=["Pre-foreclosure","Lis pendens"],
                ))
            except Exception as e:
                logging.debug("Foreclosure page error: %s", e)
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

# ── DISTRESS STACKING ─────────────────────────────────────────────────────────
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
    """
    Safe dedupe — never drops records without a clear duplicate key.
    Priority: doc_num (exact match) > address+type combo.
    """
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
            out.append(rec)  # no key → always keep

    return out

def tag_new_this_week(records: list) -> list:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    for rec in records:
        if rec.filed and rec.filed >= cutoff:
            if "New this week" not in rec.flags:
                rec.flags.append("New this week")
    return records

# ── WRITE OUTPUTS ─────────────────────────────────────────────────────────────
def write_outputs(records: list, sheriff: list, probate: list,
                  tax_delinquent: list, foreclosures: list):

    def to_dict(r):
        d = asdict(r)
        d["score"] = score_record(r)
        return d

    all_dicts = [to_dict(r) for r in records]

    for p in [DATA_DIR/"records.json", DASHBOARD_DIR/"records.json"]:
        write_json(p, all_dicts)
    logging.info("Wrote data (records.json: %d records)", len(all_dicts))

    hot      = [d for d in all_dicts if d.get("hot_stack")]
    s_dicts  = [to_dict(r) for r in sheriff]
    pr_dicts = [to_dict(r) for r in probate]
    td_dicts = [to_dict(r) for r in tax_delinquent]
    fc_dicts = [to_dict(r) for r in foreclosures]
    absentee = [d for d in all_dicts if d.get("is_absentee")]
    oos      = [d for d in all_dicts if d.get("is_out_of_state")]
    vacant   = [d for d in all_dicts if d.get("is_vacant_land")]
    inherited= [d for d in all_dicts if d.get("is_inherited")]
    liens    = [d for d in all_dicts if "lien" in d.get("doc_type","").lower()]
    divorces = [d for d in all_dicts if "divorce" in d.get("doc_type","").lower()]

    for fname, data in [
        ("hot_stack.json",       hot),
        ("sheriff_sales.json",   s_dicts),
        ("probate.json",         pr_dicts),
        ("tax_delinquent.json",  td_dicts),
        ("foreclosure.json",     fc_dicts),
        ("absentee.json",        absentee),
        ("out_of_state.json",    oos),
        ("inherited.json",       inherited),
        ("vacant_land.json",     vacant),
        ("liens.json",           liens),
        ("divorces.json",        divorces),
        ("subject_to.json",      []),
        ("bankruptcy.json",      []),
        ("code_violations.json", []),
    ]:
        for p in [DATA_DIR/fname, DASHBOARD_DIR/fname]:
            write_json(p, data)

    # GHL CSV
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

# ── MAIN ──────────────────────────────────────────────────────────────────────
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

    # AREIS enrichment — owner, mailing address, absentee, OOS, vacant
    all_records = enrich_with_areis(all_records)

    # Score all records
    for rec in all_records:
        rec.score = score_record(rec)

    # Tag new this week
    all_records = tag_new_this_week(all_records)

    # Distress stacking → hot stack
    all_records = apply_distress_stacking(all_records)

    # Safe dedupe
    before = len(all_records)
    all_records = dedupe(all_records)
    logging.info("Total after dedupe: %d (was %d)", len(all_records), before)

    # Sort: hot stack → distress count → score
    all_records.sort(key=lambda r: (r.hot_stack, r.distress_count, r.score), reverse=True)

    write_outputs(all_records, sh_records, pr_records, td_records, fc_records)

if __name__ == "__main__":
    asyncio.run(main())
