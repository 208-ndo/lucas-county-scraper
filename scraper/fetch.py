"""
Toledo / Lucas County — Motivated Seller Intelligence Platform
=============================================================
Sources:
  1. Toledo Legal News      — Sheriff sales, foreclosures, liens, probate, divorces
  2. Lucas County AREIS     — Parcel owner/address/value lookup (iasWorld)
  3. data.toledo.gov        — Parcel bulk GeoJSON (owner, address, assessed value)
  4. Lucas County Treasurer — Delinquent tax list
  5. Toledo Municipal Court — Code violations / housing orders

Same architecture as Akron Data scraper.
Runs daily via GitHub Actions, deploys to GitHub Pages.
"""
import argparse, asyncio, csv, io, json, logging, re, zipfile
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE_DIR      = Path(__file__).resolve().parent.parent
DATA_DIR      = BASE_DIR / "data"
DASHBOARD_DIR = BASE_DIR / "dashboard"
DEBUG_DIR     = DATA_DIR / "debug"

DEFAULT_OUTPUT_JSON_PATHS = [DATA_DIR/"records.json", DASHBOARD_DIR/"records.json"]
DEFAULT_OUTPUT_CSV_PATH   = DATA_DIR / "ghl_export.csv"
DEFAULT_ENRICHED_CSV_PATH = DATA_DIR / "records.enriched.csv"
DEFAULT_REPORT_PATH       = DATA_DIR / "match_report.json"

LOOKBACK_DAYS   = 90
SOURCE_NAME     = "Toledo / Lucas County, Ohio"
OH_APPRECIATION = 0.04

# ── URLs ──────────────────────────────────────────────────────────────────
TLN_BASE              = "https://www.toledolegalnews.com"
TLN_SHERIFF_URL       = "https://www.toledolegalnews.com/legal_notices/sherrif_sales_lucas/"
TLN_TAX_SHERIFF_URL   = "https://www.toledolegalnews.com/legal_notices/tax_sherrif_sales/"
TLN_FORECLOSURES_URL  = "https://www.toledolegalnews.com/legal_notices/notice_of_foreclosure_complaints/"
TLN_LIENS_URL         = "https://www.toledolegalnews.com/liens/"
TLN_LIEN_MECH_URL     = "https://www.toledolegalnews.com/liens/mechanics/"
TLN_LIEN_TAX_URL      = "https://www.toledolegalnews.com/liens/us_tax/"
TLN_LIEN_CHILD_URL    = "https://www.toledolegalnews.com/liens/child_support/"
TLN_LIEN_COURT_URL    = "https://www.toledolegalnews.com/liens/lucas_county_commonpleas_court/"
TLN_COMMON_PLEAS_URL  = "https://www.toledolegalnews.com/courts/common_pleas_court_of_lucas_county/"
TLN_PROBATE_URL       = "https://www.toledolegalnews.com/courts/probate_court_of_lucas_county/"
TLN_DOMESTIC_URL      = "https://www.toledolegalnews.com/courts/domestic_court_of_lucas_county/"
TLN_DIVORCE_URL       = "https://www.toledolegalnews.com/legal_notices/divorce/"

# Parcel data — Toledo open data hub (GeoJSON with owner/address/value)
# Toledo parcel data — Lucas County Auditor iasWorld REST API
# iCare returns JSON when queried correctly
TOLEDO_PARCELS_URLS = [
    # iasWorld JSON export — all parcels
    "https://icare.co.lucas.oh.us/LucasCare/api/parcels?format=json&limit=250000",
    # ArcGIS REST — correct org ID for Lucas County
    "https://lucas.maps.arcgis.com/sharing/rest/content/items/parcels/data",
    # Toledo open data — correct endpoint
    "https://data.toledo.gov/resource/parcels.json?$limit=200000",
]
TOLEDO_PARCELS_URL = TOLEDO_PARCELS_URLS[0]
# Lucas County Treasurer delinquent list
LUCAS_TREASURER_URL   = "https://co.lucas.oh.us/500/Treasurer"
# Lucas County Auditor AREIS (iasWorld)
AREIS_URL             = "https://icare.co.lucas.oh.us/LucasCare/search/commonsearch.aspx?mode=address"
# Lucas County clerk records
CLERK_URL             = "https://lucas.dts-oh.com/PaxWorld5/"
# Toledo code enforcement / housing violations
TOLEDO_CODE_URL       = "https://egov.toledo.gov/trcis/SearchListing.aspx"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"}

# ── Lead type maps ────────────────────────────────────────────────────────
LEAD_TYPE_MAP = {
    "LP":"Lis Pendens","NOFC":"Pre-foreclosure","TAXDEED":"Tax Deed",
    "JUD":"Judgment","CCJ":"Certified Judgment","DRJUD":"Domestic Judgment",
    "LNCORPTX":"Corp Tax Lien","LNIRS":"IRS Lien","LNFED":"Federal Lien",
    "LN":"Lien","LNMECH":"Mechanic Lien","LNHOA":"HOA Lien","MEDLN":"Medicaid Lien",
    "PRO":"Probate / Estate","NOC":"Notice of Commencement","RELLP":"Release Lis Pendens",
    "TAX":"Tax Delinquent","SHERIFF":"Sheriff Sale","CODEVIOLATION":"Code Violation",
    "DIVORCE":"Divorce Filing","EVICTION":"Eviction",
}

RESIDENTIAL_LUCS = {"R1","R2","R3","R4","R5","RS","RM","510","511","512","513","514","515","520","521","522","523","530","531","532","533","540","541","542","550","551","560","561","570"}
STACK_BONUS = {2:15,3:25,4:40}

STATE_CODES = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"}

# ── Data classes ──────────────────────────────────────────────────────────
@dataclass
class LeadRecord:
    doc_num:str=""; doc_type:str=""; filed:str=""; cat:str=""; cat_label:str=""
    owner:str=""; grantee:str=""; amount:Optional[float]=None; legal:str=""
    prop_address:str=""; prop_city:str=""; prop_state:str="OH"; prop_zip:str=""
    mail_address:str=""; mail_city:str=""; mail_state:str=""; mail_zip:str=""
    clerk_url:str=""; flags:List[str]=field(default_factory=list); score:int=0
    match_method:str="unmatched"; match_score:float=0.0; with_address:int=0
    distress_sources:List[str]=field(default_factory=list); distress_count:int=0
    hot_stack:bool=False; parcel_id:str=""; luc:str=""; acres:str=""
    is_vacant_land:bool=False; is_vacant_home:bool=False
    is_absentee:bool=False; is_out_of_state:bool=False; is_inherited:bool=False
    phones:list=field(default_factory=list); phone_types:list=field(default_factory=list)
    emails:list=field(default_factory=list); skip_trace_source:str=""
    assessed_value:Optional[float]=None; estimated_value:Optional[float]=None
    last_sale_price:Optional[float]=None; last_sale_year:Optional[int]=None
    est_mortgage_balance:Optional[float]=None; est_equity:Optional[float]=None
    est_arrears:Optional[float]=None; est_payoff:Optional[float]=None
    subject_to_score:int=0; mortgage_signals:List[str]=field(default_factory=list)
    sheriff_sale_date:str=""; appraised_value:Optional[float]=None; lender:str=""
    code_violation_case:str=""; decedent_name:str=""
    executor_name:str=""; executor_state:str=""

# ── Helpers ───────────────────────────────────────────────────────────────
def ensure_dirs():
    for d in [DATA_DIR,DASHBOARD_DIR,DEBUG_DIR]: d.mkdir(parents=True,exist_ok=True)

def log_setup():
    logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s")

def save_debug_json(name,payload):
    try:(DEBUG_DIR/name).write_text(json.dumps(payload,indent=2),encoding="utf-8")
    except Exception as e:logging.warning("debug json %s: %s",name,e)

def save_debug_text(name,content):
    try:(DEBUG_DIR/name).write_text(content,encoding="utf-8")
    except Exception as e:logging.warning("debug text %s: %s",name,e)

def clean_text(v)->str:
    if v is None:return""
    return re.sub(r"\s+"," ",str(v)).strip()

def normalize_state(v:str)->str:
    v=clean_text(v).upper()
    if not v:return""
    v=re.sub(r"[^A-Z]","",v)
    return v if v in STATE_CODES else ""

def retry_request(url,attempts=3,timeout=30,method="GET",delay=2.0,**kwargs):
    """Retry with exponential backoff."""
    import time
    last=None
    if not url.startswith("http"):
        raise ValueError(f"Invalid URL: {url[:80]}")
    if any(x in url for x in ["facebook.com","twitter.com","linkedin.com","mailto:"]):
        raise ValueError(f"Skipping social URL")
    for i in range(1,attempts+1):
        try:
            if method=="POST":r=requests.post(url,headers=HEADERS,timeout=timeout,**kwargs)
            else:r=requests.get(url,headers=HEADERS,timeout=timeout,allow_redirects=True,**kwargs)
            r.raise_for_status();return r
        except Exception as e:
            last=e
            logging.warning("Request failed (%s/%s) %s: %s",i,attempts,url,e)
            if i<attempts:time.sleep(delay*i)
    raise last

async def pw_get_html(url:str,wait_ms:int=3000)->str:
    """Fetch a page using Playwright with full browser fingerprinting."""
    import random,time
    try:
        async with async_playwright() as p:
            browser=await p.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-blink-features=AutomationControlled",
                      "--disable-dev-shm-usage","--disable-gpu"]
            )
            ctx=await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width":1366,"height":768},
                locale="en-US",
                timezone_id="America/New_York",
                extra_http_headers={
                    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language":"en-US,en;q=0.9",
                    "Accept-Encoding":"gzip, deflate, br",
                    "DNT":"1","Upgrade-Insecure-Requests":"1",
                    "Sec-Fetch-Dest":"document","Sec-Fetch-Mode":"navigate",
                    "Sec-Fetch-Site":"none","Sec-Fetch-User":"?1",
                }
            )
            # Mask automation signals
            await ctx.add_init_script("""
                Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
                Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});
            """)
            page=await ctx.new_page()
            try:
                # First visit TLN homepage to set cookies
                await page.goto("https://www.toledolegalnews.com/",
                    wait_until="domcontentloaded",timeout=30000)
                await page.wait_for_timeout(2000+random.randint(0,1000))
                # Now visit the target page
                await page.goto(url,wait_until="domcontentloaded",timeout=30000)
                await page.wait_for_timeout(wait_ms+random.randint(0,1000))
                html=await page.content()
                logging.info("pw_get_html: %s chars from %s",len(html),url[:80])
                # Detect rate limit response
                if len(html)<500 and "Too Many" in html:
                    logging.warning("TLN rate limited on %s — waiting 10s and retrying",url[:60])
                    await page.wait_for_timeout(10000)
                    await page.goto(url,wait_until="domcontentloaded",timeout=30000)
                    await page.wait_for_timeout(3000)
                    html=await page.content()
                    logging.info("TLN retry: %s chars",len(html))
                return html
            finally:
                await page.close()
                await browser.close()
    except Exception as e:
        logging.warning("Playwright fetch failed %s: %s",url,e)
        return ""

def parse_amount(v:str)->Optional[float]:
    if not v:return None
    c=re.sub(r"[^0-9.\-]","",v)
    try:return float(c) if c else None
    except:return None

def normalize_address_key(address:str)->str:
    addr=clean_text(address).upper()
    for old,new in [("N.","N"),("S.","S"),("E.","E"),("W.","W"),("NORTH","N"),("SOUTH","S"),("EAST","E"),("WEST","W")]:
        addr=addr.replace(old,new)
    addr=re.sub(r"\b(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|BLVD|BOULEVARD|LN|LANE|CT|COURT|PL|PLACE|WAY|TER|TERRACE|CIR|CIRCLE)\b","",addr)
    return re.sub(r"\s+"," ",re.sub(r"[^A-Z0-9\s]","",addr)).strip()

def is_absentee_owner(prop_address:str,mail_address:str,mail_state:str="")->bool:
    if not prop_address or not mail_address:return False
    if re.search(r"\bP\.?\s*O\.?\s*BOX\b",mail_address.upper()):return True
    s=normalize_state(mail_state)
    if s and s!="OH":return True
    pk=normalize_address_key(prop_address);mk=normalize_address_key(mail_address)
    if not pk or not mk or pk==mk:return False
    def core(a):parts=a.split();return" ".join(parts[:2]) if len(parts)>=2 else a
    return core(pk)!=core(mk)

def is_out_of_state(mail_state:str)->bool:
    s=normalize_state(mail_state);return bool(s and s!="OH")

def normalize_name(n:str)->str:
    n=clean_text(n).upper();n=re.sub(r"[^A-Z0-9,&.\- /']"," ",n)
    return re.sub(r"\s+"," ",n).strip()

def get_last_name(n:str)->str:
    parts=normalize_name(n).split()
    return parts[-1] if parts else ""

def get_first_name(n:str)->str:
    parts=normalize_name(n).split()
    return parts[0] if parts else ""

def name_variants(name:str)->List[str]:
    n=clean_text(name).upper()
    if not n:return[]
    variants=set([n])
    parts=re.split(r"[\s,]+",n)
    parts=[p for p in parts if p]
    if len(parts)>=2:
        variants.add(f"{parts[0]} {parts[-1]}")
        variants.add(f"{parts[-1]} {parts[0]}")
        variants.add(f"{parts[-1]}, {parts[0]}")
        if len(parts)>=3:
            variants.add(f"{parts[0]} {parts[1]} {parts[-1]}")
            variants.add(f"{parts[-1]} {parts[0]} {parts[1]}")
    return[v for v in variants if v]

def likely_corporate_name(n:str)->bool:
    CORP={"LLC","INC","CORP","CO","COMPANY","TRUST","BANK","LTD","LP","PLC","HOLDINGS","PROPERTIES","REALTY","INVESTMENTS","CAPITAL","GROUP","PARTNERS","MANAGEMENT","ENTERPRISES"}
    return any(t in CORP for t in set(normalize_name(n).split()))

def category_flags(doc_type:str,owner:str="")->List[str]:
    flags=[];dt=clean_text(doc_type).upper();ou=normalize_name(owner)
    if dt=="LP":                        flags.append("Lis pendens")
    if dt=="NOFC":                      flags.append("Pre-foreclosure")
    if dt in{"JUD","CCJ","DRJUD"}:     flags.append("Judgment lien")
    if dt in{"TAXDEED","LNCORPTX","LNIRS","LNFED","TAX"}:flags.append("Tax lien")
    if dt=="LNMECH":                    flags.append("Mechanic lien")
    if dt=="PRO":                       flags.append("Probate / estate")
    if dt=="SHERIFF":                   flags.append("Sheriff sale scheduled")
    if dt=="CODEVIOLATION":             flags.append("Code violation")
    if dt=="DIVORCE":                   flags.append("Divorce filing")
    if dt=="EVICTION":                  flags.append("Eviction filed")
    if any(t in f" {ou} " for t in [" LLC"," INC"," CORP"," CO "," TRUST"," LP"," LTD"]):
        flags.append("LLC / corp owner")
    return list(dict.fromkeys(flags))

def classify_distress_source(doc_type:str)->Optional[str]:
    dt=clean_text(doc_type).upper()
    if dt in{"LP","RELLP"}:return "lis_pendens"
    if dt=="NOFC":return "foreclosure"
    if dt in{"JUD","CCJ","DRJUD"}:return "judgment"
    if dt in{"LN","LNHOA","LNFED","LNIRS","LNCORPTX","MEDLN"}:return "lien"
    if dt=="LNMECH":return "mechanic_lien"
    if dt in{"TAXDEED","TAX"}:return "tax_delinquent"
    if dt=="PRO":return "probate"
    if dt=="SHERIFF":return "sheriff_sale"
    if dt=="CODEVIOLATION":return "code_violation"
    if dt=="DIVORCE":return "divorce"
    if dt=="EVICTION":return "eviction"
    return None

# ── Mortgage / equity estimation ─────────────────────────────────────────
def estimate_mortgage_data(record:LeadRecord)->LeadRecord:
    signals=[];sto=0
    market_val=record.estimated_value
    if not market_val and record.last_sale_price and record.last_sale_price>5000:
        yrs=max(0,datetime.now().year-(record.last_sale_year or datetime.now().year))
        market_val=record.last_sale_price*((1+OH_APPRECIATION)**yrs)
    if not market_val and record.assessed_value and record.assessed_value>1000:
        market_val=record.assessed_value/0.35
    if market_val:record.estimated_value=round(market_val,2)
    if record.last_sale_price and record.last_sale_year and record.last_sale_price>5000:
        yrs_elapsed=max(0,min(30,datetime.now().year-record.last_sale_year))
        orig=record.last_sale_price*0.80;mr=0.065/12;n=360;paid=yrs_elapsed*12
        if mr>0 and paid<n:
            bal=orig*((1+mr)**n-(1+mr)**paid)/((1+mr)**n-1)
            record.est_mortgage_balance=round(max(0,bal),2)
        elif paid>=n:record.est_mortgage_balance=0.0
    if record.estimated_value and record.est_mortgage_balance is not None:
        record.est_equity=round(record.estimated_value-record.est_mortgage_balance,2)
    elif record.estimated_value and record.est_mortgage_balance is None and not record.last_sale_price:
        record.est_mortgage_balance=round(record.estimated_value*0.50,2)
        record.est_equity=round(record.estimated_value*0.50,2)
        record.est_payoff=record.est_mortgage_balance
        signals.append("Est. equity (no sale history)")
    if record.doc_type in{"LP","NOFC","TAXDEED","SHERIFF"} and record.amount and record.amount>0:
        record.est_arrears=record.amount
        record.est_payoff=record.est_mortgage_balance or record.amount
        signals.append(f"Arrears ~${record.est_arrears:,.0f}")
    if "Tax lien" in record.flags and record.amount and record.amount>0:
        record.est_arrears=(record.est_arrears or 0)+record.amount
        signals.append(f"Tax owed ~${record.amount:,.0f}")
    if record.est_equity is not None:
        if record.est_equity>50000:sto+=30;signals.append("High equity 🏦")
        elif record.est_equity>20000:sto+=20;signals.append("Moderate equity")
        elif record.est_equity>0:sto+=10
        else:signals.append("Underwater ⚠️")
    if record.doc_type in{"LP","NOFC","SHERIFF"}:sto+=25;signals.append("Active foreclosure")
    if record.doc_type=="PRO":sto+=20;signals.append("Estate / probate")
    if record.is_absentee:sto+=15;signals.append("Absentee owner")
    if record.is_out_of_state:sto+=10;signals.append("Out-of-state owner")
    if record.is_inherited:sto+=20;signals.append("Inherited property")
    if "Tax lien" in record.flags:sto+=15
    if record.est_mortgage_balance and record.estimated_value:
        ltv=record.est_mortgage_balance/record.estimated_value
        if ltv<0.5:sto+=20;signals.append("Low LTV <50%")
        elif ltv<0.7:sto+=10;signals.append("LTV <70%")
        elif ltv>0.95:signals.append("High LTV >95%")
    if sto>=50 and "🎯 Subject-To Candidate" not in record.flags:
        record.flags.append("🎯 Subject-To Candidate")
    if sto>=70 and "⭐ Prime Subject-To" not in record.flags:
        record.flags.append("⭐ Prime Subject-To")
    record.subject_to_score=min(sto,100);record.mortgage_signals=signals
    return record

def score_record(record:LeadRecord)->int:
    score=30;lf={f.lower() for f in record.flags};fs=0
    if "lis pendens" in lf:fs+=20
    if "pre-foreclosure" in lf:fs+=20
    if "judgment lien" in lf:fs+=15
    if "tax lien" in lf:fs+=15
    if "mechanic lien" in lf:fs+=10
    if "probate / estate" in lf:fs+=15
    if "vacant home" in lf:fs+=25
    if "sheriff sale scheduled" in lf:fs+=35
    if "code violation" in lf:fs+=20
    if "eviction filed" in lf:fs+=18
    if "divorce filing" in lf:fs+=15
    if "absentee owner" in lf:fs+=10
    if "out-of-state owner" in lf:fs+=12
    if "tax delinquent" in lf:fs+=10
    if "inherited property" in lf:fs+=15
    if "🎯 subject-to candidate" in lf:fs+=15
    if "⭐ prime subject-to" in lf:fs+=20
    score+=min(fs,70)
    if "lis pendens" in lf and "pre-foreclosure" in lf:score+=20
    if record.amount is not None:
        score+=15 if record.amount>100000 else(10 if record.amount>50000 else 5)
    if record.filed:
        try:
            if datetime.fromisoformat(record.filed).date()>=(datetime.now().date()-timedelta(days=7)):
                if "New this week" not in record.flags:record.flags.append("New this week")
                score+=5
        except:pass
    if record.prop_address:score+=5
    if record.mail_address:score+=3
    dc=len(set(record.distress_sources));record.distress_count=dc
    bk=min(dc,4)
    if bk>=2:
        score+=STACK_BONUS.get(bk,STACK_BONUS[4]);record.hot_stack=True
        if "🔥 Hot Stack" not in record.flags:record.flags.append("🔥 Hot Stack")
    return min(score,100)

# ── Parcel data ───────────────────────────────────────────────────────────
def load_parcel_data()->Dict[str,dict]:
    """
    Load Toledo/Lucas County parcel data.
    Tries multiple sources — works gracefully if all fail.
    Primary: Lucas County Auditor open data (Socrata API)
    Fallback: address-only index built from TLN scrape results
    """
    parcels:Dict[str,dict]={}
    try:
        logging.info("Loading Toledo parcel data...")
        # Try Socrata API — data.toledo.gov uses this format
        socrata_urls=[
            # Lucas County GIS - parcel layer with owner/address info
            "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer/0/query?where=1%3D1&outFields=PARCEL_ID,OWNER,SITE_ADDR,SITE_CITY,SITE_ZIP,MAIL_ADDR,MAIL_CITY,MAIL_STATE,MAIL_ZIP,APPRTOT,LUC&f=json&resultRecordCount=50000&resultOffset=0",
            # Toledo open data parcels
            "https://data.toledo.gov/resource/k95c-9tfe.json?$limit=200000",
            # Alternative GIS layer
            "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer/9/query?where=1%3D1&outFields=*&f=json&resultRecordCount=1000",
        ]
        resp=None
        data_json=None
        for purl in socrata_urls:
            try:
                logging.info("Trying parcel URL: %s",purl[:80])
                r=retry_request(purl,timeout=60)
                if r and len(r.content)>500:
                    try:
                        data_json=r.json()
                        if data_json:
                            logging.info("Parcel data loaded: %s bytes, type=%s",len(r.content),type(data_json).__name__)
                            break
                    except:pass
            except Exception as pe:
                logging.warning("Parcel URL failed: %s",str(pe)[:100])
        if not data_json:
            logging.warning("No parcel data available — leads will still work without address enrichment")
            return parcels
        # Handle both list (Socrata) and dict (ArcGIS) response formats
        if isinstance(data_json,list):
            raw_records=data_json  # Socrata — flat list
        elif isinstance(data_json,dict):
            features=data_json.get("features",[])
            if features and isinstance(features[0],dict):
                # ArcGIS format: features[i].attributes or features[i].properties
                raw_records=[f.get("attributes",f.get("properties",f)) for f in features]
            else:
                raw_records=data_json.get("value",data_json.get("data",[]))
        else:
            raw_records=[]
        logging.info("Parcel records to process: %s",len(raw_records))
        # Log first record to see field names
        if raw_records:
            save_debug_json("parcel_fields.json",list(raw_records[0].keys())[:30] if raw_records[0] else [])
            logging.info("Parcel fields: %s",list(raw_records[0].keys())[:10])
        for props in raw_records:
            if not props or not isinstance(props,dict):continue
            # Normalize ArcGIS uppercase field names to expected format
            props={k.upper():v for k,v in props.items()}
            # Toledo parcel fields vary — try common names
            # All keys already uppercased — try all common field name variants
            owner=clean_text(props.get("OWNER","") or props.get("OWNER_NAME","") or props.get("OWNERNAME","") or props.get("OWN1","") or "")
            addr=clean_text(props.get("SITE_ADDR","") or props.get("SITEADDR","") or props.get("ADDRESS","") or props.get("SADDR","") or "")
            city=clean_text(props.get("SITE_CITY","") or props.get("CITY","") or props.get("SCITY","") or "") or "Toledo"
            zip_=clean_text(props.get("SITE_ZIP","") or props.get("ZIP","") or props.get("ZIPCODE","") or props.get("SZIP","") or "")
            mail_addr=clean_text(props.get("MAIL_ADDR","") or props.get("MAILADR1","") or props.get("MAIL_ADDRESS","") or "")
            mail_city=clean_text(props.get("MAIL_CITY","") or props.get("MAILCITY","") or "")
            mail_state=clean_text(props.get("MAIL_STATE","") or props.get("MAILSTATE","") or "") or "OH"
            mail_zip=clean_text(props.get("MAIL_ZIP","") or props.get("MAILZIP","") or "")
            parcel_id=clean_text(props.get("PARCEL_ID","") or props.get("PARCELID","") or props.get("PARID","") or props.get("PID","") or "")
            assessed=None
            for k in ["ASSESSED_VALUE","ASSDVAL","TOTAL_APPR","TOTALAPPR","assessed_value"]:
                v=clean_text(props.get(k,""))
                if v:
                    try:
                        assessed=float(re.sub(r"[^0-9.]","",v))
                        if assessed>100:break
                    except:pass
            luc=(clean_text(props.get("LUC","")) or
                 clean_text(props.get("LAND_USE","")) or
                 clean_text(props.get("land_use","")) or "")
            if not addr:continue
            key=normalize_address_key(addr)
            if not key:continue
            rec={
                "parcel_id":parcel_id,"owner":owner.title(),
                "prop_address":addr.title(),"prop_city":city.title(),"prop_zip":zip_,
                "mail_address":mail_addr.title(),"mail_city":mail_city.title(),
                "mail_state":normalize_state(mail_state) or "OH","mail_zip":mail_zip,
                "assessed_value":assessed,
                "est_market_value":round(assessed/0.35) if assessed and assessed>100 else None,
                "luc":luc,
            }
            parcels[key]=rec
            # Also index by owner name variants
            for v in name_variants(owner):
                parcels[f"OWNER:{v}"]=rec
        logging.info("Parcel data loaded: %s addresses",len([k for k in parcels if not k.startswith("OWNER:")]))
        save_debug_json("parcel_sample.json",list({k:v for k,v in parcels.items() if not k.startswith("OWNER:")}.values())[:25])
    except Exception as e:
        logging.warning("Parcel load failed: %s",e)
    return parcels

def match_parcel(owner:str,prop_address:str,parcels:Dict[str,dict])->Optional[dict]:
    """Try to match a lead to a parcel record."""
    if prop_address:
        key=normalize_address_key(prop_address)
        if key and key in parcels:return parcels[key]
    if owner:
        for v in name_variants(owner):
            k=f"OWNER:{v}"
            if k in parcels:return parcels[k]
    return None

def enrich_lead(record:LeadRecord,parcels:Dict[str,dict])->LeadRecord:
    matched=match_parcel(record.owner,record.prop_address,parcels)
    if matched:
        record.prop_address=record.prop_address or matched.get("prop_address","")
        record.prop_city=record.prop_city or matched.get("prop_city","") or "Toledo"
        record.prop_zip=record.prop_zip or matched.get("prop_zip","")
        record.mail_address=record.mail_address or matched.get("mail_address","")
        record.mail_city=record.mail_city or matched.get("mail_city","")
        record.mail_state=record.mail_state or matched.get("mail_state","OH")
        record.mail_zip=record.mail_zip or matched.get("mail_zip","")
        record.parcel_id=record.parcel_id or matched.get("parcel_id","")
        record.luc=record.luc or matched.get("luc","")
        if not record.assessed_value:record.assessed_value=matched.get("assessed_value")
        if not record.estimated_value:record.estimated_value=matched.get("est_market_value")
        record.match_method="parcel_lookup";record.match_score=0.9
    if not record.prop_city:record.prop_city="Toledo"
    if not record.prop_state:record.prop_state="OH"
    record.with_address=1 if record.prop_address else 0
    record.is_absentee=is_absentee_owner(record.prop_address,record.mail_address,record.mail_state)
    record.is_out_of_state=is_out_of_state(record.mail_state)
    if record.is_absentee and "Absentee owner" not in record.flags:record.flags.append("Absentee owner")
    if record.is_out_of_state and "Out-of-state owner" not in record.flags:record.flags.append("Out-of-state owner")
    record.flags=list(dict.fromkeys(record.flags+category_flags(record.doc_type,record.owner)))
    record=estimate_mortgage_data(record);record.score=score_record(record)
    return record

# ── Toledo Legal News scrapers ────────────────────────────────────────────
def try_parse_date(text:str)->Optional[str]:
    text=clean_text(text)
    if not text:return None
    for p in [r"\b\d{4}-\d{2}-\d{2}\b",r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",r"\b\w+ \d{1,2},? \d{4}\b"]:
        m=re.search(p,text)
        if m:
            raw=m.group(0)
            for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%B %d, %Y","%B %d %Y"):
                try:return datetime.strptime(raw,fmt).date().isoformat()
                except:continue
    return None

def infer_doc_type(text:str)->Optional[str]:
    t=clean_text(text).upper()
    if any(x in t for x in ["LIS PENDENS"," LP ","LP-","LIS-PENDENS"]):return "LP"
    if any(x in t for x in ["NOTICE OF FORECLOSURE","FORECLOS","NOFC","COMPLAINT TO FORECLOSE"]):return "NOFC"
    if any(x in t for x in ["DIVORCE","DISSOLUTION OF MARRIAGE"]):return "DIVORCE"
    if any(x in t for x in ["EVICTION","FED ","FORCIBLE ENTRY","UNLAWFUL DETAINER"]):return "EVICTION"
    if any(x in t for x in ["CERTIFIED JUDGMENT","DOMESTIC JUDGMENT","JUDGMENT"]):return "JUD"
    if any(x in t for x in ["TAX DEED","TAXDEED"]):return "TAXDEED"
    if any(x in t for x in ["IRS LIEN","FEDERAL LIEN","TAX LIEN","US TAX"]):return "LNFED"
    if "MECHANIC LIEN" in t:return "LNMECH"
    if "CHILD SUPPORT" in t:return "LN"
    if "LIEN" in t:return "LN"
    if "PROBATE" in t or "ESTATE OF" in t:return "PRO"
    if "NOTICE OF COMMENCEMENT" in t:return "NOC"
    return None

def split_vs(caption:str)->Tuple[str,str]:
    cap=clean_text(caption);upper=cap.upper()
    for sep in [" -VS- "," VS. "," VS "," V. "," V "]:
        if sep in upper:
            parts=re.split(re.escape(sep),cap,maxsplit=1,flags=re.IGNORECASE)
            if len(parts)==2:return clean_text(parts[0]),clean_text(parts[1])
    return "",""

async def scrape_tln_page(url:str,doc_type_hint:str=None)->List[LeadRecord]:
    """Scrape a Toledo Legal News listing page for leads using Playwright."""
    records:List[LeadRecord]=[]
    try:
        html=await pw_get_html(url)
        if not html:return records
        soup=BeautifulSoup(html,"lxml")
        save_debug_text(f"tln_{doc_type_hint or 'page'}.html",html[:8000])
        text_preview=soup.get_text(" ")[:500]
        logging.info("TLN page %s: %s chars, preview: %s",url[-40:],len(html),text_preview[:100])
        text=soup.get_text(" ")

        # Try table rows
        for row in soup.select("tr"):
            cells=[clean_text(td.get_text(" ")) for td in row.select("td,th")]
            if not cells or len(cells)<2:continue
            rt=clean_text(" ".join(cells))
            if len(rt)<10:continue
            dt=doc_type_hint or infer_doc_type(rt)
            if not dt:continue
            filed=try_parse_date(rt) or datetime.now().date().isoformat()
            try:
                if datetime.fromisoformat(filed).date()<(datetime.now().date()-timedelta(days=LOOKBACK_DAYS)):continue
            except:pass
            plaintiff,defendant=split_vs(rt)
            owner=defendant.title() if defendant else ""
            if not owner:
                m=re.search(r"([A-Z][A-Z\s,\.]{5,40}(?:LLC|INC|CORP|TRUST)?)",rt)
                owner=clean_text(m.group(1)).title() if m else ""
            am=re.search(r"\$[\d,]+(?:\.\d{2})?",rt)
            amt=parse_amount(am.group(0)) if am else None
            # Extract address
            addr_m=re.search(r"(\d{2,5}\s+[A-Z][A-Za-z\s]{3,25}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL|WAY|TER|CIR|PKWY)\.?)",rt,re.IGNORECASE)
            prop_address=clean_text(addr_m.group(1)).title() if addr_m else ""
            link=row.find("a",href=True)
            href=clean_text(link.get("href","")) if link else ""
            clerk_url=requests.compat.urljoin(TLN_BASE,href) if href else url
            case_m=re.search(r"\b(\d{4}[A-Z]{2}\d{4,8}|\w{2}\d{4}-\d{4,8}|CI\d{4}-\d+|TF\d+|LN\d+)\b",rt)
            doc_num=clean_text(case_m.group(1)) if case_m else f"{dt}-{len(records)+1:04d}"
            flags=category_flags(dt,owner)
            rec=LeadRecord(
                doc_num=doc_num,doc_type=dt,filed=filed,cat=dt,
                cat_label=LEAD_TYPE_MAP.get(dt,dt),
                owner=owner,grantee=plaintiff.title() if plaintiff else "",
                amount=amt,prop_address=prop_address,prop_city="Toledo",prop_state="OH",
                clerk_url=clerk_url,flags=flags,
                distress_sources=[s for s in [classify_distress_source(dt)] if s],
            )
            rec=estimate_mortgage_data(rec);rec.score=score_record(rec)
            records.append(rec)

        # Also try article/paragraph format (TLN uses both)
        if not records:
            # Pattern: "LN202409951; $809.79; PERSONAL INCOME TAX; ...; OWNER NAME; ADDRESS"
            lien_pat=re.compile(
                r"(LN\d{6,}|CI\d{4}-\d+|TF\d+);\s*\$?([\d,\.]+);\s*([^;]+);\s*([^;]+);\s*([^;]+);\s*([^;\n]+)",
                re.IGNORECASE)
            for m in lien_pat.finditer(text):
                doc_num=clean_text(m.group(1))
                try:amt=float(m.group(2).replace(",",""))
                except:amt=None
                source=clean_text(m.group(3))
                plaintiff=clean_text(m.group(4))
                owner=clean_text(m.group(5)).title()
                addr_raw=clean_text(m.group(6))
                dt_inf=infer_doc_type(source) or doc_type_hint or "LN"
                addr_m=re.search(r"(\d{2,5}\s+\w[\w\s]{3,30}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL)\.?),\s*(\w[\w\s]+),\s*OH\s*(\d{5})?",addr_raw,re.IGNORECASE)
                prop_address=clean_text(addr_m.group(1)).title() if addr_m else ""
                prop_city=clean_text(addr_m.group(2)).title() if addr_m else "Toledo"
                prop_zip=clean_text(addr_m.group(3)) if addr_m and addr_m.group(3) else ""
                flags=category_flags(dt_inf,owner)
                rec=LeadRecord(
                    doc_num=doc_num,doc_type=dt_inf,filed=datetime.now().date().isoformat(),
                    cat=dt_inf,cat_label=LEAD_TYPE_MAP.get(dt_inf,dt_inf),
                    owner=owner,grantee=plaintiff,amount=amt,
                    prop_address=prop_address,prop_city=prop_city,prop_state="OH",prop_zip=prop_zip,
                    clerk_url=url,flags=flags,
                    distress_sources=[s for s in [classify_distress_source(dt_inf)] if s],
                )
                rec=estimate_mortgage_data(rec);rec.score=score_record(rec)
                records.append(rec)

    except Exception as e:logging.warning("TLN scrape %s failed: %s",url,e)
    return records

async def scrape_tln_sheriff_sales()->List[LeadRecord]:
    records:List[LeadRecord]=[]
    try:
        logging.info("Scraping sheriff sales...")
        for url in [TLN_SHERIFF_URL,TLN_TAX_SHERIFF_URL]:
            html=await pw_get_html(url)
            if not html:continue
            soup=BeautifulSoup(html,"lxml")
            save_debug_text("sheriff_page.html",html[:8000])
            logging.info("Sheriff page %s chars",len(html))
            # TLN sheriff sale format: case# | address | amount | date
            for row in soup.select("tr"):
                cells=[clean_text(td.get_text(" ")) for td in row.select("td")]
                if not cells or len(cells)<3:continue
                rt=" ".join(cells)
                # Skip header rows
                if any(x in rt.upper() for x in ["CASE","PARCEL","ADDRESS","AMOUNT","DATE"]) and len(rt)<80:continue
                addr_m=re.search(r"(\d{2,5}\s+[A-Z][A-Za-z\s]{3,30}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL)\.?)",rt,re.IGNORECASE)
                prop_address=clean_text(addr_m.group(1)).title() if addr_m else ""
                am=re.search(r"\$([\d,]+(?:\.\d{2})?)",rt)
                try:amt=float(am.group(1).replace(",","")) if am else None
                except:amt=None
                dm=try_parse_date(rt)
                case_m=re.search(r"\b(\d{4}[A-Z]{2}\d{4,}|TF\d+|CI\d{4}-\d+)\b",rt)
                doc_num=clean_text(case_m.group(1)) if case_m else f"SHERIFF-{len(records)+1:04d}"
                # Skip rows with no useful content
                if not prop_address and not doc_num.startswith("SHERIFF"):continue
                # Extract parcel from "Parcel: XXXX-XXXX-XXXX"
                parcel_m=re.search(r"Parcel[:\s]+([A-Z0-9\-]+)",rt,re.IGNORECASE)
                parcel=clean_text(parcel_m.group(1)) if parcel_m else ""
                flags=["Sheriff sale scheduled","Foreclosure","🔥 Hot Stack"]
                link=row.find("a",href=True)
                href=clean_text(link.get("href","")) if link else ""
                clerk_url=requests.compat.urljoin(TLN_BASE,href) if href else url
                rec=LeadRecord(
                    doc_num=doc_num,doc_type="SHERIFF",filed=dm or datetime.now().date().isoformat(),
                    cat="SHERIFF",cat_label="Sheriff Sale",
                    amount=amt,appraised_value=amt,
                    prop_address=prop_address,prop_city="Toledo",prop_state="OH",
                    parcel_id=parcel,sheriff_sale_date=dm or "",
                    clerk_url=clerk_url,flags=flags,
                    distress_sources=["sheriff_sale","foreclosure"],
                    distress_count=2,hot_stack=True,with_address=1 if prop_address else 0,
                )
                rec=estimate_mortgage_data(rec);rec.score=score_record(rec)
                records.append(rec)
        logging.info("Sheriff sales: %s",len(records))
        save_debug_json("sheriff_sales.json",[asdict(r) for r in records[:20]])
    except Exception as e:logging.warning("Sheriff sales failed: %s",e)
    return records

async def scrape_tln_foreclosures()->List[LeadRecord]:
    logging.info("Scraping foreclosures / lis pendens...")
    recs=await scrape_tln_page(TLN_FORECLOSURES_URL,"NOFC")
    logging.info("Foreclosures: %s",len(recs))
    return recs

async def scrape_tln_liens()->List[LeadRecord]:
    logging.info("Scraping liens...")
    recs=[]
    for url,dt in [
        (TLN_LIEN_CHILD_URL,"LN"),
        (TLN_LIEN_MECH_URL,"LNMECH"),
        (TLN_LIEN_COURT_URL,"LN"),
        (TLN_LIEN_TAX_URL,"LNFED"),
    ]:
        try:
            r=await scrape_tln_page(url,dt)
            recs.extend(r)
            logging.info("Liens from %s: %s",url,len(r))
        except Exception as e:
            logging.warning("Lien URL %s: %s",url,e)
    logging.info("Total liens: %s",len(recs))
    return recs

async def scrape_tln_common_pleas()->List[LeadRecord]:
    """Scrape Toledo Legal News Common Pleas daily filings for LP, foreclosure, liens."""
    records:List[LeadRecord]=[]
    try:
        logging.info("Scraping Common Pleas filings...")
        html=await pw_get_html(TLN_COMMON_PLEAS_URL)
        soup=BeautifulSoup(html,"lxml")
        # Find links to recent daily filing pages
        links=[]
        for a in soup.select("a[href]"):
            href=clean_text(a.get("href",""))
            # Skip social share / mailto / external links
            if any(x in href for x in ["facebook","twitter","mailto","linkedin","utm_source"]):
                continue
            if "common_pleas" in href and ("filings" in href or "received" in href or "article" in href):
                if href.startswith("http"):
                    full=href
                else:
                    full=requests.compat.urljoin(TLN_BASE,href)
                if full not in links and "toledolegalnews.com" in full:
                    links.append(full)
        logging.info("Common Pleas daily filing pages: %s",len(links))
        for url in links[:7]:  # last 7 days
            try:
                r=await scrape_tln_page(url)
                records.extend(r)
            except Exception as e:
                logging.warning("Common Pleas page %s: %s",url,e)
        logging.info("Common Pleas leads: %s",len(records))
    except Exception as e:logging.warning("Common Pleas failed: %s",e)
    return records

async def scrape_tln_probate()->List[LeadRecord]:
    logging.info("Scraping probate / estate...")
    records:List[LeadRecord]=[]
    try:
        html=await pw_get_html(TLN_PROBATE_URL)
        if not html:return records
        soup=BeautifulSoup(html,"lxml")
        text=soup.get_text(" ")
        save_debug_text("probate_text.txt",text[:5000])
        estate_pat=re.compile(
            r"Estate\s+of\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}),?\s+(?:deceased|DECEASED)",re.IGNORECASE)
        for m in estate_pat.finditer(text):
            name=clean_text(m.group(1))
            surrounding=text[max(0,m.start()-50):m.end()+600]
            dm=re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})",surrounding)
            filed=datetime.now().date().isoformat()
            if dm:
                for fmt in ("%m/%d/%Y","%m/%d/%y"):
                    try:filed=datetime.strptime(dm.group(1),fmt).date().isoformat();break
                    except:continue
            rec=LeadRecord(
                doc_num=f"PRO-{name.replace(' ','-')}-{len(records)+1}",
                doc_type="PRO",filed=filed,cat="PRO",cat_label="Probate / Estate",
                owner=name.title(),decedent_name=name.title(),is_inherited=True,
                flags=["Probate / estate","Inherited property"],
                distress_sources=["probate"],distress_count=1,
                clerk_url=TLN_PROBATE_URL,match_method="probate_name",
            )
            rec=estimate_mortgage_data(rec);rec.score=score_record(rec)
            records.append(rec)
        logging.info("Probate: %s",len(records))
    except Exception as e:logging.warning("Probate failed: %s",e)
    return records

async def scrape_tln_divorce()->List[LeadRecord]:
    logging.info("Scraping divorces...")
    recs=await scrape_tln_page(TLN_DIVORCE_URL,"DIVORCE")
    # Also try domestic court page
    try:
        r2=await scrape_tln_page(TLN_DOMESTIC_URL,"DIVORCE")
        recs.extend(r2)
    except:pass
    logging.info("Divorces: %s",len(recs))
    return recs

async def scrape_lucas_tax_delinquent()->List[LeadRecord]:
    """
    Scrape Lucas County tax delinquent list.
    Toledo Legal News publishes the official delinquent list.
    """
    records:List[LeadRecord]=[]
    try:
        logging.info("Scraping tax delinquent...")
        urls=[
            "https://www.toledolegalnews.com/legal_notices/foreclosures/",
            "https://co.lucas.oh.us/500/Treasurer",
        ]
        for url in urls:
            try:
                html=await pw_get_html(url)
                if not html:continue
                soup=BeautifulSoup(html,"lxml")
                text=soup.get_text(" ")
                # Look for delinquent parcel patterns
                # TF (tax foreclosure) case numbers
                tf_pat=re.compile(r"(TF\d{6,}|TF\d{4}-\d+)\s+(.{10,80}?)\s+\$?([\d,]+(?:\.\d{2})?)",re.IGNORECASE)
                for m in tf_pat.finditer(text):
                    case_num=clean_text(m.group(1))
                    details=clean_text(m.group(2))
                    try:amt=float(m.group(3).replace(",",""))
                    except:amt=None
                    addr_m=re.search(r"(\d{2,5}\s+\w[\w\s]{3,25}(?:ST|AVE|RD|DR|BLVD|LN|CT|PL)\.?)",details,re.IGNORECASE)
                    prop_address=clean_text(addr_m.group(1)).title() if addr_m else ""
                    parcel_m=re.search(r"([A-Z0-9]{2}-\d{6}|\d{2}-\d{6}-\d{3})",details)
                    parcel=clean_text(parcel_m.group(1)) if parcel_m else ""
                    rec=LeadRecord(
                        doc_num=case_num,doc_type="TAX",filed=datetime.now().date().isoformat(),
                        cat="TAX",cat_label="Tax Delinquent",amount=amt,
                        prop_address=prop_address,prop_city="Toledo",prop_state="OH",
                        parcel_id=parcel,clerk_url=url,
                        flags=["Tax delinquent","Tax lien"],
                        distress_sources=["tax_delinquent"],distress_count=1,
                    )
                    rec=estimate_mortgage_data(rec);rec.score=score_record(rec)
                    records.append(rec)
                if records:break
            except Exception as e:logging.warning("Tax delin URL %s: %s",url,e)
        logging.info("Tax delinquent: %s",len(records))
    except Exception as e:logging.warning("Tax delinquent failed: %s",e)
    return records

# ── Cross-stacking ────────────────────────────────────────────────────────
def cross_stack_by_address(records:List[LeadRecord])->List[LeadRecord]:
    addr_map:Dict[str,List[int]]=defaultdict(list)
    for i,r in enumerate(records):
        if r.prop_address:
            key=normalize_address_key(r.prop_address)
            if key:addr_map[key].append(i)
    stacked=0
    for key,idxs in addr_map.items():
        if len(idxs)<2:continue
        all_sources:set=set()
        for i in idxs:all_sources.update(records[i].distress_sources or [])
        if len(all_sources)<2:continue
        for i in idxs:
            r=records[i]
            r.distress_sources=list(set(list(r.distress_sources or[])+list(all_sources)))
            r.distress_count=len(r.distress_sources)
            r.hot_stack=True
            if "🔥 Hot Stack" not in r.flags:r.flags.append("🔥 Hot Stack")
            if "📍 Cross-List Match" not in r.flags:r.flags.append("📍 Cross-List Match")
            r=estimate_mortgage_data(r);r.score=score_record(r);records[i]=r
        stacked+=1
    logging.info("Cross-stacking: %s property groups → Hot Stack",stacked)
    return records

def dedupe_records(records:List[LeadRecord])->List[LeadRecord]:
    final,seen=[],set()
    for r in records:
        nd=re.sub(r"^(PCF1|PCF2)-","",clean_text(r.doc_num).upper())
        key=(nd,clean_text(r.doc_type).upper(),normalize_name(r.owner),clean_text(r.filed))
        if key in seen:continue
        seen.add(key);final.append(r)
    return final

# ── Output ────────────────────────────────────────────────────────────────
def split_name(n):
    parts=clean_text(n).split()
    if not parts:return"",""
    if len(parts)==1:return parts[0],""
    return parts[0]," ".join(parts[1:])

def write_json(path,payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(payload,indent=2),encoding="utf-8")

def build_payload(records):
    return{
        "fetched_at":datetime.now(timezone.utc).isoformat(),"source":SOURCE_NAME,
        "date_range":{"from":(datetime.now()-timedelta(days=LOOKBACK_DAYS)).date().isoformat(),"to":datetime.now().date().isoformat()},
        "total":len(records),
        "with_address":sum(1 for r in records if r.prop_address),
        "hot_stack_count":sum(1 for r in records if r.hot_stack),
        "sheriff_sale_count":sum(1 for r in records if r.doc_type=="SHERIFF"),
        "probate_count":sum(1 for r in records if r.doc_type=="PRO"),
        "tax_delinquent_count":sum(1 for r in records if r.doc_type=="TAX"),
        "foreclosure_count":sum(1 for r in records if r.doc_type=="NOFC"),
        "absentee_count":sum(1 for r in records if r.is_absentee),
        "out_of_state_count":sum(1 for r in records if r.is_out_of_state),
        "subject_to_count":sum(1 for r in records if r.subject_to_score>=50),
        "prime_subject_to_count":sum(1 for r in records if r.subject_to_score>=70),
        "records":[asdict(r) for r in records],
    }

def write_json_outputs(records,extra_json_path=None):
    payload=build_payload(records)
    paths=list(DEFAULT_OUTPUT_JSON_PATHS)
    if extra_json_path:paths.append(extra_json_path)
    seen=set()
    for path in paths:
        if str(path) in seen:continue
        seen.add(str(path));write_json(path,payload)
    logging.info("Wrote main JSON outputs")

def write_category_json(records):
    categories={
        "hot_stack":        [r for r in records if r.hot_stack],
        "sheriff_sales":    [r for r in records if r.doc_type=="SHERIFF"],
        "probate":          [r for r in records if r.doc_type=="PRO"],
        "tax_delinquent":   [r for r in records if r.doc_type=="TAX"],
        "foreclosure":      [r for r in records if r.doc_type in{"NOFC","LP","TAXDEED"}],
        "liens":            [r for r in records if r.doc_type in{"LN","LNMECH","LNFED","LNIRS","LNCORPTX","MEDLN"}],
        "absentee":         [r for r in records if r.is_absentee],
        "out_of_state":     [r for r in records if r.is_out_of_state],
        "subject_to":       [r for r in records if r.subject_to_score>=50],
        "prime_subject_to": [r for r in records if r.subject_to_score>=70],
        "divorces":         [r for r in records if r.doc_type=="DIVORCE"],
    }
    descs={
        "hot_stack":       "🔥 2+ distress signals — highest priority",
        "sheriff_sales":   "⚡ Properties scheduled for sheriff auction",
        "probate":         "⚖️ Estate / probate filings — inherited properties",
        "tax_delinquent":  "💰 Tax delinquent / tax foreclosure filings",
        "foreclosure":     "⚠️ Active foreclosure / lis pendens",
        "liens":           "🔗 Judgment and tax liens",
        "absentee":        "📭 Absentee owner — mailing differs from property",
        "out_of_state":    "🌎 Out-of-state owner",
        "subject_to":      "🎯 Subject-To candidates (score ≥50)",
        "prime_subject_to":"⭐ Prime Subject-To deals (score ≥70)",
        "divorces":        "💔 Divorce / dissolution filings",
    }
    for cat,recs in categories.items():
        recs_s=sorted(recs,key=lambda r:(r.hot_stack,r.distress_count,r.subject_to_score,r.score),reverse=True)
        payload={
            "fetched_at":datetime.now(timezone.utc).isoformat(),"source":SOURCE_NAME,
            "category":cat,"description":descs[cat],"total":len(recs_s),
            "records":[asdict(r) for r in recs_s],
        }
        for path in [DATA_DIR/f"{cat}.json",DASHBOARD_DIR/f"{cat}.json"]:
            write_json(path,payload)
        logging.info("Wrote %s: %s records",cat,len(recs_s))

def write_csv(records,csv_path):
    csv_path.parent.mkdir(parents=True,exist_ok=True)
    fieldnames=["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
                "Property Address","Property City","Property State","Property Zip",
                "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
                "Seller Score","Subject-To Score","Motivated Seller Flags","Distress Sources","Distress Count",
                "Hot Stack","Absentee Owner","Out-of-State Owner","Inherited",
                "Assessed Value","Est Market Value","Est Equity","Est Arrears","Est Payoff","Mortgage Signals",
                "Parcel ID","LUC Code","Match Method","Source","Public Records URL"]
    with csv_path.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fieldnames);w.writeheader()
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
                "Est Equity":f"${r.est_equity:,.0f}" if r.est_equity is not None else "",
                "Est Arrears":f"${r.est_arrears:,.0f}" if r.est_arrears else "",
                "Est Payoff":f"${r.est_payoff:,.0f}" if r.est_payoff else "",
                "Mortgage Signals":"; ".join(r.mortgage_signals),
                "Parcel ID":r.parcel_id,"LUC Code":r.luc,
                "Match Method":r.match_method,
                "Source":SOURCE_NAME,"Public Records URL":r.clerk_url,
            })
    logging.info("Wrote CSV: %s",csv_path)

# ── Main ──────────────────────────────────────────────────────────────────
async def main():
    args_p=argparse.ArgumentParser()
    args_p.add_argument("--out-csv",default=str(DEFAULT_ENRICHED_CSV_PATH))
    args=args_p.parse_args()
    ensure_dirs();log_setup()
    logging.info("=== Toledo / Lucas County — Motivated Seller Intelligence ===")

    # 1. Load parcel data (owner + address + value index)
    parcels=load_parcel_data()

    # 2. Scrape all sources
    all_records:List[LeadRecord]=[]

    sheriff    = await scrape_tln_sheriff_sales()
    foreclos   = await scrape_tln_foreclosures()
    liens      = await scrape_tln_liens()
    common_pl  = await scrape_tln_common_pleas()
    probate    = await scrape_tln_probate()
    divorce    = await scrape_tln_divorce()
    tax_delin  = await scrape_lucas_tax_delinquent()

    all_records = sheriff+foreclos+liens+common_pl+probate+divorce+tax_delin
    logging.info("Total before enrich: %s",len(all_records))

    # 3. Enrich with parcel data
    enriched=[]
    for r in all_records:
        try:
            r=enrich_lead(r,parcels)
            enriched.append(r)
        except Exception as e:
            logging.warning("Enrich failed %s: %s",r.doc_num,e)
            enriched.append(r)
    all_records=enriched

    # 4. Cross-stack + dedupe + sort
    all_records=cross_stack_by_address(all_records)
    all_records=dedupe_records(all_records)
    all_records.sort(
        key=lambda r:(r.doc_type=="SHERIFF",r.hot_stack,r.distress_count,r.subject_to_score,r.score,r.filed),
        reverse=True
    )
    logging.info("Total after dedupe: %s",len(all_records))

    # 5. Write outputs
    write_json_outputs(all_records)
    write_category_json(all_records)
    write_csv(all_records,DEFAULT_OUTPUT_CSV_PATH)
    if Path(args.out_csv)!=DEFAULT_OUTPUT_CSV_PATH:
        write_csv(all_records,Path(args.out_csv))

    logging.info(
        "=== DONE === Total:%s | ⚡Sheriff:%s | 🔥HotStack:%s | ⚖️Probate:%s | "
        "⚠️Foreclosure:%s | 🔗Liens:%s | 💰TaxDelin:%s | 📭Absentee:%s | 🌎OOS:%s | "
        "🎯SubjectTo:%s | ⭐PrimeSubTo:%s | 💔Divorce:%s",
        len(all_records),
        sum(1 for r in all_records if r.doc_type=="SHERIFF"),
        sum(1 for r in all_records if r.hot_stack),
        sum(1 for r in all_records if r.doc_type=="PRO"),
        sum(1 for r in all_records if r.doc_type in{"NOFC","LP"}),
        sum(1 for r in all_records if r.doc_type in{"LN","LNMECH","LNFED"}),
        sum(1 for r in all_records if r.doc_type=="TAX"),
        sum(1 for r in all_records if r.is_absentee),
        sum(1 for r in all_records if r.is_out_of_state),
        sum(1 for r in all_records if r.subject_to_score>=50),
        sum(1 for r in all_records if r.subject_to_score>=70),
        sum(1 for r in all_records if r.doc_type=="DIVORCE"),
    )

if __name__=="__main__":
    asyncio.run(main())
