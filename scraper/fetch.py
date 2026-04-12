"""
DROP-IN REPLACEMENT for Lucas County parcel loading in fetch.py
================================================================
Replace your existing `load_lucas_parcel_data()` function AND
`build_parcel_indexes()` function with the code below.

The old code tried to pull owner/address from ArcGIS layers that
only contain geometry (AREA_NUM, BLOCK_NUM, ACREAGE) — no owner,
no address. That's why parcel_data: 0 addresses indexed.

This new approach uses the Lucas County AREIS iasWorld public
search at icare.co.lucas.oh.us to look up each lead by address
and get owner name, mailing address, land use code, and value.

HOW TO USE:
1. Find the old load_lucas_parcel_data() in your fetch.py and
   replace the entire function with the one below.
2. Find the old build_parcel_indexes() and replace it too.
3. In your main() function, change the call from:
      parcel_index = load_lucas_parcel_data()
   to:
      parcel_index = {}   # populated lazily by enrich_record_areis()
   Then after scraping, call:
      all_records = enrich_all_records_areis(all_records)
"""

import re
import time
import logging
import asyncio
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
AREIS_BASE      = "https://icare.co.lucas.oh.us/lucascare"
AREIS_ADDR_URL  = f"{AREIS_BASE}/search/commonsearch.aspx?mode=address"
AREIS_OWNER_URL = f"{AREIS_BASE}/search/commonsearch.aspx?mode=owner"
AREIS_PARID_URL = f"{AREIS_BASE}/search/commonsearch.aspx?mode=parid"
AREIS_DETAIL    = f"{AREIS_BASE}/search/commonsearch.aspx?mode=detail"

AREIS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": AREIS_BASE,
}

# Land use codes that indicate VACANT land (no structure)
VACANT_LAND_LUCS = {
    "400", "401", "402", "403", "404", "405", "406", "407", "408", "409",
    "500", "501", "502", "503", "504", "505", "506", "510", "511", "512",
    "550", "551", "552", "553", "554", "555",
    "700", "701", "702", "703", "704", "705",
    "800", "801", "802", "803", "880", "881",
}

# ─────────────────────────────────────────────────────────────────────────────
# SESSION  (shared across all lookups)
# ─────────────────────────────────────────────────────────────────────────────
_areis_session: Optional[requests.Session] = None

def _get_areis_session() -> requests.Session:
    global _areis_session
    if _areis_session is None:
        _areis_session = requests.Session()
        _areis_session.headers.update(AREIS_HEADERS)
        # Warm up — grab homepage to get any cookies/viewstate
        try:
            _areis_session.get(AREIS_BASE + "/main/homepage.aspx", timeout=15)
        except Exception:
            pass
    return _areis_session


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _clean(s) -> str:
    if s is None:
        return ""
    return " ".join(str(s).split()).strip()


def _normalize_state(s: str) -> str:
    """Return 2-letter state abbreviation or empty string."""
    s = _clean(s).upper()
    if len(s) == 2 and s.isalpha():
        return s
    state_map = {
        "OHIO": "OH", "MICHIGAN": "MI", "INDIANA": "IN",
        "FLORIDA": "FL", "TEXAS": "TX", "GEORGIA": "GA",
        "CALIFORNIA": "CA", "PENNSYLVANIA": "PA", "NEW YORK": "NY",
    }
    return state_map.get(s, "")


def _parse_street_num(address: str) -> tuple:
    """Split '2175 Aberdeen' into ('2175', 'ABERDEEN')."""
    address = _clean(address).upper()
    m = re.match(r"^(\d+[A-Z]?)\s+(.*)", address)
    if m:
        return m.group(1), m.group(2).strip()
    return "", address


def _is_absentee(prop_addr: str, mail_addr: str, mail_city: str, mail_state: str) -> bool:
    """
    True if mailing address is meaningfully different from property address.
    Handles PO Boxes, out-of-state, and different street addresses.
    """
    if not mail_addr:
        return False
    mail_up = _clean(mail_addr).upper()
    prop_up = _clean(prop_addr).upper()

    # PO Box is always absentee
    if mail_up.startswith("PO BOX") or mail_up.startswith("P.O. BOX"):
        return True

    # Out of state mailing = absentee
    if mail_state and mail_state.upper() not in ("OH", ""):
        return True

    # Different city = absentee (owner in Maumee but property in Toledo, etc.)
    if mail_city and mail_city.upper() not in ("TOLEDO", ""):
        prop_city_guess = "TOLEDO"  # most Lucas County props are Toledo
        if mail_city.upper() != prop_city_guess:
            return True

    # Compare street numbers
    prop_num, prop_street = _parse_street_num(prop_up)
    mail_num, mail_street = _parse_street_num(mail_up)

    if prop_num and mail_num and prop_num != mail_num:
        return True
    if prop_street and mail_street:
        # Normalize common abbreviations
        for abbr, full in [("ST", "STREET"), ("AVE", "AVENUE"), ("DR", "DRIVE"),
                           ("RD", "ROAD"), ("LN", "LANE"), ("BLVD", "BOULEVARD")]:
            prop_street = prop_street.replace(f" {abbr}", f" {full}")
            mail_street = mail_street.replace(f" {abbr}", f" {full}")
        if prop_street != mail_street:
            return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# AREIS LOOKUP  — by owner name (for records that have owner but no address)
# ─────────────────────────────────────────────────────────────────────────────
def _areis_lookup_by_owner(owner_name: str) -> list:
    """
    Search AREIS by owner name. Returns list of result dicts.
    Each dict has: parcel_id, prop_address, prop_city, owner_name,
                   mail_address, mail_city, mail_state, mail_zip,
                   luc, acres, est_market_value
    """
    if not owner_name or len(owner_name) < 3:
        return []

    sess = _get_areis_session()

    # AREIS owner search uses a POST with owner name
    # Last name goes in 'ownerlast', first in 'ownerfirst'
    # For "COMSTOCK KELLIE ANN" style (all caps, last first), split on first space
    parts = owner_name.upper().strip().split()
    last = parts[0] if parts else owner_name
    first = parts[1] if len(parts) > 1 else ""

    try:
        resp = sess.post(
            AREIS_OWNER_URL,
            data={
                "ownerlast": last,
                "ownerfirst": first,
                "searchType": "owner",
                "btnSearch": "Search",
            },
            timeout=20,
        )
        return _parse_areis_results(resp.text)
    except Exception as e:
        logging.debug("AREIS owner lookup failed for %s: %s", owner_name, e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# AREIS LOOKUP  — by address
# ─────────────────────────────────────────────────────────────────────────────
def _areis_lookup_by_address(street_num: str, street_name: str) -> list:
    """
    Search AREIS by property address. Returns list of result dicts.
    """
    if not street_num or not street_name:
        return []

    sess = _get_areis_session()
    try:
        resp = sess.post(
            AREIS_ADDR_URL,
            data={
                "stno": street_num,
                "stname": street_name,
                "searchType": "address",
                "btnSearch": "Search",
            },
            timeout=20,
        )
        return _parse_areis_results(resp.text)
    except Exception as e:
        logging.debug("AREIS addr lookup failed %s %s: %s", street_num, street_name, e)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# AREIS RESULT PARSER
# ─────────────────────────────────────────────────────────────────────────────
def _parse_areis_results(html: str) -> list:
    """
    Parse the AREIS search results table.
    Returns list of dicts with parcel data.
    """
    results = []
    if not html or len(html) < 200:
        return results

    soup = BeautifulSoup(html, "lxml")

    # Results are in a table — find rows with parcel links
    for row in soup.select("table tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 4:
            continue
        texts = [_clean(c.get_text()) for c in cells]

        # Skip header rows
        if any(t.lower() in ("parcel id", "owner", "address", "land use") for t in texts):
            continue

        # A valid result row has a parcel ID (digits + dashes)
        parcel_id = ""
        for t in texts:
            if re.match(r"^\d{2}-\d{6}-\d{3}-\d{3}$", t):
                parcel_id = t
                break
            # Also match just digits
            if re.match(r"^\d{14,17}$", t.replace("-", "")):
                parcel_id = t
                break

        if not parcel_id:
            continue

        # Extract the link to detail page
        detail_link = None
        for a in row.find_all("a", href=True):
            if "detail" in a["href"].lower() or "parid" in a["href"].lower():
                detail_link = a["href"]
                break

        # Build basic record from row cells
        record = {
            "parcel_id": parcel_id,
            "prop_address": "",
            "prop_city": "",
            "owner_name": "",
            "mail_address": "",
            "mail_city": "",
            "mail_state": "OH",
            "mail_zip": "",
            "luc": "",
            "acres": "",
            "est_market_value": None,
            "detail_url": detail_link,
        }

        # Try to map cells: typical order is
        # ParcelID | Owner | Address | LandUse | TotalValue
        if len(texts) >= 2:
            record["owner_name"] = texts[1]
        if len(texts) >= 3:
            record["prop_address"] = texts[2]
        if len(texts) >= 4:
            record["luc"] = texts[3]
        if len(texts) >= 5:
            val_str = texts[4].replace("$", "").replace(",", "").strip()
            try:
                record["est_market_value"] = float(val_str)
            except ValueError:
                pass

        results.append(record)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# AREIS DETAIL PAGE — gets mailing address (not in search results)
# ─────────────────────────────────────────────────────────────────────────────
def _areis_get_detail(parcel_id: str) -> dict:
    """
    Fetch the AREIS detail page for a parcel to get mailing address,
    exact owner name, land use code, and assessed value.
    """
    sess = _get_areis_session()
    detail = {
        "owner_name": "",
        "mail_address": "",
        "mail_city": "",
        "mail_state": "OH",
        "mail_zip": "",
        "prop_address": "",
        "prop_city": "Toledo",
        "luc": "",
        "acres": "",
        "est_market_value": None,
        "assessed_value": None,
    }

    try:
        resp = sess.get(
            f"{AREIS_BASE}/search/commonsearch.aspx?mode=detail&parid={parcel_id}",
            timeout=20,
        )
        html = resp.text
        if not html or len(html) < 500:
            return detail

        soup = BeautifulSoup(html, "lxml")

        # AREIS detail page has labeled rows like:
        # "Owner Name:" | "SMITH JOHN"
        # "Mailing Address:" | "123 MAIN ST"
        # "Mail City:" | "TOLEDO"
        # "Property Address:" | "456 OAK AVE"
        # "Land Use:" | "510 - Single Family"
        # "Assessed Value:" | "$45,000"
        # "Est. Market Value:" | "$128,571"

        label_map = {
            "owner name":        "owner_name",
            "owner":             "owner_name",
            "mailing address":   "mail_address",
            "mail address":      "mail_address",
            "mail city":         "mail_city",
            "mailing city":      "mail_city",
            "mail state":        "mail_state",
            "mailing state":     "mail_state",
            "mail zip":          "mail_zip",
            "mailing zip":       "mail_zip",
            "property address":  "prop_address",
            "prop address":      "prop_address",
            "property city":     "prop_city",
            "land use":          "luc",
            "land use code":     "luc",
            "acreage":           "acres",
            "acres":             "acres",
        }

        # Scan all table rows for label | value pairs
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                label = _clean(cells[0].get_text()).lower().rstrip(":")
                value = _clean(cells[1].get_text())
                if label in label_map and value:
                    detail[label_map[label]] = value

        # Also try to find value in span/div with specific IDs or classes
        # AREIS uses asp.net controls with predictable IDs
        for field_id, key in [
            ("_lblOwner",         "owner_name"),
            ("_lblMailAddress",   "mail_address"),
            ("_lblMailCity",      "mail_city"),
            ("_lblMailState",     "mail_state"),
            ("_lblMailZip",       "mail_zip"),
            ("_lblPropAddress",   "prop_address"),
            ("_lblLandUse",       "luc"),
            ("_lblAcreage",       "acres"),
        ]:
            el = soup.find(id=lambda x: x and field_id in x)
            if el:
                val = _clean(el.get_text())
                if val:
                    detail[key] = val

        # Parse assessed / market values from the page text
        text = soup.get_text(" ")
        for pattern, key in [
            (r"(?:Est\.?\s*Market\s*Value|EMV)[:\s]+\$?([\d,]+)", "est_market_value"),
            (r"(?:Assessed\s*Value|Total\s*Assessed)[:\s]+\$?([\d,]+)", "assessed_value"),
            (r"(?:Total\s*Value|Market\s*Value)[:\s]+\$?([\d,]+)", "est_market_value"),
        ]:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                try:
                    v = float(m.group(1).replace(",", ""))
                    if v > 0:
                        detail[key] = v
                except ValueError:
                    pass

        # If we got assessed but not market, estimate (Ohio = 35% ratio)
        if detail["assessed_value"] and not detail["est_market_value"]:
            detail["est_market_value"] = round(detail["assessed_value"] / 0.35)

        # Normalize state
        detail["mail_state"] = _normalize_state(detail["mail_state"]) or "OH"

    except Exception as e:
        logging.debug("AREIS detail failed for %s: %s", parcel_id, e)

    return detail


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENRICHMENT FUNCTION — call this after scraping all records
# ─────────────────────────────────────────────────────────────────────────────
def enrich_all_records_areis(records: list, max_workers: int = 4) -> list:
    """
    Enrich all lead records with AREIS parcel data.
    For each record:
      1. Parse street number + name from record's prop_address
      2. Search AREIS to confirm/get owner name
      3. Fetch detail page for mailing address
      4. Set is_absentee, is_out_of_state, is_vacant_land flags

    Records are updated IN PLACE and returned.
    Uses a small delay between requests to be polite.
    """
    total = len(records)
    logging.info("AREIS enrichment: %d records to process", total)

    enriched = 0
    absentee = 0
    oos = 0
    vacant = 0

    # Cache to avoid re-fetching same parcel
    cache: dict = {}

    for i, rec in enumerate(records):
        if i % 50 == 0:
            logging.info("AREIS enrichment: %d/%d done | absentee=%d oos=%d vacant=%d",
                         i, total, absentee, oos, vacant)

        # Get address from record (try multiple field names)
        prop_addr = _clean(
            getattr(rec, "prop_address", None)
            or rec.get("prop_address", "") if isinstance(rec, dict) else ""
        )

        owner = _clean(
            getattr(rec, "owner", None)
            or rec.get("owner", "") if isinstance(rec, dict) else ""
        )

        if not prop_addr and not owner:
            continue

        # Parse address into number + street
        street_num, street_name = _parse_street_num(prop_addr)

        # Check cache first (keyed by address)
        cache_key = f"{street_num}|{street_name}" if street_num else f"owner|{owner[:30]}"
        if cache_key in cache:
            detail = cache[cache_key]
        else:
            # Search by address first, fall back to owner name
            detail = None
            parcel_id = None

            if street_num and street_name:
                results = _areis_lookup_by_address(street_num, street_name)
                if results:
                    parcel_id = results[0].get("parcel_id")

            if not parcel_id and owner:
                results = _areis_lookup_by_owner(owner)
                if results:
                    # Try to match by address
                    for r in results:
                        if street_num and street_num in _clean(r.get("prop_address", "")):
                            parcel_id = r.get("parcel_id")
                            break
                    if not parcel_id and results:
                        parcel_id = results[0].get("parcel_id")

            if parcel_id:
                detail = _areis_get_detail(parcel_id)
                time.sleep(0.4)  # polite delay
            else:
                detail = {}

            cache[cache_key] = detail

        if not detail:
            continue

        # Apply enrichment to record
        def _set(field, value):
            if isinstance(rec, dict):
                if not rec.get(field):
                    rec[field] = value
            else:
                if not getattr(rec, field, None):
                    try:
                        setattr(rec, field, value)
                    except AttributeError:
                        pass

        def _get(field):
            if isinstance(rec, dict):
                return rec.get(field, "")
            return getattr(rec, field, "") or ""

        if detail.get("owner_name"):
            _set("owner", detail["owner_name"])
        if detail.get("prop_address"):
            _set("prop_address", detail["prop_address"])
        if detail.get("prop_city"):
            _set("prop_city", detail["prop_city"])
        if detail.get("mail_address"):
            _set("mail_address", detail["mail_address"])
        if detail.get("mail_city"):
            _set("mail_city", detail["mail_city"])
        if detail.get("mail_state"):
            _set("mail_state", detail["mail_state"])
        if detail.get("mail_zip"):
            _set("mail_zip", detail["mail_zip"])
        if detail.get("luc"):
            _set("luc", detail["luc"])
        if detail.get("acres"):
            _set("acres", detail["acres"])
        if detail.get("est_market_value"):
            _set("est_market_value", detail["est_market_value"])
        if detail.get("assessed_value"):
            _set("assessed_value", detail["assessed_value"])
        if detail.get("parcel_id"):
            _set("parcel_id", detail.get("parcel_id", ""))

        enriched += 1

        # Now apply flags
        mail_addr = _get("mail_address")
        mail_city = _get("mail_city")
        mail_state = _get("mail_state")
        luc = _get("luc").split("-")[0].strip() if _get("luc") else ""

        # Absentee owner
        is_absentee = _is_absentee(prop_addr, mail_addr, mail_city, mail_state)
        if isinstance(rec, dict):
            rec["is_absentee"] = is_absentee
        else:
            try:
                rec.is_absentee = is_absentee
            except AttributeError:
                pass

        if is_absentee:
            absentee += 1
            flags = _get("flags") or []
            if isinstance(flags, list) and "Absentee owner" not in flags:
                flags.append("Absentee owner")
                if isinstance(rec, dict):
                    rec["flags"] = flags
                else:
                    try:
                        rec.flags = flags
                    except AttributeError:
                        pass

        # Out of state
        is_oos = mail_state not in ("OH", "", "0", "3")
        if is_oos:
            oos += 1
            if isinstance(rec, dict):
                rec["is_out_of_state"] = True
            else:
                try:
                    rec.is_out_of_state = True
                except AttributeError:
                    pass
            flags = _get("flags") or []
            if isinstance(flags, list) and "Out of state owner" not in flags:
                flags.append("Out of state owner")
                if isinstance(rec, dict):
                    rec["flags"] = flags
                else:
                    try:
                        rec.flags = flags
                    except AttributeError:
                        pass

        # Vacant land
        is_vacant = luc in VACANT_LAND_LUCS or luc.startswith("5") or luc.startswith("4")
        if is_vacant:
            vacant += 1
            if isinstance(rec, dict):
                rec["is_vacant_land"] = True
            else:
                try:
                    rec.is_vacant_land = True
                except AttributeError:
                    pass

    logging.info(
        "AREIS enrichment complete: %d/%d enriched | absentee=%d | out-of-state=%d | vacant=%d",
        enriched, total, absentee, oos, vacant
    )
    return records


# ─────────────────────────────────────────────────────────────────────────────
# REPLACEMENT build_parcel_indexes() for Lucas County
# ─────────────────────────────────────────────────────────────────────────────
def build_parcel_indexes_lucas():
    """
    Lucas County doesn't have downloadable CAMA files.
    Returns empty indexes — enrichment happens lazily via AREIS
    after all records are scraped.

    Replace your old build_parcel_indexes() call with this,
    then call enrich_all_records_areis(all_records) after scraping.
    """
    logging.info("Lucas County: skipping ArcGIS parcel pull (no owner/addr fields).")
    logging.info("Owner/address enrichment will happen via AREIS after scraping.")

    # Return empty structures matching what the rest of the code expects
    owner_index = {}         # last_name -> list of parcel dicts
    last_name_index = {}
    first_last_index = {}
    parcel_rows = []
    mail_by_pid = {}

    return owner_index, last_name_index, first_last_index, parcel_rows, mail_by_pid


# ─────────────────────────────────────────────────────────────────────────────
# WHERE TO ADD enrich_all_records_areis() IN YOUR main()
# ─────────────────────────────────────────────────────────────────────────────
"""
In your main() function, find the section after all scrapers finish
and before writing output files. It probably looks like:

    all_records = clerk_records + probate_records + ...
    all_records, report = enrich_with_parcel_data(all_records, ...)
    all_records = apply_distress_stacking(all_records, ...)
    all_records = dedupe_records(all_records)

Change it to:

    all_records = clerk_records + probate_records + ...

    # NEW: AREIS enrichment replaces the old parcel lookup
    all_records = enrich_all_records_areis(all_records)

    all_records = apply_distress_stacking(all_records, ...)
    all_records = dedupe_records(all_records)

That's it. The AREIS enricher handles owner, address, mailing,
absentee, out-of-state, and vacant flags all in one pass.
"""
