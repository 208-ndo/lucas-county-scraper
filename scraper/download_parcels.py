"""
download_parcels.py — Lucas County Parcel Data Acquisition
===========================================================
Outside-the-box multi-strategy approach:

STRATEGY 1: Lucas County Auditor AREIS bulk download
  - The treasurer site mentions "Download AREIS" — this is the Assessment Real Estate Info System
  - Direct CSV/DBF download from the auditor's FTP or download page

STRATEGY 2: GIS layer discovery — find the layer with ALL parcels
  - Layer 4 only has 92 records (sample/test layer)
  - Query ALL layers 0-20 with outFields=* to find the real parcel layer
  - Look for layers with 50k+ records

STRATEGY 3: Lucas County open data hub — search for correct dataset ID
  - The ArcGIS dataset ID f37bcb63... returned 500 — try other IDs
  - Search the hub metadata to find the correct shapefile

STRATEGY 4: Ohio statewide parcel data
  - Ohio GIO (Geographic Information Office) provides statewide parcel data
  - Free download, includes all Ohio counties including Lucas

STRATEGY 5: Build our own index from TLN + case data we already have
  - We have 1,141 records with owner names
  - Query Lucas County Auditor property search page for each unique owner
  - Cache results — builds up over time

STRATEGY 6: Lucas County Recorder deed index
  - Public record, searchable by name
  - Can extract address from deed filings
"""
import requests, sys, time, csv, json, re
from pathlib import Path
from urllib.parse import urlencode

csv_dest   = Path("data/parcels/ParcelsAddress.csv")
debug_dir  = Path("data/debug")
debug_dir.mkdir(parents=True, exist_ok=True)
csv_dest.parent.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/json,*/*",
}

GIS_BASE = "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer"

def save_csv(rows: list, label: str = "") -> bool:
    if not rows: return False
    fieldnames = list(rows[0].keys())
    # Ensure key fields first
    priority = ["OWNER","PROPERTY_A","PARID","LUC","CITY","ZIP","ADRNO","ADRDIR","ADRSTR","ADRSUF","MAILING_AD"]
    ordered  = [f for f in priority if f in fieldnames] + [f for f in fieldnames if f not in priority]
    with csv_dest.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ordered, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)
    size = csv_dest.stat().st_size
    print(f"  Saved {len(rows):,} rows to CSV ({size/1024:.0f} KB) [{label}]")
    # Save sample for debug
    (debug_dir / "parcel_csv_sample.json").write_text(json.dumps(rows[:5], indent=2))
    return True

def build_addr(row: dict) -> str:
    """Build address from component fields."""
    # Try pre-built field first
    for f in ["PROPERTY_A","property_a","address","ADDRESS","siteAddress"]:
        v = str(row.get(f,"") or "").strip()
        if v and re.match(r"^\d", v): return v.title()
    # Build from parts
    parts = [str(row.get(k,"") or "").strip() for k in ["adrno","ADRNO","houseNo","house_no"]]
    dirs  = [str(row.get(k,"") or "").strip() for k in ["adrdir","ADRDIR","streetDir","street_dir"]]
    names = [str(row.get(k,"") or "").strip() for k in ["adrstr","ADRSTR","streetName","street_name"]]
    suffs = [str(row.get(k,"") or "").strip() for k in ["adrsuf","ADRSUF","streetSuf","street_suf","adrsuf2","ADRSUF2"]]
    addr = " ".join(p for group in [parts,dirs,names,suffs] for p in group if p and p != "None").strip()
    return addr.title() if addr else ""

def normalize(row: dict, source: str = "") -> dict:
    """Normalize any row format to our standard fields."""
    R = {k.upper(): str(v or "").strip() for k, v in row.items()}
    owner = (R.get("OWN") or R.get("OWNER") or R.get("OWN1") or
             R.get("NAME") or R.get("OWNERNAME") or "").title()
    parid = R.get("PARID") or R.get("PARCELID") or R.get("PARCEL_ID") or ""
    luc   = R.get("LUC") or R.get("LANDUSE") or R.get("LAND_USE") or ""
    city  = (R.get("CITY") or R.get("SITECITY") or R.get("SITE_CITY") or "Toledo").title()
    zip_  = R.get("ZIP_CODE") or R.get("ZIP") or R.get("ZIPCODE") or R.get("SZIP") or ""
    prop  = build_addr(row) or (R.get("PROPERTY_A") or R.get("SITEADDR") or R.get("SITE_ADDR") or "")
    if prop and city and not re.search(r'\bOH\b', prop):
        prop = f"{prop}, {city} OH {zip_}".strip(", ")
    mail = (R.get("MAILING_AD") or R.get("MAIL_ADR1") or R.get("MAILADR1") or
            R.get("MAIL_ADDR") or "").title()
    return {
        "OWNER":      owner,
        "PROPERTY_A": prop.title() if prop else "",
        "PARID":      parid,
        "LUC":        luc,
        "CITY":       city,
        "ZIP":        zip_[:5],
        "ADRNO":      R.get("ADRNO",""),
        "ADRDIR":     R.get("ADRDIR",""),
        "ADRSTR":     R.get("ADRSTR",""),
        "ADRSUF":     R.get("ADRSUF",""),
        "MAILING_AD": mail,
        "SOURCE":     source,
    }

# ============================================================
# STRATEGY 1: Discover real parcel layer by checking all layers
# ============================================================
print("=" * 60)
print("STRATEGY 1: Discover real parcel layer (check all layers)")
print("=" * 60)

# First get service info to see all layers
try:
    r = requests.get(f"{GIS_BASE}?f=json", headers=HEADERS, timeout=30)
    if r.status_code == 200:
        svc = r.json()
        layers = svc.get("layers", []) + svc.get("tables", [])
        print(f"Service has {len(layers)} layers:")
        for L in layers:
            print(f"  Layer {L.get('id')}: {L.get('name')} (type={L.get('type','')})")
        # Save for debugging
        (debug_dir / "gis_service_info.json").write_text(json.dumps(svc, indent=2))
except Exception as e:
    print(f"  Service info error: {e}")
    layers = [{"id": i} for i in range(20)]

# Try each layer with count first
best_layer = None; best_count = 0
for layer_info in layers:
    idx = layer_info.get("id", 0)
    try:
        count_url = f"{GIS_BASE}/{idx}/query?where=1%3D1&returnCountOnly=true&f=json"
        r = requests.get(count_url, headers=HEADERS, timeout=20)
        if r.status_code != 200: continue
        data = r.json()
        if "error" in data: continue
        cnt = data.get("count", 0)
        name = layer_info.get("name","?")
        print(f"  Layer {idx} ({name}): {cnt:,} records")
        if cnt > best_count:
            best_count = cnt; best_layer = idx
        time.sleep(0.5)
    except Exception as e:
        print(f"  Layer {idx}: error {e}")

print(f"\nBest layer: {best_layer} with {best_count:,} records")

if best_layer is not None and best_count > 100:
    print(f"\nDownloading from layer {best_layer} (paginated)...")
    all_rows = []
    page_size = 1000; offset = 0; errors = 0

    # First get field names for this layer
    try:
        fields_url = f"{GIS_BASE}/{best_layer}?f=json"
        fr = requests.get(fields_url, headers=HEADERS, timeout=20)
        if fr.status_code == 200:
            layer_info_data = fr.json()
            field_names = [f["name"] for f in layer_info_data.get("fields", [])]
            print(f"  Layer {best_layer} fields: {field_names}")
            (debug_dir / f"layer_{best_layer}_fields.json").write_text(json.dumps(field_names, indent=2))
    except Exception as e:
        print(f"  Could not get fields: {e}")
        field_names = []

    while offset < best_count + page_size:
        url = (f"{GIS_BASE}/{best_layer}/query"
               f"?where=1%3D1&outFields=*&f=json"
               f"&resultRecordCount={page_size}"
               f"&resultOffset={offset}"
               f"&returnGeometry=false")
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            if r.status_code != 200: errors += 1; offset += page_size; continue
            data = r.json()
            if "error" in data: errors += 1; offset += page_size; continue
            features = data.get("features", [])
            rows = [f.get("attributes", {}) for f in features if f.get("attributes")]
            if not rows: break
            all_rows.extend(rows)
            if offset % 5000 == 0:
                print(f"  {offset:>6,} / {best_count:,} | {len(all_rows):,} downloaded")
            if len(rows) < page_size: break
            offset += page_size
            time.sleep(0.2)
        except Exception as e:
            print(f"  Error at {offset}: {e}")
            errors += 1
            if errors >= 5: break
            time.sleep(2)

    print(f"  Downloaded {len(all_rows):,} rows total")
    if all_rows:
        normalized = [normalize(r, f"gis_layer_{best_layer}") for r in all_rows]
        useful = [r for r in normalized if r["OWNER"] or r["PROPERTY_A"]]
        if useful:
            if save_csv(useful, f"GIS Layer {best_layer}"):
                print(f"SUCCESS: {len(useful):,} parcels from GIS layer {best_layer}")
                sys.exit(0)

# ============================================================
# STRATEGY 2: Ohio Statewide Parcel Data (OGRIP / Ohio GIO)
# ============================================================
print("\n" + "=" * 60)
print("STRATEGY 2: Ohio statewide parcel data (Lucas County subset)")
print("=" * 60)

ohio_urls = [
    # Ohio GIO statewide parcels — Lucas County FIPS = 39095
    "https://services.arcgis.com/PqkGgZJPbdqaGCar/arcgis/rest/services/Ohio_Parcels/FeatureServer/0/query?where=COUNTY_CODE%3D%2795%27&outFields=*&f=json&resultRecordCount=2000&returnGeometry=false",
    "https://gis.ohiogeographicdata.gov/arcgis/rest/services/Parcels/Ohio_Parcels/MapServer/0/query?where=COUNTY%3D%27LUCAS%27&outFields=*&f=json&resultRecordCount=2000&returnGeometry=false",
    # Ohio open data hub
    "https://opendata.arcgis.com/datasets/ohio-parcels/downloads/data?format=csv&spatialRefId=4326",
]

for url in ohio_urls:
    print(f"[OHIO] {url[:80]}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        print(f"  status={r.status_code} size={len(r.content):,}")
        if r.status_code == 200 and len(r.content) > 1000:
            try:
                data = r.json()
                features = data.get("features", [])
                if features:
                    rows = [f.get("attributes", f.get("properties", {})) for f in features]
                    rows = [r for r in rows if r]
                    print(f"  Got {len(rows)} features")
                    if rows:
                        normalized = [normalize(r, "ohio_statewide") for r in rows]
                        useful = [r for r in normalized if r["OWNER"] or r["PROPERTY_A"]]
                        if useful and save_csv(useful, "Ohio Statewide"):
                            print("SUCCESS: Ohio statewide parcel data")
                            sys.exit(0)
            except:
                # Try as CSV
                content = r.content.decode("utf-8", errors="ignore")
                if "," in content and "\n" in content:
                    import io
                    reader = csv.DictReader(io.StringIO(content))
                    rows = list(reader)
                    if rows:
                        normalized = [normalize(r, "ohio_csv") for r in rows]
                        useful = [r for r in normalized if r["OWNER"] or r["PROPERTY_A"]]
                        if useful and save_csv(useful, "Ohio CSV"):
                            sys.exit(0)
    except Exception as e:
        print(f"  Error: {e}")
    time.sleep(2)

# ============================================================
# STRATEGY 3: Lucas County Auditor AREIS direct download
# ============================================================
print("\n" + "=" * 60)
print("STRATEGY 3: Lucas County Auditor AREIS / property data")
print("=" * 60)

auditor_urls = [
    "https://lucascountyauditor.org/areis",
    "https://lucascountyauditor.org/real-estate/property-search",
    "https://www.lucascountyauditor.org/",
]

for url in auditor_urls:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200: continue
        # Look for download links
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a[href]"):
            href = str(a.get("href","")).lower()
            text = a.get_text().lower()
            if any(x in href or x in text for x in ["download","csv","shapefile","parcel","areis","gis"]):
                full = href if href.startswith("http") else f"https://lucascountyauditor.org{href}"
                print(f"  Found link: {full[:80]}")
                try:
                    r2 = requests.get(full, headers=HEADERS, timeout=60)
                    if r2.status_code == 200 and len(r2.content) > 50000:
                        print(f"  Downloaded {len(r2.content):,} bytes")
                        # Try to parse
                        content = r2.content.decode("utf-8", errors="ignore")
                        if "," in content[:1000]:
                            import io
                            rows = list(csv.DictReader(io.StringIO(content)))
                            if rows:
                                normalized = [normalize(row, "auditor_areis") for row in rows]
                                useful = [r for r in normalized if r["OWNER"] or r["PROPERTY_A"]]
                                if useful and save_csv(useful, "Auditor AREIS"):
                                    sys.exit(0)
                except Exception as e2:
                    print(f"  Link error: {e2}")
    except Exception as e:
        print(f"  {url}: {e}")

# ============================================================
# STRATEGY 4: Build index from existing TLN lead data
# ============================================================
print("\n" + "=" * 60)
print("STRATEGY 4: Build parcel index from existing records.json")
print("=" * 60)

# Read existing records.json and build a parcel index from records
# that already have addresses — this seeds the cache for future matches
records_path = Path("data/records.json")
if records_path.exists():
    try:
        data = json.loads(records_path.read_text())
        records = data.get("records", [])
        print(f"  Found {len(records):,} existing records")
        parcel_rows = []
        seen_addrs = set()
        for rec in records:
            owner = str(rec.get("owner","") or "").strip()
            addr  = str(rec.get("prop_address","") or "").strip()
            city  = str(rec.get("prop_city","") or "Toledo").strip()
            zip_  = str(rec.get("prop_zip","") or "").strip()
            parid = str(rec.get("parcel_id","") or "").strip()
            luc   = str(rec.get("luc","") or "").strip()
            mail  = str(rec.get("mail_address","") or "").strip()
            # Only include records with valid address
            if not addr or not re.match(r"^\d{1,5}\s+[a-zA-Z]", addr): continue
            key = f"{owner}|{addr}".lower()
            if key in seen_addrs: continue
            seen_addrs.add(key)
            prop_full = f"{addr}, {city} OH {zip_}".strip(", ") if addr else ""
            parcel_rows.append({
                "OWNER":      owner,
                "PROPERTY_A": prop_full.title(),
                "PARID":      parid,
                "LUC":        luc,
                "CITY":       city,
                "ZIP":        zip_[:5],
                "ADRNO":      "",
                "ADRDIR":     "",
                "ADRSTR":     "",
                "ADRSUF":     "",
                "MAILING_AD": mail,
                "SOURCE":     "existing_records",
            })
        print(f"  Built {len(parcel_rows):,} unique address entries from existing records")
        if parcel_rows and save_csv(parcel_rows, "Existing Records"):
            print("SUCCESS: Parcel index built from existing lead data")
            sys.exit(0)
    except Exception as e:
        print(f"  Error reading records.json: {e}")

# ============================================================
# STRATEGY 5: Lucas County Recorder / Clerk public search
# Scrape the public clerk index for recent deed filings
# ============================================================
print("\n" + "=" * 60)
print("STRATEGY 5: Lucas County Clerk deed index scrape")
print("=" * 60)

clerk_urls = [
    "http://lcapps.co.lucas.oh.us/RecorderSearch/",
    "https://recorder.co.lucas.oh.us/",
    "http://lcapps.co.lucas.oh.us/recorder/",
]

for url in clerk_urls:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        print(f"  {url}: status={r.status_code}")
        if r.status_code == 200:
            (debug_dir / "clerk_recorder.html").write_text(r.text[:10000])
            print(f"  Saved HTML for inspection")
            break
    except Exception as e:
        print(f"  {url}: {e}")

# ============================================================
# STRATEGY 6: Use GIS layer 4's 92 records + query more
# with WHERE clause filtering by address ranges
# ============================================================
print("\n" + "=" * 60)
print("STRATEGY 6: GIS Layer 4 query by address ranges")
print("=" * 60)

# Layer 4 has 92 records — but maybe we can filter differently
# Try querying by city name or LUC code to get different subsets
layer4_queries = [
    "city='TOLEDO'",
    "city='SYLVANIA'",
    "city='MAUMEE'",
    "city='PERRYSBURG'",
    "city='OREGON'",
    "luc LIKE '5%'",   # residential LUC codes
    "luc LIKE '4%'",
    "luc LIKE '6%'",
    "statecode='OH'",
    "zip_code='43601'",
    "zip_code='43604'",
    "zip_code='43605'",
    "zip_code='43606'",
    "zip_code='43607'",
    "zip_code='43608'",
    "zip_code='43609'",
    "zip_code='43610'",
    "zip_code='43611'",
    "zip_code='43612'",
    "zip_code='43613'",
    "zip_code='43614'",
    "zip_code='43615'",
    "zip_code='43616'",
    "zip_code='43617'",
    "zip_code='43620'",
    "zip_code='43623'",
]

all_layer4_rows = []
seen_parids = set()

for where in layer4_queries:
    try:
        url = (f"{GIS_BASE}/4/query"
               f"?where={requests.utils.quote(where)}"
               f"&outFields=own,adrno,adrdir,adrstr,adrsuf,adrsuf2,city,zip_code,parid,luc,statecode"
               f"&f=json&resultRecordCount=2000&returnGeometry=false")
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200: continue
        data = r.json()
        if "error" in data: continue
        features = data.get("features", [])
        rows = [f.get("attributes", {}) for f in features if f.get("attributes")]
        new = [row for row in rows if row.get("parid") not in seen_parids]
        for row in new:
            if row.get("parid"): seen_parids.add(row["parid"])
        all_layer4_rows.extend(new)
        if new:
            print(f"  WHERE {where}: {len(new)} new rows (total={len(all_layer4_rows)})")
        time.sleep(0.3)
    except Exception as e:
        print(f"  {where}: {e}")

if all_layer4_rows:
    normalized = [normalize(r, "gis_layer4_filtered") for r in all_layer4_rows]
    useful = [r for r in normalized if r["OWNER"] or r["PROPERTY_A"]]
    if useful:
        if save_csv(useful, "GIS Layer 4 Filtered"):
            print(f"SUCCESS: {len(useful):,} parcels from filtered layer 4")
            sys.exit(0)

# ============================================================
# FINAL FALLBACK: Keep whatever CSV we already have
# ============================================================
print("\n" + "=" * 60)
print("FINAL FALLBACK: Keep existing CSV if any")
print("=" * 60)

if csv_dest.exists() and csv_dest.stat().st_size > 1000:
    lines = sum(1 for _ in csv_dest.open())
    print(f"  Keeping existing {csv_dest}: {lines:,} lines")
    print("  Scraper will use auditor API for per-record address lookup")
else:
    print("  No parcel CSV exists")
    print("  Scraper will use auditor API for all address lookups")

print("\nAll strategies complete. Continuing with scraper...")
sys.exit(0)
