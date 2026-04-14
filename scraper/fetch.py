"""
download_parcels.py
Downloads Lucas County parcel data using multiple fallback strategies.
Now correctly handles the GIS field names: own, adrno, adrdir, adrstr, adrsuf, city, zip_code, parid, luc
Called by scrape.yml workflow — cached weekly by GitHub Actions.
"""
import requests, zipfile, io, shutil, sys, time, csv, json
from pathlib import Path

dest     = Path("data/parcels/ParcelsAddress.dbf")
csv_dest = Path("data/parcels/ParcelsAddress.csv")
HEADERS  = {"User-Agent": "Mozilla/5.0 (compatible; LucasCountyScraper/3.0)"}

# ── GIS field name → our normalized field name mapping ────────────────────
# These are the ACTUAL field names from the Lucas County GIS REST API
# (confirmed from parcel_fields.json debug file)
FIELD_MAP = {
    # Owner name fields
    "own":       "OWNER",
    "name":      "OWNER",
    "owner":     "OWNER",
    "own1":      "OWNER",
    # Address number
    "adrno":     "ADRNO",
    # Direction prefix (N, S, E, W)
    "adrdir":    "ADRDIR",
    # Street name
    "adrstr":    "ADRSTR",
    # Street suffix (ST, AVE, RD etc)
    "adrsuf":    "ADRSUF",
    "adrsuf2":   "ADRSUF2",
    # City
    "city":      "CITY",
    # ZIP
    "zip_code":  "ZIP",
    "zipcode":   "ZIP",
    "zip":       "ZIP",
    # Parcel ID
    "parid":     "PARID",
    "parcel_id": "PARID",
    "parcelid":  "PARID",
    # Land use code
    "luc":       "LUC",
    # Mailing address (if present)
    "mail_adr1": "MAILING_AD",
    "mailadr1":  "MAILING_AD",
    "mailing_ad":"MAILING_AD",
    "property_a":"PROPERTY_A",
}

def normalize_row(row: dict) -> dict:
    """Map GIS field names to our standard field names."""
    out = {}
    for k, v in row.items():
        mapped = FIELD_MAP.get(k.lower().strip(), k.upper())
        out[mapped] = str(v).strip() if v is not None else ""
    # Build PROPERTY_A from components if not already present
    if "PROPERTY_A" not in out or not out["PROPERTY_A"]:
        parts = [
            out.get("ADRNO",""),
            out.get("ADRDIR",""),
            out.get("ADRSTR",""),
            out.get("ADRSUF",""),
            out.get("ADRSUF2",""),
        ]
        addr = " ".join(p for p in parts if p).strip()
        city = out.get("CITY","")
        zip_ = out.get("ZIP","")
        if addr and city:
            out["PROPERTY_A"] = f"{addr}, {city} OH {zip_}".strip()
        elif addr:
            out["PROPERTY_A"] = addr
    return out

def has_useful_fields(row: dict) -> bool:
    """Check if a row has owner or address data."""
    keys_lower = {k.lower() for k in row.keys()}
    owner_fields = {"own", "name", "owner", "own1", "ownername"}
    addr_fields  = {"adrno", "adrstr", "property_a", "address", "addr"}
    return bool(owner_fields & keys_lower or addr_fields & keys_lower)

def save_csv(rows: list) -> bool:
    """Save normalized rows to CSV."""
    if not rows:
        return False
    # Normalize all rows
    normalized = [normalize_row(r) for r in rows]
    # Get all unique fieldnames
    all_fields = list({k for r in normalized for k in r.keys()})
    # Ensure key fields are first
    priority = ["OWNER","PROPERTY_A","MAILING_AD","PARID","LUC","CITY","ZIP","ADRNO","ADRDIR","ADRSTR","ADRSUF"]
    fieldnames = [f for f in priority if f in all_fields] + [f for f in all_fields if f not in priority]
    with csv_dest.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(normalized)
    print(f"      Saved {len(normalized)} rows to {csv_dest}")
    return True

def try_zip(url: str) -> bool:
    print(f"[ZIP] {url[:90]}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=180)
        print(f"      status={r.status_code} size={len(r.content):,}")
        if r.status_code != 200 or len(r.content) < 10000:
            return False
        z = zipfile.ZipFile(io.BytesIO(r.content))
        dbfs = sorted(
            [n for n in z.namelist() if n.lower().endswith(".dbf")],
            key=lambda n: ("address" not in n.lower(), n)
        )
        if not dbfs:
            print(f"      No DBF. Files: {z.namelist()[:8]}")
            return False
        z.extract(dbfs[0], "data/parcels/")
        for p in Path("data/parcels").rglob("*.dbf"):
            if p != dest:
                shutil.move(str(p), str(dest))
                break
        if dest.exists() and dest.stat().st_size > 50000:
            print(f"      DBF OK: {dest.stat().st_size:,} bytes")
            return True
    except Exception as e:
        print(f"      Error: {e}")
    return False

def try_gis_layer(base: str, idx: int, max_records: int = 2000) -> bool:
    """
    Query Lucas County GIS REST API for a specific layer.
    Uses pagination to get more records if needed.
    """
    # Request all fields including the confirmed ones
    fields = "own,name,adrno,adrdir,adrstr,adrsuf,adrsuf2,city,zip_code,parid,luc,statecode,zone,mh_area"
    url = (f"{base}/{idx}/query"
           f"?where=1%3D1"
           f"&outFields={fields}"
           f"&f=json"
           f"&resultRecordCount={max_records}"
           f"&returnGeometry=false")
    print(f"[GIS] Layer {idx}: {base.split('/')[-2]}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=90)
        if r.status_code != 200:
            print(f"      status={r.status_code}")
            return False
        data = r.json()
        if "error" in data:
            print(f"      API error: {data['error']}")
            return False
        features = data.get("features", [])
        if not features:
            print(f"      No features")
            return False
        rows = [f.get("attributes", {}) for f in features]
        rows = [r for r in rows if r]
        print(f"      {len(rows)} features, fields: {list(rows[0].keys())[:8]}")
        if not has_useful_fields(rows[0]):
            print(f"      No useful fields")
            return False
        # Check if we got owner or address data
        with_owner = sum(1 for r in rows if r.get("own") or r.get("name"))
        with_addr  = sum(1 for r in rows if r.get("adrno") or r.get("adrstr"))
        print(f"      owner={with_owner}/{len(rows)} addr={with_addr}/{len(rows)}")
        if with_owner < 10 and with_addr < 10:
            print(f"      Too few useful rows")
            return False
        return save_csv(rows)
    except Exception as e:
        print(f"      Error: {e}")
    return False

def try_gis_all_fields(base: str, idx: int) -> bool:
    """Try with outFields=* to get everything."""
    url = (f"{base}/{idx}/query"
           f"?where=1%3D1"
           f"&outFields=*"
           f"&f=json"
           f"&resultRecordCount=2000"
           f"&returnGeometry=false")
    print(f"[GIS*] Layer {idx} all fields")
    try:
        r = requests.get(url, headers=HEADERS, timeout=90)
        if r.status_code != 200:
            return False
        data = r.json()
        features = data.get("features", [])
        if not features:
            return False
        rows = [f.get("attributes", {}) for f in features]
        rows = [r for r in rows if r]
        if not rows:
            return False
        print(f"      {len(rows)} features | fields: {list(rows[0].keys())}")
        if not has_useful_fields(rows[0]):
            return False
        with_owner = sum(1 for r in rows if r.get("own") or r.get("name") or r.get("OWNER"))
        with_addr  = sum(1 for r in rows if r.get("adrno") or r.get("ADRNO") or r.get("adrstr"))
        print(f"      owner={with_owner} addr={with_addr}")
        if with_owner < 5 and with_addr < 5:
            return False
        return save_csv(rows)
    except Exception as e:
        print(f"      Error: {e}")
    return False

# ── STRATEGY 1: Shapefile ZIP downloads ───────────────────────────────────
print("=" * 60)
print("STRATEGY 1: Shapefile ZIP downloads")
print("=" * 60)
zip_urls = [
    "https://opendata.arcgis.com/datasets/f37bcb63d5ac4a3b9d926ade17f72be5_0/downloads/data?format=shp&spatialRefId=4326",
    "https://hub.arcgis.com/api/v3/datasets/f37bcb63d5ac4a3b9d926ade17f72be5_0/downloads/data?format=shp&spatialRefId=4326",
    "https://lucascountyauditor.org/GIS/ParcelsAddress.zip",
    "https://www.co.lucas.oh.us/GIS/ParcelsAddress.zip",
]
for url in zip_urls:
    if try_zip(url):
        print("SUCCESS: ZIP download")
        sys.exit(0)
    time.sleep(2)

# ── STRATEGY 2: GIS REST API — confirmed working layers ───────────────────
print("\n" + "=" * 60)
print("STRATEGY 2: Lucas County GIS REST API (confirmed field names)")
print("=" * 60)

# These are the confirmed working GIS service bases
gis_bases = [
    "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer",
    "https://gis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer",
    "https://lcaudgis.co.lucas.oh.us/arcgis/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer",
]

for base in gis_bases:
    # Try specific fields first (faster)
    for idx in range(0, 8):
        if try_gis_layer(base, idx):
            print(f"SUCCESS: GIS layer {idx} with specific fields")
            sys.exit(0)
        time.sleep(1)
    # Try all fields fallback
    for idx in range(0, 8):
        if try_gis_all_fields(base, idx):
            print(f"SUCCESS: GIS layer {idx} with all fields")
            sys.exit(0)
        time.sleep(1)

# ── STRATEGY 3: Lucas County Auditor AREIS download ───────────────────────
# The treasurer page mentions "Download AREIS" which is the Assessment Real Estate Info System
print("\n" + "=" * 60)
print("STRATEGY 3: AREIS / bulk data downloads")
print("=" * 60)
areis_urls = [
    "https://www.lucascountytreasurer.org/areis/download",
    "https://lucascountyauditor.org/areis/parcels.csv",
    "https://lucascountyauditor.org/api/parcels/download",
]
for url in areis_urls:
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        if r.status_code == 200 and len(r.content) > 10000:
            csv_dest.write_bytes(r.content)
            print(f"SUCCESS: AREIS download {len(r.content):,} bytes")
            sys.exit(0)
    except Exception as e:
        print(f"  {url[:60]}: {e}")
    time.sleep(1)

print("\nAll strategies exhausted.")
print("Scraper will use Lucas County Auditor API for per-record lookups.")
# Exit 0 so the workflow continues — don't block the scraper
sys.exit(0)
