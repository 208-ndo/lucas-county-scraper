"""
download_parcels.py
Downloads ALL Lucas County parcel data using the confirmed GIS REST API layer 4.
Uses pagination (resultOffset) to get all ~200k+ parcels, not just the first 2000.
Saves as ParcelsAddress.csv for fetch.py to read.

Confirmed working:
  Base: https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer
  Layer: 4
  Fields: adrsuf2, adrdir, parid, city, own, adrsuf, statecode, zip_code, adrno, adrstr, luc, zone
"""
import requests, sys, time, csv, json
from pathlib import Path

csv_dest = Path("data/parcels/ParcelsAddress.csv")
HEADERS  = {"User-Agent": "Mozilla/5.0 (compatible; LucasCountyScraper/3.0)"}

# Confirmed working GIS base and layer
GIS_BASE  = "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer"
GIS_LAYER = 4

# Fields confirmed to exist on this layer
# 'own' = owner name, 'adrno'+'adrdir'+'adrstr'+'adrsuf' = address parts
# 'city' = city, 'zip_code' = zip, 'parid' = parcel ID, 'luc' = land use code
FIELDS = "own,adrno,adrdir,adrstr,adrsuf,adrsuf2,city,zip_code,parid,luc,statecode"

def fetch_page(offset: int, page_size: int = 1000) -> list:
    """Fetch one page of parcel records."""
    url = (
        f"{GIS_BASE}/{GIS_LAYER}/query"
        f"?where=1%3D1"
        f"&outFields={FIELDS}"
        f"&f=json"
        f"&resultRecordCount={page_size}"
        f"&resultOffset={offset}"
        f"&returnGeometry=false"
        f"&orderByFields=parid"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=90)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code} at offset {offset}")
            return []
        data = r.json()
        if "error" in data:
            print(f"  API error at offset {offset}: {data['error']}")
            return []
        features = data.get("features", [])
        rows = [f.get("attributes", {}) for f in features]
        rows = [r for r in rows if r]
        # Check if there are more records
        exceeded = data.get("exceededTransferLimit", False)
        return rows, exceeded
    except Exception as e:
        print(f"  Error at offset {offset}: {e}")
        return [], False

def build_property_address(row: dict) -> str:
    """Build full property address from component GIS fields."""
    parts = [
        str(row.get("adrno","") or "").strip(),
        str(row.get("adrdir","") or "").strip(),
        str(row.get("adrstr","") or "").strip(),
        str(row.get("adrsuf","") or "").strip(),
        str(row.get("adrsuf2","") or "").strip(),
    ]
    addr = " ".join(p for p in parts if p and p != "None").strip()
    city = str(row.get("city","") or "").strip()
    zip_ = str(row.get("zip_code","") or "").strip()
    if addr and city:
        return f"{addr.title()}, {city.title()} OH {zip_}".strip()
    return addr.title() if addr else ""

def normalize_row(row: dict) -> dict:
    """Convert GIS field names to our standard names."""
    owner = str(row.get("own","") or "").strip().title()
    parid = str(row.get("parid","") or "").strip()
    luc   = str(row.get("luc","") or "").strip()
    city  = str(row.get("city","") or "Toledo").strip().title()
    zip_  = str(row.get("zip_code","") or "").strip()
    prop_addr = build_property_address(row)
    return {
        "OWNER":      owner,
        "PROPERTY_A": prop_addr,
        "PARID":      parid,
        "LUC":        luc,
        "CITY":       city,
        "ZIP":        zip_,
        "ADRNO":      str(row.get("adrno","") or "").strip(),
        "ADRDIR":     str(row.get("adrdir","") or "").strip(),
        "ADRSTR":     str(row.get("adrstr","") or "").strip(),
        "ADRSUF":     str(row.get("adrsuf","") or "").strip(),
        "MAILING_AD": "",  # not in this layer — will use owner address lookup
    }

print("=" * 60)
print("Lucas County GIS REST API — Paginated Parcel Download")
print(f"Layer {GIS_LAYER} | Fields: {FIELDS}")
print("=" * 60)

# First check how many total records exist
try:
    count_url = f"{GIS_BASE}/{GIS_LAYER}/query?where=1%3D1&returnCountOnly=true&f=json"
    r = requests.get(count_url, headers=HEADERS, timeout=30)
    if r.status_code == 200:
        total = r.json().get("count", 0)
        print(f"Total records in layer: {total:,}")
    else:
        total = 0
        print(f"Could not get count (status {r.status_code})")
except Exception as e:
    total = 0
    print(f"Count query error: {e}")

# Download all pages
all_rows = []
page_size = 1000
offset = 0
max_pages = 300  # safety limit (300k records max)
consecutive_errors = 0

print(f"\nDownloading pages (page_size={page_size})...")

while offset <= max_pages * page_size:
    result = fetch_page(offset, page_size)
    if isinstance(result, tuple):
        rows, exceeded = result
    else:
        rows = result; exceeded = False

    if not rows:
        consecutive_errors += 1
        if consecutive_errors >= 3:
            print(f"  3 consecutive errors at offset {offset}, stopping")
            break
        time.sleep(2)
        offset += page_size
        continue

    consecutive_errors = 0
    all_rows.extend(rows)

    if offset % 10000 == 0 or offset < 5000:
        print(f"  Offset {offset:>6,} | Downloaded {len(all_rows):>6,} rows | Last batch: {len(rows)}")

    # If we got fewer rows than page_size, we've hit the end
    if len(rows) < page_size:
        print(f"  Last page at offset {offset} ({len(rows)} rows) — download complete")
        break

    if not exceeded and len(rows) < page_size:
        break

    offset += page_size
    time.sleep(0.3)  # polite delay

print(f"\nTotal rows downloaded: {len(all_rows):,}")

if not all_rows:
    print("ERROR: No rows downloaded!")
    sys.exit(0)

# Check data quality
with_owner = sum(1 for r in all_rows if r.get("own","").strip())
with_addr  = sum(1 for r in all_rows if r.get("adrno","").strip() or r.get("adrstr","").strip())
print(f"With owner: {with_owner:,} | With address: {with_addr:,}")

# Normalize and save
print(f"\nNormalizing and saving to {csv_dest}...")
normalized = [normalize_row(r) for r in all_rows]

# Filter out rows with neither owner nor address
useful = [r for r in normalized if r["OWNER"] or r["PROPERTY_A"]]
print(f"Useful rows (owner or address): {len(useful):,}")

fieldnames = ["OWNER","PROPERTY_A","PARID","LUC","CITY","ZIP","ADRNO","ADRDIR","ADRSTR","ADRSUF","MAILING_AD"]

with csv_dest.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    w.writerows(useful)

size_kb = csv_dest.stat().st_size / 1024
print(f"Saved: {csv_dest} ({size_kb:,.0f} KB, {len(useful):,} rows)")

# Save a sample for debugging
sample_path = Path("data/debug/parcel_csv_sample.json")
sample_path.parent.mkdir(exist_ok=True)
sample_path.write_text(json.dumps(useful[:10], indent=2))
print(f"Sample saved to {sample_path}")

print("\nSUCCESS: Parcel data ready for fetch.py")
sys.exit(0)
