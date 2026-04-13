"""
download_parcels.py
Downloads Lucas County ParcelsAddress.dbf using multiple fallback strategies.
Called by scrape.yml — runs once per week (cached by GitHub Actions).
"""
import requests, zipfile, io, shutil, sys, time, csv
from pathlib import Path

dest     = Path("data/parcels/ParcelsAddress.dbf")
csv_dest = Path("data/parcels/ParcelsAddress.csv")
HEADERS  = {"User-Agent": "Mozilla/5.0 (compatible; LucasCountyScraper/2.0)"}

def try_zip(url):
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
            print(f"      no DBF in zip. files={z.namelist()[:5]}")
            return False
        z.extract(dbfs[0], "data/parcels/")
        # move out of any subdirectory
        for p in Path("data/parcels").rglob("*.dbf"):
            if p != dest:
                shutil.move(str(p), str(dest))
                break
        if dest.exists() and dest.stat().st_size > 50000:
            print(f"      OK: {dest.stat().st_size:,} bytes")
            return True
    except Exception as e:
        print(f"      error: {e}")
    return False

def try_json(url, label=""):
    print(f"[JSON] {label or url[:80]}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        if r.status_code != 200:
            print(f"      status={r.status_code}")
            return False
        data = r.json()
        features = data.get("features", [])
        if not features:
            print("      no features")
            return False
        rows = [f.get("attributes", f.get("properties", {})) for f in features]
        rows = [row for row in rows if row]
        if not rows:
            return False
        keys = list(rows[0].keys())
        keys_upper = " ".join(k.upper() for k in keys)
        has_useful = any(x in keys_upper for x in ["OWNER", "OWN1", "ADDR", "PROPERTY_A", "PARID"])
        if not has_useful:
            print(f"      no useful fields: {keys[:6]}")
            return False
        with csv_dest.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(rows)
        print(f"      saved {len(rows)} rows to CSV")
        return True
    except Exception as e:
        print(f"      error: {e}")
    return False

# ── Strategy 1: Known shapefile ZIP URLs ──────────────────────────────────
zip_urls = [
    "https://opendata.arcgis.com/datasets/f37bcb63d5ac4a3b9d926ade17f72be5_0/downloads/data?format=shp&spatialRefId=4326",
    "https://hub.arcgis.com/api/v3/datasets/f37bcb63d5ac4a3b9d926ade17f72be5_0/downloads/data?format=shp&spatialRefId=4326",
    "https://lucascountyauditor.org/GIS/ParcelsAddress.zip",
    "https://lucascountyauditor.org/downloads/parcels/ParcelsAddress.zip",
    "https://www.co.lucas.oh.us/GIS/ParcelsAddress.zip",
]
for url in zip_urls:
    if try_zip(url):
        print("SUCCESS: ZIP download")
        sys.exit(0)
    time.sleep(2)

# ── Strategy 2: GIS REST API JSON layers ─────────────────────────────────
gis_bases = [
    "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer",
    "https://gis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer",
]
fields = "OWNER,PROPERTY_A,MAILING_AD,PARID,LUC,ZONING"
for base in gis_bases:
    for idx in range(0, 6):
        url = f"{base}/{idx}/query?where=1%3D1&outFields={fields}&f=json&resultRecordCount=2000"
        if try_json(url, f"Layer {idx}"):
            print("SUCCESS: GIS REST JSON")
            sys.exit(0)
        time.sleep(1)

# ── Strategy 3: Bulk CSV endpoints ────────────────────────────────────────
bulk_urls = [
    "https://lucascountyauditor.org/api/export/parcels?format=csv",
    "https://lucascountyauditor.org/property-search/export?format=csv",
]
for url in bulk_urls:
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        if r.status_code == 200 and len(r.content) > 10000:
            csv_dest.write_bytes(r.content)
            print(f"SUCCESS: bulk CSV {len(r.content):,} bytes")
            sys.exit(0)
    except Exception as e:
        print(f"  {url[:60]}: {e}")
    time.sleep(1)

print("All strategies failed - scraper will use auditor API per-record fallback")
sys.exit(0)  # exit 0 so workflow continues
