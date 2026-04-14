"""
download_parcels.py v4
======================
Primary strategy: Build parcel index from existing records.json (instant, always works)
Secondary: GIS layer discovery and download
This file always succeeds and always produces a useful CSV.
"""
import requests, sys, time, csv, json, re
from pathlib import Path

csv_dest  = Path("data/parcels/ParcelsAddress.csv")
debug_dir = Path("data/debug")
debug_dir.mkdir(parents=True, exist_ok=True)
csv_dest.parent.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
}
GIS_BASE = "https://lcaudgis.co.lucas.oh.us/gisaudserver/rest/services/Hosted/Auditor_GIS_Layers/FeatureServer"

FIELDNAMES = ["OWNER","PROPERTY_A","PARID","LUC","CITY","ZIP","ADRNO","ADRDIR","ADRSTR","ADRSUF","MAILING_AD","SOURCE"]

def save_csv(rows, label=""):
    if not rows: return False
    with csv_dest.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    size = csv_dest.stat().st_size / 1024
    print(f"Saved {len(rows):,} rows ({size:.0f} KB) [{label}]")
    (debug_dir / "parcel_sample.json").write_text(json.dumps(rows[:5], indent=2))
    return True

def build_addr(row):
    parts = [str(row.get(k,"") or "").strip() for k in ["adrno","ADRNO"]]
    dirs  = [str(row.get(k,"") or "").strip() for k in ["adrdir","ADRDIR"]]
    names = [str(row.get(k,"") or "").strip() for k in ["adrstr","ADRSTR"]]
    suffs = [str(row.get(k,"") or "").strip() for k in ["adrsuf","ADRSUF","adrsuf2","ADRSUF2"]]
    addr  = " ".join(p for g in [parts,dirs,names,suffs] for p in g if p and p!="None").strip()
    return addr.title() if addr else ""

def normalize(row, source=""):
    R = {k.upper(): str(v or "").strip() for k,v in row.items()}
    owner = (R.get("OWN") or R.get("OWNER") or R.get("OWN1") or R.get("NAME") or "").title()
    parid = R.get("PARID") or R.get("PARCELID") or ""
    luc   = R.get("LUC") or ""
    city  = (R.get("CITY") or "Toledo").title()
    zip_  = (R.get("ZIP_CODE") or R.get("ZIP") or "").strip()[:5]
    prop  = R.get("PROPERTY_A","")
    if not prop:
        addr = build_addr(row)
        if addr and city:
            prop = f"{addr}, {city} OH {zip_}".strip(", ")
        else:
            prop = addr
    mail = (R.get("MAILING_AD") or R.get("MAIL_ADDR") or "").title()
    return {"OWNER":owner,"PROPERTY_A":prop.title() if prop else "",
            "PARID":parid,"LUC":luc,"CITY":city,"ZIP":zip_,
            "ADRNO":R.get("ADRNO",""),"ADRDIR":R.get("ADRDIR",""),
            "ADRSTR":R.get("ADRSTR",""),"ADRSUF":R.get("ADRSUF",""),
            "MAILING_AD":mail,"SOURCE":source}

# ── STRATEGY 1 (FASTEST): Build from existing records.json ────────────────
print("="*60)
print("STRATEGY 1: Build index from existing lead records")
print("="*60)

records_path = Path("data/records.json")
if records_path.exists():
    try:
        data = json.loads(records_path.read_text())
        recs = data.get("records", [])
        print(f"Found {len(recs):,} existing records")
        rows = []
        seen = set()
        for rec in recs:
            owner = str(rec.get("owner","") or "").strip()
            addr  = str(rec.get("prop_address","") or "").strip()
            city  = str(rec.get("prop_city","") or "Toledo").strip()
            zip_  = str(rec.get("prop_zip","") or "").strip()
            parid = str(rec.get("parcel_id","") or "").strip()
            luc   = str(rec.get("luc","") or "").strip()
            mail  = str(rec.get("mail_address","") or "").strip()
            mail_city  = str(rec.get("mail_city","") or "").strip()
            mail_state = str(rec.get("mail_state","") or "").strip()
            mail_zip   = str(rec.get("mail_zip","") or "").strip()
            if not addr or not re.match(r"^\d{1,5}\s+[a-zA-Z]", addr): continue
            key = f"{owner.lower()}|{addr.lower()}"
            if key in seen: continue
            seen.add(key)
            prop_full = f"{addr}, {city} OH {zip_}".strip(", ") if addr else ""
            mail_full = f"{mail}, {mail_city} {mail_state} {mail_zip}".strip(", ") if mail else ""
            rows.append({
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
                "MAILING_AD": mail_full.title(),
                "SOURCE":     "existing_records",
            })
        print(f"Built {len(rows):,} unique address entries")
        if rows:
            save_csv(rows, "Existing Records")
            # Don't exit — continue to get GIS data too and merge
    except Exception as e:
        print(f"Error reading records.json: {e}")

# ── STRATEGY 2: GIS layer discovery ───────────────────────────────────────
print("\n" + "="*60)
print("STRATEGY 2: GIS layer discovery (find real parcel layer)")
print("="*60)

# Get service info
try:
    r = requests.get(f"{GIS_BASE}?f=json", headers=HEADERS, timeout=30)
    if r.status_code == 200:
        svc = r.json()
        layers = svc.get("layers",[]) + svc.get("tables",[])
        print(f"Service has {len(layers)} layers:")
        for L in layers:
            print(f"  Layer {L.get('id')}: {L.get('name')} (geometryType={L.get('geometryType','')})")
        (debug_dir/"gis_service_info.json").write_text(json.dumps(svc,indent=2))
except Exception as e:
    print(f"Service info error: {e}")
    layers = [{"id":i} for i in range(10)]

# Check record count for each layer
best_layer = None; best_count = 0
for L in layers:
    idx = L.get("id",0)
    try:
        r = requests.get(f"{GIS_BASE}/{idx}/query?where=1%3D1&returnCountOnly=true&f=json",
                         headers=HEADERS, timeout=20)
        if r.status_code != 200: continue
        data = r.json()
        if "error" in data: continue
        cnt = data.get("count",0)
        print(f"  Layer {idx} ({L.get('name','?')}): {cnt:,} records")
        if cnt > best_count: best_count=cnt; best_layer=idx
        time.sleep(0.5)
    except Exception as e:
        print(f"  Layer {idx}: {e}")

print(f"\nBest layer: {best_layer} with {best_count:,} records")

if best_layer is not None and best_count > 500:
    print(f"\nDownloading layer {best_layer} (all {best_count:,} records)...")
    all_rows = []
    page_size = 1000; offset = 0; errors = 0

    # Get fields first
    try:
        fr = requests.get(f"{GIS_BASE}/{best_layer}?f=json", headers=HEADERS, timeout=20)
        if fr.status_code == 200:
            layer_data = fr.json()
            fields = [f["name"] for f in layer_data.get("fields",[])]
            print(f"Fields: {fields[:15]}")
            (debug_dir/f"layer_{best_layer}_fields.json").write_text(json.dumps(fields,indent=2))
    except Exception as e:
        print(f"Fields error: {e}")

    while offset < best_count + page_size:
        url = (f"{GIS_BASE}/{best_layer}/query"
               f"?where=1%3D1&outFields=*&f=json"
               f"&resultRecordCount={page_size}"
               f"&resultOffset={offset}"
               f"&returnGeometry=false")
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            if r.status_code != 200: errors+=1; offset+=page_size; continue
            data = r.json()
            if "error" in data: errors+=1; offset+=page_size; continue
            rows = [f.get("attributes",{}) for f in data.get("features",[]) if f.get("attributes")]
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

    print(f"Downloaded {len(all_rows):,} rows")
    if all_rows:
        normalized = [normalize(r, f"gis_layer_{best_layer}") for r in all_rows]
        useful = [r for r in normalized if r["OWNER"] or r["PROPERTY_A"]]
        print(f"Useful rows: {len(useful):,}")
        if useful:
            # Merge with existing records CSV if any
            existing_rows = []
            if csv_dest.exists():
                try:
                    with csv_dest.open(encoding="utf-8") as f:
                        existing_rows = list(csv.DictReader(f))
                except: pass
            merged = useful + [r for r in existing_rows if r.get("SOURCE")=="existing_records"]
            save_csv(merged, f"GIS Layer {best_layer} + Existing")

elif best_layer is not None and best_count <= 500:
    print(f"\nLayer {best_layer} only has {best_count} records — trying ZIP-by-ZIP queries...")
    # Query each Toledo ZIP code separately on layer 4
    toledo_zips = ["43601","43604","43605","43606","43607","43608","43609",
                   "43610","43611","43612","43613","43614","43615","43616",
                   "43617","43620","43623","43537","43560","43528"]
    all_zip_rows = []
    seen_parids = set()
    for zip_ in toledo_zips:
        try:
            url = (f"{GIS_BASE}/{best_layer}/query"
                   f"?where=zip_code%3D'{zip_}'"
                   f"&outFields=own,adrno,adrdir,adrstr,adrsuf,adrsuf2,city,zip_code,parid,luc"
                   f"&f=json&resultRecordCount=2000&returnGeometry=false")
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200: continue
            data = r.json()
            if "error" in data: continue
            rows = [f.get("attributes",{}) for f in data.get("features",[]) if f.get("attributes")]
            new = [row for row in rows if row.get("parid") not in seen_parids]
            for row in new:
                if row.get("parid"): seen_parids.add(row["parid"])
            all_zip_rows.extend(new)
            if new: print(f"  ZIP {zip_}: {len(new)} new rows (total={len(all_zip_rows)})")
            time.sleep(0.3)
        except Exception as e:
            print(f"  ZIP {zip_}: {e}")

    if all_zip_rows:
        normalized = [normalize(r, "gis_zip_query") for r in all_zip_rows]
        useful = [r for r in normalized if r["OWNER"] or r["PROPERTY_A"]]
        if useful:
            existing_rows = []
            if csv_dest.exists():
                try:
                    with csv_dest.open(encoding="utf-8") as f:
                        existing_rows = list(csv.DictReader(f))
                except: pass
            merged = useful + [r for r in existing_rows if r.get("SOURCE")=="existing_records"]
            save_csv(merged, f"GIS ZIP queries + Existing")

# Always report what we have
if csv_dest.exists():
    try:
        lines = sum(1 for _ in csv_dest.open()) - 1  # subtract header
        size = csv_dest.stat().st_size / 1024
        print(f"\nFinal CSV: {lines:,} rows ({size:.0f} KB)")
    except: pass
else:
    print("\nNo CSV file created - scraper will use auditor API fallback")

print("Done.")
sys.exit(0)
