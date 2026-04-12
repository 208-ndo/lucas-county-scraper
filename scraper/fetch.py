def write_outputs(records: list, sheriff: list, probate: list,
                  tax_delinquent: list, foreclosures: list):

    def to_dict(r):
        d = asdict(r)
        d["score"] = score_record(r)
        return d

    all_dicts = [to_dict(r) for r in records]

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

    # ── Main records.json — wrapped in metadata object so dashboard reads it ──
    meta = {
        "total":               len(all_dicts),
        "fetched_at":          datetime.now(timezone.utc).isoformat(),
        "hot_stack_count":     len(hot),
        "sheriff_sale_count":  len(s_dicts),
        "probate_count":       len(pr_dicts),
        "tax_delinquent_count":len(td_dicts),
        "foreclosure_count":   len(fc_dicts),
        "absentee_count":      len(absentee),
        "out_of_state_count":  len(oos),
        "vacant_land_count":   len(vacant),
        "inherited_count":     len(inherited),
        "liens_count":         len(liens),
        "code_violation_count":0,
        "vacant_home_count":   0,
        "subject_to_count":    0,
        "records":             all_dicts,
    }
    for p in [DATA_DIR / "records.json", DASHBOARD_DIR / "records.json"]:
        write_json(p, meta)
    logging.info("Wrote data (records.json: %d records)", len(all_dicts))

    # ── Category JSONs (plain arrays — dashboard loads these separately) ──
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
        ("vacant_homes.json",    []),
        ("evictions.json",       []),
        ("prime_subject_to.json",[]),
    ]:
        for p in [DATA_DIR / fname, DASHBOARD_DIR / fname]:
            write_json(p, data)

    # ── GHL CSV ──
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
            owner = d.get("owner", "") or ""
            parts = owner.split()
            emv   = d.get("est_market_value")
            w.writerow({
                "First Name":  parts[0] if parts else "",
                "Last Name":   " ".join(parts[1:]) if len(parts) > 1 else "",
                "Phone": d.get("phone", ""), "Email": "",
                "Property Address": d.get("prop_address", ""),
                "Property City":    d.get("prop_city", ""),
                "Property Zip":     d.get("prop_zip", ""),
                "Mailing Address":  d.get("mail_address", ""),
                "Mailing City":     d.get("mail_city", ""),
                "Mailing State":    d.get("mail_state", ""),
                "Mailing Zip":      d.get("mail_zip", ""),
                "Lead Type":        d.get("doc_type", ""),
                "Date Filed":       d.get("filed", ""),
                "Document Number":  d.get("doc_num", ""),
                "Amount/Debt Owed": d.get("amount", ""),
                "Seller Score":     d.get("score", 0),
                "Motivated Seller Flags": "; ".join(d.get("flags") or []),
                "Distress Sources": "; ".join(d.get("distress_sources") or []),
                "Distress Count":   d.get("distress_count", 0),
                "Hot Stack":        "YES" if d.get("hot_stack") else "",
                "Absentee Owner":   "YES" if d.get("is_absentee") else "",
                "Out of State":     "YES" if d.get("is_out_of_state") else "",
                "Vacant Land":      "YES" if d.get("is_vacant_land") else "",
                "Inherited":        "YES" if d.get("is_inherited") else "",
                "Equity Est":       f"${emv:,.0f}" if emv else "",
                "Parcel ID":        d.get("parcel_id", ""),
                "LUC":              d.get("luc", ""),
                "Source":           SOURCE_NAME,
                "Public Records URL": d.get("clerk_url", ""),
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
