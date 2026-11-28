"""Simulate what Databricks would send for the Catalog Service.

Grain: ONE ROW PER PRODUCT (sellable G-code). Everything about the product is
nested in typed structs/arrays (identifiers, texts, attributes, media, markets
with prices). Categories come from another pipeline: products only carry
category_id references.

Input : a semicolon CSV export of products (path as argv[1])
Output: ./out/
  plain_parquet/catalog/product/export_date=YYYY-MM-DD/part-*.snappy.parquet + _SUCCESS
  plain_parquet/catalog/category_ref/...      (stand-in for the other pipeline)
  delta/product/                              (Delta table, v0 = day 1, v1 = day 2)
  drift/                                      (files that must trip the validator)
"""
import csv, hashlib, json, os, random, re, shutil, sys, uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import pyarrow as pa, pyarrow.parquet as pq

random.seed(7)
SRC = sys.argv[1]
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
shutil.rmtree(OUT, ignore_errors=True)

def anon(url):  # replace the source host with a neutral one
    if not url: return url
    return re.sub(r"^https?://([^/]+)",
                  lambda m: "https://media.example-mro.com" if m.group(1).startswith("media") else "https://shop.example-mro.com", url)

def h(s, n=8): return hashlib.sha1(s.encode()).hexdigest()[:n].upper()
def dec(x, q="0.0001"): return Decimal(str(x)).quantize(Decimal(q), ROUND_HALF_UP)

# ------------------------------------------------------------------ schema
D = pa.decimal128(18, 4)
SCHEMA = pa.schema([
    pa.field("product_id", pa.string(), nullable=False),          # G-code
    pa.field("family", pa.struct([("family_id", pa.string()), ("name_fr", pa.string())])),
    pa.field("brand", pa.struct([("brand_id", pa.string()), ("name", pa.string())])),
    pa.field("mfr_ref", pa.string()),
    pa.field("lifecycle_status", pa.string()),                      # ACTIVE | PHASE_OUT | BLOCKED
    pa.field("identifiers", pa.list_(pa.struct([
        ("type", pa.string()), ("value", pa.string()), ("market", pa.string()), ("source", pa.string())]))),
    pa.field("category_refs", pa.list_(pa.struct([
        ("category_id", pa.string()), ("taxonomy", pa.string()), ("is_primary", pa.bool_())]))),
    pa.field("texts", pa.list_(pa.struct([
        ("locale", pa.string()), ("name", pa.string()), ("erp_name", pa.string()),
        ("family_name", pa.string()), ("slug", pa.string()), ("status", pa.string())]))),
    pa.field("attributes", pa.list_(pa.struct([
        ("code", pa.string()), ("value_type", pa.string()), ("value_string", pa.string()),
        ("value_number", pa.decimal128(18, 6)), ("value_bool", pa.bool_()), ("unit", pa.string()),
        ("locale", pa.string())]))),
    pa.field("media", pa.list_(pa.struct([
        ("asset_id", pa.string()), ("asset_type", pa.string()), ("role", pa.string()), ("url", pa.string()),
        ("mime_type", pa.string()), ("locale", pa.string()), ("revision_ts", pa.timestamp("us", tz="UTC")),
        ("sort_order", pa.int32())]))),
    pa.field("markets", pa.list_(pa.struct([
        ("market", pa.string()), ("status", pa.string()), ("visible_anonymous", pa.bool_()),
        ("sales_unit", pa.string()), ("min_order_qty", pa.int32()), ("pack_size", pa.int32()),
        ("stock_status", pa.string()), ("lead_time_days", pa.int32()), ("url", pa.string()),
        ("prices", pa.list_(pa.struct([
            ("price_list", pa.string()), ("currency", pa.string()), ("min_qty", D),
            ("unit_price", D), ("price_per", pa.int32()),
            ("valid_from", pa.date32()), ("valid_to", pa.date32())])))]))),
    pa.field("source_updated_at", pa.timestamp("us", tz="UTC")),
    pa.field("export_id", pa.string(), nullable=False),
    pa.field("export_ts", pa.timestamp("us", tz="UTC"), nullable=False),
], metadata={"contract": "catalog.product", "contract_version": "1.0.0"})

UNIT = {"unité": "PCE", "jeu": "SET", "étui": "CASE", "boite": "BOX", "lot": "LOT", "paquet": "PACK", "": None}
MARKETS = {  # market: (currency, price factor, locale for texts, share of assortment)
    "FR": ("EUR", 1.00, "fr-FR", 1.00), "BE": ("EUR", 1.03, "fr-BE", 0.70),
    "DE": ("EUR", 1.06, "de-DE", 0.55), "PL": ("PLN", 4.35, "pl-PL", 0.30),
}
SLUG = lambda s: re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s.lower()
            .translate(str.maketrans("àâäéèêëîïôöùûüç", "aaaeeeeiioouuuc")))).strip("-")

def parse_rev(url):
    m = re.search(r"v=(\d{12})", url or "")
    return datetime.strptime(m.group(1), "%y%m%d%H%M%S").replace(tzinfo=timezone.utc) if m else None

def media(url, kind, role, sort):
    if not url: return None
    m = re.search(r"/(\d{10,})/", url)
    return {"asset_id": f"MA-{m.group(1) if m else h(url, 12)}", "asset_type": kind, "role": role,
            "url": url, "mime_type": "application/pdf" if url.split("?")[0].endswith(".pdf") else "image/jpeg",
            "locale": "fr-FR" if kind == "DATASHEET" else None, "revision_ts": parse_rev(url), "sort_order": sort}

def attrs_from(name, erp):  # best-effort parse: the real feed must carry attribute values
    out, txt = [], f"{name} {erp}"
    for code, rx, unit in [("length", r"(\d+(?:[.,]\d+)?)\s?mm\b", "mm"), ("size_inch", r"(\d+(?:[.,]\d+)?)\s?(?:'|\")", "in"),
                           ("torque_max", r"(\d+(?:[.,]\d+)?)\s?N\.?m\b", "N.m"), ("voltage", r"(\d+(?:[.,]\d+)?)\s?V\b", "V")]:
        m = re.search(rx, txt, re.I)
        if m: out.append({"code": code, "value_type": "NUMBER", "value_string": None, "value_number": dec(m.group(1).replace(",", "."), "0.000001"),
                          "value_bool": None, "unit": unit, "locale": None})
    if re.search(r"gain[ée]", txt, re.I):
        out.append({"code": "insulated_grip", "value_type": "BOOL", "value_string": None, "value_number": None, "value_bool": True, "unit": None, "locale": None})
    return out

def build_rows(rows, export_id, export_ts, day):
    out, cats = [], {}
    for r in rows:
        path = [r[f"cat_l{i}"] for i in range(2, 6)]
        cid = "CAT-" + h(" > ".join(path))
        cats[cid] = path
        fam = "FAM-" + h(r["range_name"])
        ean = r["ean"].replace(".0", "").zfill(13) if r["ean"] else None
        base = float(r["price_ht_eur"]); n_tiers = int(r["price_tiers"] or 1)
        markets = []
        for mk, (cur, fx, loc, share) in MARKETS.items():
            if mk != "FR" and random.random() > share: continue
            status = "ACTIVE" if mk == "FR" else random.choices(["ACTIVE", "NOT_IN_ERP", "BLOCKED"], [0.85, 0.12, 0.03])[0]
            p = base * fx * (1 + random.uniform(-0.03, 0.03) if mk != "FR" else 1)
            prices = [{"price_list": f"{mk}_LIST", "currency": cur, "min_qty": dec(q, "0.001"),
                       "unit_price": dec(p * d), "price_per": 1, "valid_from": date(2026, 1, 1), "valid_to": None}
                      for q, d in [(1, 1.0), (10, 0.95), (50, 0.90)][:n_tiers]]
            if mk == "FR" and r["crossout_ht_eur"]:
                prices.append({"price_list": "FR_STRIKETHROUGH", "currency": "EUR", "min_qty": dec(1, "0.001"),
                               "unit_price": dec(r["crossout_ht_eur"]), "price_per": 1,
                               "valid_from": day - timedelta(days=7), "valid_to": day + timedelta(days=21)})
            lt = int(float(r["lead_time_days"])) if r["lead_time_days"] else None
            markets.append({"market": mk, "status": status, "visible_anonymous": status == "ACTIVE",
                            "sales_unit": UNIT.get(r["sales_unit"], "PCE") or "PCE", "min_order_qty": 1, "pack_size": 1,
                            "stock_status": r["stock_status"] if mk == "FR" else random.choice(["IN_STOCK", "AVAILABLE_LEAD_TIME"]),
                            "lead_time_days": lt if mk == "FR" else random.choice([None, 3, 10]),
                            "url": anon(r["url"]) if mk == "FR" else None, "prices": prices})
        texts = [{"locale": "fr-FR", "name": r["name"], "erp_name": r["erp_sku_name"], "family_name": r["range_name"],
                  "slug": SLUG(r["range_name"]), "status": "SOURCE"}]
        texts += [{"locale": MARKETS[m["market"]][2], "name": None, "erp_name": None, "family_name": None, "slug": None,
                   "status": "MISSING"} for m in markets if m["market"] in ("DE", "PL")]   # realistic enrichment gaps
        ids = [{"type": "INTERNAL_REF", "value": r.get("internal_ref") or next(v for k, v in r.items() if k.endswith("_ref") and k != "mfr_ref"), "market": "FR", "source": "ERP_FR"},
               {"type": "MFR_REF", "value": r["mfr_ref"], "market": None, "source": "PIM"},
               {"type": "MDM_ID", "value": "MDM" + h(r["product_id"], 10), "market": None, "source": "MDM"}]
        if ean: ids.append({"type": "EAN", "value": ean, "market": None, "source": "PIM"})
        out.append({
            "product_id": r["product_id"], "family": {"family_id": fam, "name_fr": r["range_name"]},
            "brand": {"brand_id": "BR-" + h(r["brand"], 6), "name": r["brand"]}, "mfr_ref": r["mfr_ref"],
            "lifecycle_status": "ACTIVE", "identifiers": ids,
            "category_refs": [{"category_id": cid, "taxonomy": "WEB", "is_primary": True}],
            "texts": texts, "attributes": attrs_from(r["name"], r["erp_sku_name"]),
            "media": [m for m in (media(anon(r["image_url"]), "IMAGE", "PRIMARY", 1),
                                  media(anon(r["datasheet_url"]), "DATASHEET", "TECHNICAL_DATASHEET", 1)) if m],
            "markets": markets,
            "source_updated_at": export_ts - timedelta(hours=random.randint(1, 24 * 90)),
            "export_id": export_id, "export_ts": export_ts})
    return sorted(out, key=lambda x: x["product_id"]), cats

def write_plain(rows, root, day, export_id, n_files=2):
    d = os.path.join(root, f"export_date={day.isoformat()}")
    os.makedirs(d, exist_ok=True)
    job = uuid.UUID(int=random.getrandbits(128))
    step = -(-len(rows) // n_files)
    for i in range(n_files):
        tbl = pa.Table.from_pylist(rows[i * step:(i + 1) * step], schema=SCHEMA)
        pq.write_table(tbl, os.path.join(d, f"part-{i:05d}-{job}-c000.snappy.parquet"),
                       compression="snappy", row_group_size=250)   # small row groups to exercise partitioning
    open(os.path.join(d, "_SUCCESS"), "w").close()                  # Spark job-completion marker

src = list(csv.DictReader(open(SRC, encoding="utf-8-sig"), delimiter=";"))

# ---------- day 1: full export
d1, ts1 = date(2026, 11, 27), datetime(2026, 11, 27, 2, 0, tzinfo=timezone.utc)
rows1, cats = build_rows(src, "EXP-20261127T0200Z", ts1, d1)
write_plain(rows1, f"{OUT}/plain_parquet/catalog/product", d1, "EXP-20261127T0200Z")

# ---------- day 2: price changes, 5 delisted, 10 phase-out, 1 new text locale
random.seed(8)
d2, ts2 = date(2026, 11, 28), datetime(2026, 11, 28, 2, 0, tzinfo=timezone.utc)
rows2, _ = build_rows(src[5:], "EXP-20261128T0200Z", ts2, d2)
for r in rows2[:10]: r["lifecycle_status"] = "PHASE_OUT"
for r in random.sample(rows2, 40):
    for m in r["markets"]:
        for p in m["prices"]: p["unit_price"] = dec(float(p["unit_price"]) * 1.04)
write_plain(rows2, f"{OUT}/plain_parquet/catalog/product", d2, "EXP-20261128T0200Z")

# ---------- categories: stand-in for the other pipeline (ids referenced by products)
cat_rows = []
for cid, path in cats.items():
    for lvl in range(1, 5):
        pid = "CAT-" + h(" > ".join(path[:lvl]))
        cat_rows.append({"category_id": pid, "parent_id": ("CAT-" + h(" > ".join(path[:lvl - 1]))) if lvl > 1 else None,
                         "level": lvl + 1, "name_fr": path[lvl - 1], "path_fr": " > ".join(path[:lvl])})
cat_rows = list({c["category_id"]: c for c in cat_rows}.values())
cd = f"{OUT}/plain_parquet/catalog/category_ref/export_date={d2.isoformat()}"
os.makedirs(cd, exist_ok=True)
pq.write_table(pa.Table.from_pylist(cat_rows), f"{cd}/part-00000-c000.snappy.parquet")
open(f"{cd}/_SUCCESS", "w").close()

# ---------- Delta table: what Delta Sharing would expose (v0 = day 1, v1 = day 2)
try:
    from deltalake import write_deltalake
    dp = f"{OUT}/delta/product"
    write_deltalake(dp, pa.Table.from_pylist(rows1, schema=SCHEMA), mode="overwrite")
    write_deltalake(dp, pa.Table.from_pylist(rows2, schema=SCHEMA), mode="overwrite")
except Exception as e:
    print("delta skipped:", e)

# ---------- drift fixtures: each must be handled by the validator's policy
dd = f"{OUT}/drift"; os.makedirs(dd, exist_ok=True)
sample = rows1[:50]
t = pa.Table.from_pylist(sample, schema=SCHEMA)
pq.write_table(t.append_column("new_marketing_flag", pa.array([True] * len(sample))), f"{dd}/extra_column.parquet")      # ACCEPT + warn
pq.write_table(t.drop_columns(["mfr_ref"]), f"{dd}/missing_optional_column.parquet")                                 # ACCEPT + warn
pq.write_table(t.drop_columns(["product_id"]), f"{dd}/missing_required_column.parquet")                               # REJECT
bad = t.set_column(t.schema.get_field_index("export_ts"), "export_ts", pa.array([str(x) for x in t["export_ts"].to_pylist()]))
pq.write_table(bad, f"{dd}/incompatible_type.parquet")                                                                # REJECT
ren = t.rename_columns(["productId" if c == "product_id" else c for c in t.column_names])
pq.write_table(ren, f"{dd}/renamed_column.parquet")                                                                   # REJECT unless alias
dup = pa.concat_tables([t, t.slice(0, 3)])
pq.write_table(dup, f"{dd}/duplicate_keys.parquet")                                                                   # data check: dedup + warn

print(f"day1={len(rows1)} day2={len(rows2)} categories={len(cat_rows)}")
