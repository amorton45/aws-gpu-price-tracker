#!/usr/bin/env python3
"""
AWS GPU-price tracker with historical back-fill.

• Walks back month-by-month from today to BACKFILL_START.
• Writes a row only when it finds a valid on-demand USD price.
• Appends today's price each run.
"""

import boto3, csv, datetime as dt, gzip, json, requests
from pathlib import Path

# ---------- customise ----------
REGION_NAME   = "us-east-1"                 # Pricing API lives here
PREFERRED_RC  = "us-east-1"                 # Region code you care about
INSTANCE_TYPES = {                          # Add Blackwell when published
    "p5.48xlarge": "H100",
}
BACKFILL_START = dt.date(2023, 1, 1)
CSV_FILE       = Path("gpu_prices.csv")
# ---------------------------------

pricing = boto3.client("pricing", region_name=REGION_NAME)


# ---------- helpers ----------
def fetch_price_file(day: dt.date) -> dict | None:
    """Return the price-list JSON effective on <day> (or None)."""
    resp = pricing.list_price_lists(
        ServiceCode="AmazonEC2",
        CurrencyCode="USD",
        EffectiveDate=day.isoformat(),
        MaxResults=1,
    )
    if not resp["PriceLists"]:
        return None

    arn = resp["PriceLists"][0]["PriceListArn"]
    url = pricing.get_price_list_file_url(PriceListArn=arn, FileFormat="json")["Url"]

    blob = requests.get(url, timeout=60).content
    if blob[:2] == b"\x1f\x8b":              # gzip
        blob = gzip.decompress(blob)

    return json.loads(blob)


def extract_price(j: dict, instance: str) -> float | None:
    """Return on-demand USD price for <instance> from one price file."""
    # collect every SKU whose attributes match the instance type + Linux
    candidates = []
    for sku, prod in j["products"].items():
        attrs = prod["attributes"]
        if attrs.get("instanceType") == instance and attrs.get("operatingSystem") == "Linux":
            candidates.append((sku, attrs))

    if not candidates:
        return None

    # prefer the candidate in PREFERRED_RC if present
    candidates.sort(key=lambda x: x[1].get("regionCode") != PREFERRED_RC)
    sku = candidates[0][0]

    # pull its On-Demand term
    term = next(iter(j["terms"]["OnDemand"][sku].values()))
    dim  = next(iter(term["priceDimensions"].values()))
    return float(dim["pricePerUnit"]["USD"])


def load_existing_dates() -> set[str]:
    if not CSV_FILE.exists():
        return set()
    with CSV_FILE.open() as f:
        return {line.split(",")[0] for line in f.readlines()[1:]}


def write_rows(rows: list[dict]):
    new_file = not CSV_FILE.exists()
    with CSV_FILE.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "instance", "price_usd"])
        if new_file:
            w.writeheader()
        w.writerows(rows)


# ---------- main ----------
def main():
    existing = load_existing_dates()
    out_rows: list[dict] = []

    # ---- 1. monthly back-fill ----
    month = dt.date.today().replace(day=1)
    while month >= BACKFILL_START:
        stamp = month.isoformat()
        if stamp in existing:
            month = (month - dt.timedelta(days=1)).replace(day=1)
            continue

        plist = fetch_price_file(month)
        if plist:
            for inst in INSTANCE_TYPES:
                price = extract_price(plist, inst)
                if price is not None:          # only write real prices
                    out_rows.append({"timestamp": stamp,
                                     "instance": inst,
                                     "price_usd": price})

        month = (month - dt.timedelta(days=1)).replace(day=1)

    # ---- 2. today’s price ----
    today_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat()
    if today_iso not in existing:
        plist_now = fetch_price_file(dt.date.today())
        if plist_now:
            for inst in INSTANCE_TYPES:
                price = extract_price(plist_now, inst)
                if price is not None:
                    out_rows.append({"timestamp": today_iso,
                                     "instance": inst,
                                     "price_usd": price})

    # ---- write CSV ----
    if out_rows:
        write_rows(out_rows)
        print(f"Wrote {len(out_rows)} new rows.")
    else:
        print("No new data to add.")


if __name__ == "__main__":
    main()
