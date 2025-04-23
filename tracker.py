#!/usr/bin/env python3
"""
AWS GPU-price tracker with historical back-fill.

* First run:
    – Walks back month-by-month (BACKFILL_START → today)
    – Adds ONE row per <month, instance> when a real on-demand USD price exists
* Every run:
    – Appends today’s price (UTC timestamp)
* CSV schema: timestamp, instance, price_usd
"""

import boto3
import csv
import datetime as dt
import gzip
import json
import requests
from pathlib import Path

# ─────────── configuration ───────────
REGION_NAME = "us-east-1"                # Pricing API endpoint
INSTANCE_TYPES = {                       # Add Blackwell when published
    "p5.48xlarge": "H100",
}
BACKFILL_START = dt.date(2023, 1, 1)     # earliest month to fetch
CSV_FILE = Path("gpu_prices.csv")
# ──────────────────────────────────────

pricing = boto3.client("pricing", region_name=REGION_NAME)


# ---------- helpers ----------
def fetch_price_file(day: dt.date) -> dict | None:
    """Return the price-list JSON effective on <day>, or None if AWS has none."""
    resp = pricing.list_price_lists(
        ServiceCode="AmazonEC2",
        CurrencyCode="USD",
        EffectiveDate=day.isoformat(),
        MaxResults=1,
    )
    if not resp["PriceLists"]:
        return None

    arn = resp["PriceLists"][0]["PriceListArn"]
    url = pricing.get_price_list_file_url(
        PriceListArn=arn,
        FileFormat="json",          # required
    )["Url"]

    blob = requests.get(url, timeout=60).content
    if blob[:2] == b"\x1f\x8b":     # gzip magic bytes
        blob = gzip.decompress(blob)

    return json.loads(blob)


def extract_price(j: dict, instance: str) -> float | None:
    """
    Return the on-demand USD price for <instance> from one price file.

    Strategy: find the FIRST SKU with matching instanceType + Linux OS.
    No region/location filter (ensures we get a price even if attributes vary).
    """
    for sku, prod in j["products"].items():
        attrs = prod["attributes"]
        if attrs.get("instanceType") == instance and attrs.get("operatingSystem") == "Linux":
            term = next(iter(j["terms"]["OnDemand"][sku].values()))
            dim  = next(iter(term["priceDimensions"].values()))
            return float(dim["pricePerUnit"]["USD"])
    return None


def load_existing_dates() -> set[str]:
    """Return timestamps already present in gpu_prices.csv."""
    if not CSV_FILE.exists():
        return set()
    with CSV_FILE.open() as f:
        return {line.split(",")[0] for line in f.readlines()[1:]}


def write_rows(rows: list[dict]) -> None:
    new_file = not CSV_FILE.exists()
    with CSV_FILE.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "instance", "price_usd"])
        if new_file:
            w.writeheader()
        w.writerows(rows)


# ---------- main ----------
def main() -> None:
    existing = load_existing_dates()
    out_rows: list[dict] = []

    # 1️⃣  Historical back-fill (month-level)
    month = dt.date.today().replace(day=1)
    while month >= BACKFILL_START:
        stamp = month.isoformat()
        if stamp in existing:                       # already recorded
            month = (month - dt.timedelta(days=1)).replace(day=1)
            continue

        plist = fetch_price_file(month)
        if plist:
            for inst in INSTANCE_TYPES:
                price = extract_price(plist, inst)
                if price is not None:               # record only real prices
                    out_rows.append(
                        {"timestamp": stamp, "instance": inst, "price_usd": price}
                    )

        month = (month - dt.timedelta(days=1)).replace(day=1)

    # 2️⃣  Today’s price (daily granularity)
    today_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat()
    if today_iso not in existing:
        plist_now = fetch_price_file(dt.date.today())
        if plist_now:
            for inst in INSTANCE_TYPES:
                price = extract_price(plist_now, inst)
                if price is not None:
                    out_rows.append(
                        {"timestamp": today_iso, "instance": inst, "price_usd": price}
                    )

    # 3️⃣  Write results
    if out_rows:
        write_rows(out_rows)
        print(f"Wrote {len(out_rows)} new rows.")
    else:
        print("No new data to add.")


if __name__ == "__main__":
    main()
