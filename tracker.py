#!/usr/bin/env python3
"""
AWS GPU-price tracker + historical back-fill.

• On the first run it walks backward month-by-month (default: Jan 2023 → today)
  and records the on-demand USD price that was effective each month.
• Every run (including the first) appends *today’s* price.
• Subsequent daily runs add one new row.

CSV schema: timestamp, instance, price_usd
"""

import boto3
import csv
import datetime as dt
import gzip
import json
import os
import requests
from pathlib import Path

# --------- customise here ----------
REGION_NAME   = "us-east-1"                # Pricing API lives here
LOCATION      = "US East (N. Virginia)"    # Which AWS region’s price you want
INSTANCE_TYPES = {
    "p5.48xlarge": "H100",
    # add Blackwell instance type when AWS publishes it, e.g. "p6d.??": "B100"
}
BACKFILL_START = dt.date(2023, 1, 1)       # earliest month you care about
CSV_FILE       = Path("gpu_prices.csv")
# ------------------------------------

pricing = boto3.client("pricing", region_name=REGION_NAME)


# ---------- helpers ----------
def fetch_price_file(day: dt.date) -> dict | None:
    """
    Download the JSON price list that was effective on <day>.
    Returns None if AWS has no file for that date.
    """
    resp = pricing.list_price_lists(
        ServiceCode="AmazonEC2",
        CurrencyCode="USD",
        EffectiveDate=day.isoformat(),
        MaxResults=1,
    )
    if not resp["PriceLists"]:
        return None  # <— graceful skip

    arn = resp["PriceLists"][0]["PriceListArn"]
    url = pricing.get_price_list_file_url(
        PriceListArn=arn,
        FileFormat="json"       # required as of 2024-12
    )["Url"]

    blob = requests.get(url, timeout=60).content
    if blob[:2] == b"\x1f\x8b":          # gzip magic bytes
        blob = gzip.decompress(blob)

    return json.loads(blob)


def extract_price(price_json: dict, instance: str) -> float | None:
    """Return on-demand USD price for <instance> inside one price file."""
    # 1. find the SKU
    sku = next(
        (
            s
            for s, p in price_json["products"].items()
            if p["attributes"].get("instanceType") == instance
            and p["attributes"].get("location") == LOCATION
            and p["attributes"].get("operatingSystem") == "Linux"
        ),
        None,
    )
    if not sku:
        return None

    # 2. read its On-Demand term
    term = next(iter(price_json["terms"]["OnDemand"][sku].values()))
    dim  = next(iter(term["priceDimensions"].values()))
    return float(dim["pricePerUnit"]["USD"])


def load_existing_dates() -> set[str]:
    """Return the set of timestamp strings already in the CSV."""
    if not CSV_FILE.exists():
        return set()
    with CSV_FILE.open() as f:
        return {row.split(",")[0] for row in f.readlines()[1:]}


def write_rows(rows: list[dict]):
    new_file = not CSV_FILE.exists()
    with CSV_FILE.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "instance", "price_usd"])
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


# ---------- main ----------
def main():
    existing = load_existing_dates()
    out_rows: list[dict] = []

    # ---- STEP 1: historical back-fill ----
    month = dt.date.today().replace(day=1)
    while month >= BACKFILL_START:
        stamp = month.isoformat()
        if stamp in existing:
            month = (month - dt.timedelta(days=1)).replace(day=1)
            continue

        price_file = fetch_price_file(month)
        if not price_file:          # no list for that month
            month = (month - dt.timedelta(days=1)).replace(day=1)
            continue

        for inst in INSTANCE_TYPES:
            price = extract_price(price_file, inst)
            out_rows.append(
                {"timestamp": stamp, "instance": inst, "price_usd": price}
            )

        month = (month - dt.timedelta(days=1)).replace(day=1)

    # ---- STEP 2: today's price ----
    today_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat()
    if today_iso not in existing:
        price_file = fetch_price_file(dt.date.today())
        if price_file:
            for inst in INSTANCE_TYPES:
                price = extract_price(price_file, inst)
                out_rows.append(
                    {"timestamp": today_iso, "instance": inst, "price_usd": price}
                )

    # ---- write to CSV ----
    if out_rows:
        write_rows(out_rows)
        print(f"Wrote {len(out_rows)} new rows.")
    else:
        print("No new data to add.")


if __name__ == "__main__":
    main()
