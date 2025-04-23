#!/usr/bin/env python3
"""
AWS GPU price tracker + historical back-fill (since 2023-01-01).

• First run: walks backward month-by-month, loads each historical
  price file, and appends the price that was effective that month.
• Every run: records today's price.
"""

import boto3, csv, datetime as dt, json, os, requests
from pathlib import Path

# ---------- customise here ----------
REGION_NAME = "us-east-1"                # Pricing API lives here
LOCATION    = "US East (N. Virginia)"    # AWS region to price
INSTANCE_TYPES = {
    "p5.48xlarge": "H100",               # add Blackwell when AWS announces
}
BACKFILL_START = dt.date(2023, 1, 1)     # earliest month you want
# -------------------------------------

pricing = boto3.client("pricing", region_name=REGION_NAME)
CSV_FILE = Path("gpu_prices.csv")


def fetch_price_file(day: dt.date) -> dict:
    """Download the price-list JSON that was effective on <day>."""
    resp = pricing.list_price_lists(
        ServiceCode="AmazonEC2",
        EffectiveDate=day.isoformat(),
        CurrencyCode="USD",
        RegionCode=REGION_NAME,
        MaxResults=1,
    )
    arn = resp["PriceLists"][0]["PriceListArn"]
    url = pricing.get_price_list_file_url(PriceListArn=arn)["Url"]
    return requests.get(url, timeout=30).json()     # ~ a few MB


def extract_price(j: dict, instance: str) -> float | None:
    """Pull the on-demand USD price for <instance> from a price file."""
    # 1. find the SKU for our instance in the products section
    sku = next(
        (
            s
            for s, p in j["products"].items()
            if p["attributes"].get("instanceType") == instance
            and p["attributes"].get("location") == LOCATION
            and p["attributes"].get("operatingSystem") == "Linux"
        ),
        None,
    )
    if not sku:
        return None

    # 2. look up that SKU in the OnDemand terms
    term = next(iter(j["terms"]["OnDemand"][sku].values()))
    dim  = next(iter(term["priceDimensions"].values()))
    return float(dim["pricePerUnit"]["USD"])


def load_existing_dates() -> set[str]:
    if not CSV_FILE.exists():
        return set()
    with CSV_FILE.open() as f:
        return {row.split(",")[0] for row in f.readlines()[1:]}   # timestamps


def write_rows(rows: list[dict]):
    new_file = not CSV_FILE.exists()
    with CSV_FILE.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "instance", "price_usd"])
        if new_file:
            w.writeheader()
        w.writerows(rows)


def main():
    existing_dates = load_existing_dates()
    out_rows = []

    # ---------- STEP 1: back-fill ----------
    month = dt.date.today().replace(day=1)
    while month >= BACKFILL_START:
        stamp = month.isoformat()
        if stamp in existing_dates:
            month = (month - dt.timedelta(days=1)).replace(day=1)
            continue       # already have this month
        price_file = fetch_price_file(month)
        for inst in INSTANCE_TYPES:
            price = extract_price(price_file, inst)
            out_rows.append({"timestamp": stamp, "instance": inst,
                             "price_usd": price})
        month = (month - dt.timedelta(days=1)).replace(day=1)

    # ---------- STEP 2: today's price ----------
    today = dt.datetime.utcnow().replace(microsecond=0).isoformat()
    if today not in existing_dates:      # one row per day
        for inst in INSTANCE_TYPES:
            price = extract_price(fetch_price_file(dt.date.today()), inst)
            out_rows.append({"timestamp": today, "instance": inst,
                             "price_usd": price})

    # ---------- write & done ----------
    if out_rows:
        write_rows(out_rows)
        print(f"Wrote {len(out_rows)} new rows.")
    else:
        print("No new data to add.")


if __name__ == "__main__":
    main()
