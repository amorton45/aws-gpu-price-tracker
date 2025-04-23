#!/usr/bin/env python3
"""
AWS GPU price tracker.
- Records on-demand USD price for each EC2 instance in INSTANCE_TYPES.
- Appends one row per run to gpu_prices.csv (UTC timestamp, instance, price).
"""
import boto3, csv, datetime as dt, json, os, sys

REGION_NAME = "us-east-1"       # Pricing API lives here, regardless of target region
LOCATION    = "US East (N. Virginia)"   # Change if you want a different AWS region
INSTANCE_TYPES = {
    "p5.48xlarge": "H100",
    # "p6.???": "Blackwell"  # leave placeholder; will start working once AWS posts prices
}

def get_price(client, instance_type, location=LOCATION):
    filters = [
        {"Type":"TERM_MATCH","Field":"instanceType","Value":instance_type},
        {"Type":"TERM_MATCH","Field":"location","Value":location},
        {"Type":"TERM_MATCH","Field":"operatingSystem","Value":"Linux"},
        {"Type":"TERM_MATCH","Field":"tenancy","Value":"Shared"},
        {"Type":"TERM_MATCH","Field":"preInstalledSw","Value":"NA"},
        {"Type":"TERM_MATCH","Field":"capacitystatus","Value":"Used"},
    ]
    resp = client.get_products(ServiceCode="AmazonEC2", Filters=filters, MaxResults=1)
    if not resp["PriceList"]:
        return None
    price_item = json.loads(resp["PriceList"][0])
    ondemand_term = next(iter(price_item["terms"]["OnDemand"].values()))
    pricedim      = next(iter(ondemand_term["priceDimensions"].values()))
    return float(pricedim["pricePerUnit"]["USD"])

def main():
    client = boto3.client("pricing", region_name=REGION_NAME)
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat()
    rows = []
    for inst in INSTANCE_TYPES:
        price = get_price(client, inst)
        rows.append({"timestamp": now, "instance": inst, "price_usd": price})
        print(f"{inst}: {price}")
    # append to CSV
    outfile = "gpu_prices.csv"
    new_file = not os.path.exists(outfile)
    with open(outfile, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp","instance","price_usd"])
        if new_file:
            writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
