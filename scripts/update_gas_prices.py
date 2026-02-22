#!/usr/bin/env python3
"""
Costco Gas Price Tracker

Fetches all US Costco gas station locations via the Costco warehouse API,
extracts any available gas price data, and appends a daily snapshot to
data_clean/gas_prices_history.csv.

Note: Costco does not always publish gas prices through their public API.
On the first run, inspect the "Sample API fields" output to see which fields
are actually returned — then update extract_prices() accordingly.
"""

import os
import json
from datetime import date

import pandas as pd
import requests


CSV_PATH = "data_clean/gas_prices_history.csv"

COLUMNS = [
    "date", "store_id", "store_name", "city", "state", "zip",
    "lat", "lon", "regular_price", "premium_price", "diesel_price",
]

WAREHOUSE_API = "https://www.costco.com/AjaxWarehouseBrowseLookupView"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, */*",
    "Referer": "https://www.costco.com/warehouse-locations.html",
}


def fetch_all_gas_stations() -> list[dict]:
    """
    Return all US Costco locations that have a gas station.
    Searches from the geographic center of the contiguous US with a high
    result cap to capture all stores in a single request.
    """
    resp = requests.get(
        WAREHOUSE_API,
        headers=HEADERS,
        params={
            "langId": "-1",
            "storeId": "10301",
            "numOfWarehouses": "1000",
            "hasGas": "true",
            "hasTires": "false",
            "hasFoodCourt": "false",
            "hasHearingAids": "false",
            "hasPharmacy": "false",
            "hasOptical": "false",
            "hasOpenDrive": "false",
            "latitude": "39.83",   # geographic center of contiguous US
            "longitude": "-98.58",
            "countryCode": "US",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _to_float(val) -> float | None:
    """Convert a value to float, returning None if not parseable."""
    if val is None or val == "" or val == "N/A":
        return None
    try:
        return float(str(val).replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def extract_prices(store: dict) -> tuple[float | None, float | None, float | None]:
    """
    Try to extract regular, premium, and diesel prices from a store record.
    Field names below are common candidates — update if the API uses different names.
    Run the script once and check the "Sample API fields" output to confirm.
    """
    regular = _to_float(
        store.get("gasPrice")
        or store.get("regularGasPrice")
        or store.get("regularPrice")
        or store.get("gasolinePrice")
        or store.get("unleadedPrice")
    )
    premium = _to_float(
        store.get("premiumGasPrice")
        or store.get("premiumPrice")
    )
    diesel = _to_float(
        store.get("dieselPrice")
        or store.get("diesel")
    )
    return regular, premium, diesel


def load_existing() -> pd.DataFrame:
    if os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0:
        return pd.read_csv(CSV_PATH)
    return pd.DataFrame(columns=COLUMNS)


def main():
    today = date.today().isoformat()
    print(f"Costco gas price tracker — {today}")

    stores = fetch_all_gas_stations()
    print(f"Found {len(stores)} gas station locations.")

    # On first run (or for debugging), print the raw fields returned for one store
    # so you can identify gas price field names and update extract_prices() above.
    if stores:
        print("\nSample API fields from first store:")
        print(json.dumps(stores[0], indent=2))
        print()

    rows = []
    for store in stores:
        regular, premium, diesel = extract_prices(store)
        rows.append({
            "date":          today,
            "store_id":      store.get("storeNumber", ""),
            "store_name":    store.get("displayName", ""),
            "city":          store.get("city", ""),
            "state":         store.get("state", ""),
            "zip":           store.get("zipCode", ""),
            "lat":           store.get("latitude", ""),
            "lon":           store.get("longitude", ""),
            "regular_price": regular,
            "premium_price": premium,
            "diesel_price":  diesel,
        })

    df_new = pd.DataFrame(rows, columns=COLUMNS)
    df_existing = load_existing()
    df_out = pd.concat([df_existing, df_new], ignore_index=True)
    df_out.to_csv(CSV_PATH, index=False)

    prices_found = df_new["regular_price"].notna().sum()
    print(
        f"Saved {len(rows)} store records. "
        f"Prices captured: {prices_found}/{len(rows)}. "
        f"CSV total rows: {len(df_out)}."
    )
    if prices_found == 0:
        print(
            "\nNo gas prices found in the API response. "
            "Check the 'Sample API fields' output above to identify the correct "
            "field names, then update extract_prices() in this script."
        )


if __name__ == "__main__":
    main()
