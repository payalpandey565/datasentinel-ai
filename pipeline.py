
import json
import sys
from datetime import datetime
from pathlib import Path

import requests

from silver_layer import run_silver_pipeline
from gold_layer import run_gold_pipeline
from data_quality import run_data_quality_pipeline


BASE_DIR = Path(__file__).resolve().parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"

API_ENDPOINTS = {
    "products": "https://dummyjson.com/products",
    "carts": "https://dummyjson.com/carts",
}


def fetch_live_data(endpoint):
    """Fetch every page of data from a paginated API."""
    records = []
    limit = 100
    skip = 0

    while True:
        response = requests.get(
            endpoint,
            params={"limit": limit, "skip": skip},
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()
        batch = payload.get("products", payload.get("carts", []))
        records.extend(batch)

        total = payload.get("total", len(records))
        skip += len(batch)

        if not batch or skip >= total:
            break

    return records


def run_bronze_ingestion():
    """Fetch fresh API data and store the raw JSON in Bronze."""
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    run_dir = BRONZE_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    results = {}

    for name, endpoint in API_ENDPOINTS.items():
        print(f"Fetching live {name} data...")

        records = fetch_live_data(endpoint)
        results[name] = records

        output_path = run_dir / f"{name}.json"

        with output_path.open("w", encoding="utf-8") as file:
            json.dump(records, file, indent=2, ensure_ascii=False)

        print(f"  Retrieved {len(records)} records")
        print(f"  Saved to {output_path}")

    metadata = {
        "run_id": run_id,
        "ingestion_timestamp": datetime.now().isoformat(),
        "source": "DummyJSON REST API",
        "products_count": len(results["products"]),
        "carts_count": len(results["carts"]),
        "status": "SUCCESS",
    }

    with (run_dir / "metadata.json").open(
        "w", encoding="utf-8"
    ) as file:
        json.dump(metadata, file, indent=2)

    print(f"\nBronze ingestion completed: {run_id}")
    return run_id


def run_full_pipeline():
    """Execute all pipeline layers in sequence."""
    started_at = datetime.now()

    print("\n" + "=" * 60)
    print("             DATASENTINEL AI PIPELINE")
    print("=" * 60)

    try:
        print("\n[1/4] BRONZE — Live API ingestion")
        run_bronze_ingestion()

        print("\n[2/4] SILVER — PySpark data cleaning")
        run_silver_pipeline()

        print("\n[3/4] GOLD — Business analytics")
        run_gold_pipeline()

        print("\n[4/4] DATA QUALITY — Validation and anomaly checks")
        run_data_quality_pipeline()

        elapsed = (datetime.now() - started_at).total_seconds()

        print("\n" + "=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print(f"Execution time: {elapsed:.2f} seconds")
        print("=" * 60)

        return True

    except Exception as error:
        print("\n" + "=" * 60)
        print("PIPELINE FAILED")
        print(f"Error: {error}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    run_full_pipeline()

