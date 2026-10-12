import json
from datetime import datetime
from pathlib import Path

import pandas as pd


# =========================================================
# DATASENTINEL AI - GOLD LAYER
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

LOW_STOCK_THRESHOLD = 10


# =========================================================
# FIND LATEST SILVER RUN
# =========================================================

def find_latest_silver_run():
    """Find the latest Silver run containing both Parquet datasets."""

    if not SILVER_DIR.exists():
        raise FileNotFoundError(
            f"Silver directory not found: {SILVER_DIR}. "
            "Run silver_layer.py first."
        )

    valid_runs = []

    for run_dir in SILVER_DIR.iterdir():
        products_file = (
            run_dir / "products" / "part-00000.parquet"
        )

        carts_file = (
            run_dir / "carts" / "part-00000.parquet"
        )

        if products_file.exists() and carts_file.exists():
            valid_runs.append(run_dir)

    if not valid_runs:
        raise FileNotFoundError(
            "No valid Silver datasets found. "
            "Run silver_layer.py first."
        )

    return max(
        valid_runs,
        key=lambda path: path.stat().st_mtime,
    )


# =========================================================
# LOAD SILVER DATA
# =========================================================

def load_silver_data(silver_run):
    """Load Silver Parquet files into Pandas DataFrames."""

    products_path = (
        silver_run / "products" / "part-00000.parquet"
    )

    carts_path = (
        silver_run / "carts" / "part-00000.parquet"
    )

    products_df = pd.read_parquet(products_path)
    carts_df = pd.read_parquet(carts_path)

    return products_df, carts_df


# =========================================================
# PRODUCT CATEGORY ANALYTICS
# =========================================================

def create_category_metrics(products_df):
    """Calculate product-level metrics grouped by category."""

    category_metrics = (
        products_df.groupby("category", dropna=False)
        .agg(
            total_products=("id", "count"),
            average_price=("price", "mean"),
            minimum_price=("price", "min"),
            maximum_price=("price", "max"),
            total_inventory=("stock", "sum"),
            average_rating=("rating", "mean"),
        )
        .reset_index()
    )

    numeric_columns = [
        "average_price",
        "minimum_price",
        "maximum_price",
        "average_rating",
    ]

    category_metrics[numeric_columns] = (
        category_metrics[numeric_columns].round(2)
    )

    return category_metrics.sort_values(
        "total_products",
        ascending=False,
    )


# =========================================================
# LOW-STOCK ANALYTICS
# =========================================================

def identify_low_stock_products(products_df):
    """Identify products whose stock is below the threshold."""

    low_stock_df = products_df[
        products_df["stock"] < LOW_STOCK_THRESHOLD
    ].copy()

    return low_stock_df.sort_values(
        "stock",
        ascending=True,
    )


# =========================================================
# CART ANALYTICS
# =========================================================

def create_cart_metrics(carts_df):
    """Create a summary of cart activity."""

    if carts_df.empty:
        return pd.DataFrame(
            columns=[
                "metric",
                "value",
            ]
        )

    total_carts = len(carts_df)

    total_cart_value = float(carts_df["total"].sum())

    average_cart_value = float(carts_df["total"].mean())

    median_cart_value = float(carts_df["total"].median())

    highest_cart_value = float(carts_df["total"].max())

    total_products_in_carts = int(
        carts_df["totalProducts"].sum()
    )

    total_items_in_carts = int(
        carts_df["totalQuantity"].sum()
    )

    unique_users = int(carts_df["userId"].nunique())

    metrics = [
        {
            "metric": "total_carts",
            "value": total_carts,
        },
        {
            "metric": "total_cart_value",
            "value": round(total_cart_value, 2),
        },
        {
            "metric": "average_cart_value",
            "value": round(average_cart_value, 2),
        },
        {
            "metric": "median_cart_value",
            "value": round(median_cart_value, 2),
        },
        {
            "metric": "highest_cart_value",
            "value": round(highest_cart_value, 2),
        },
        {
            "metric": "total_products_in_carts",
            "value": total_products_in_carts,
        },
        {
            "metric": "total_items_in_carts",
            "value": total_items_in_carts,
        },
        {
            "metric": "unique_users",
            "value": unique_users,
        },
    ]

    return pd.DataFrame(metrics)


# =========================================================
# CART VALUE DISTRIBUTION
# =========================================================

def create_cart_value_distribution(carts_df):
    """Group carts into value bands."""

    result_columns = [
        "cart_value_band",
        "cart_count",
        "total_value",
        "average_value",
    ]

    if carts_df.empty:
        return pd.DataFrame(columns=result_columns)

    bands = [
        0,
        100,
        500,
        1000,
        float("inf"),
    ]

    labels = [
        "0-100",
        "100-500",
        "500-1000",
        "1000+",
    ]

    carts = carts_df.copy()

    carts["cart_value_band"] = pd.cut(
        carts["total"],
        bins=bands,
        labels=labels,
        include_lowest=True,
        right=False,
    )

    distribution = (
        carts.groupby(
            "cart_value_band",
            observed=False,
        )
        .agg(
            cart_count=("id", "count"),
            total_value=("total", "sum"),
            average_value=("total", "mean"),
        )
        .reset_index()
    )

    distribution["total_value"] = (
        distribution["total_value"].round(2)
    )

    distribution["average_value"] = (
        distribution["average_value"].round(2)
    )

    return distribution


# =========================================================
# DATA QUALITY SUMMARY
# =========================================================

def create_data_quality_summary(products_df, carts_df):
    """Calculate basic data quality checks on Silver data."""

    product_duplicate_ids = int(
        products_df["id"].duplicated().sum()
    )

    cart_duplicate_ids = int(
        carts_df["id"].duplicated().sum()
    )

    product_missing_values = {
        column: int(count)
        for column, count in (
            products_df.isna().sum().items()
        )
    }

    cart_missing_values = {
        column: int(count)
        for column, count in (
            carts_df.isna().sum().items()
        )
    }

    product_negative_prices = int(
        (products_df["price"] < 0).sum()
    )

    product_negative_stock = int(
        (products_df["stock"] < 0).sum()
    )

    cart_negative_totals = int(
        (carts_df["total"] < 0).sum()
    )

    return {
        "products": {
            "record_count": len(products_df),
            "duplicate_ids": product_duplicate_ids,
            "missing_values": product_missing_values,
            "negative_prices": product_negative_prices,
            "negative_stock_values": product_negative_stock,
        },
        "carts": {
            "record_count": len(carts_df),
            "duplicate_ids": cart_duplicate_ids,
            "missing_values": cart_missing_values,
            "negative_totals": cart_negative_totals,
        },
    }


# =========================================================
# GENERATE GOLD OUTPUTS
# =========================================================

def run_gold_pipeline():
    """Execute the Silver-to-Gold analytics pipeline."""

    print("\n" + "=" * 60)
    print("DATASENTINEL AI - GOLD PIPELINE")
    print("=" * 60)

    # -----------------------------------------------------
    # STEP 1: FIND SILVER DATA
    # -----------------------------------------------------

    silver_run = find_latest_silver_run()

    print(f"\nSilver input: {silver_run}")

    # -----------------------------------------------------
    # STEP 2: LOAD SILVER DATA
    # -----------------------------------------------------

    products_df, carts_df = load_silver_data(silver_run)

    print(f"Silver products loaded: {len(products_df)}")
    print(f"Silver carts loaded: {len(carts_df)}")

    # -----------------------------------------------------
    # STEP 3: CALCULATE GOLD METRICS
    # -----------------------------------------------------

    print("\nCalculating product category metrics...")

    category_metrics = create_category_metrics(
        products_df
    )

    print("Identifying low-stock products...")

    low_stock_df = identify_low_stock_products(
        products_df
    )

    print("Calculating cart metrics...")

    cart_metrics = create_cart_metrics(carts_df)

    print("Calculating cart value distribution...")

    cart_distribution = create_cart_value_distribution(
        carts_df
    )

    print("Running data quality checks...")

    data_quality = create_data_quality_summary(
        products_df,
        carts_df,
    )

    # -----------------------------------------------------
    # STEP 4: CREATE GOLD OUTPUT DIRECTORY
    # -----------------------------------------------------

    run_timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    gold_run_dir = GOLD_DIR / run_timestamp

    gold_run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # -----------------------------------------------------
    # STEP 5: SAVE PARQUET OUTPUTS
    # -----------------------------------------------------

    category_metrics.to_parquet(
        gold_run_dir / "product_category_metrics.parquet",
        index=False,
        engine="pyarrow",
    )

    low_stock_df.to_parquet(
        gold_run_dir / "low_stock_products.parquet",
        index=False,
        engine="pyarrow",
    )

    cart_metrics.to_parquet(
        gold_run_dir / "cart_metrics.parquet",
        index=False,
        engine="pyarrow",
    )

    cart_distribution.to_parquet(
        gold_run_dir / "cart_value_distribution.parquet",
        index=False,
        engine="pyarrow",
    )

    # -----------------------------------------------------
    # STEP 6: CREATE PIPELINE SUMMARY
    # -----------------------------------------------------

    summary = {
        "pipeline": "DataSentinel AI Gold Pipeline",
        "status": "SUCCESS",
        "run_timestamp": run_timestamp,
        "silver_input_path": str(silver_run),
        "gold_output_path": str(gold_run_dir),
        "input_counts": {
            "products": len(products_df),
            "carts": len(carts_df),
        },
        "gold_metrics": {
            "product_categories": len(category_metrics),
            "low_stock_products": len(low_stock_df),
            "total_carts": len(carts_df),
            "cart_metrics_generated": len(cart_metrics),
        },
        "data_quality": data_quality,
    }

    summary_path = gold_run_dir / "summary.json"

    with open(
        summary_path,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            summary,
            file,
            indent=4,
            allow_nan=False,
        )

    # -----------------------------------------------------
    # STEP 7: DISPLAY RESULTS
    # -----------------------------------------------------

    print("\n" + "-" * 60)
    print("PRODUCT CATEGORY METRICS")
    print("-" * 60)

    print(
        category_metrics.to_string(index=False)
    )

    print("\n" + "-" * 60)
    print("LOW-STOCK PRODUCTS")
    print("-" * 60)

    if low_stock_df.empty:
        print("No low-stock products found.")
    else:
        print(
            low_stock_df[
                ["id", "title", "category", "stock", "price"]
            ].to_string(index=False)
        )

    print("\n" + "-" * 60)
    print("CART METRICS")
    print("-" * 60)

    print(
        cart_metrics.to_string(index=False)
    )

    print("\n" + "-" * 60)
    print("CART VALUE DISTRIBUTION")
    print("-" * 60)

    print(
        cart_distribution.to_string(index=False)
    )

    # -----------------------------------------------------
    # STEP 8: FINAL STATUS
    # -----------------------------------------------------

    print("\n" + "=" * 60)
    print("GOLD PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)

    print(f"\nGold output directory: {gold_run_dir}")
    print(f"Summary file: {summary_path}")

    print("\nGenerated files:")
    print("- product_category_metrics.parquet")
    print("- low_stock_products.parquet")
    print("- cart_metrics.parquet")
    print("- cart_value_distribution.parquet")
    print("- summary.json")

    return summary


# =========================================================
# SCRIPT ENTRY POINT
# =========================================================

if __name__ == "__main__":
    run_gold_pipeline()