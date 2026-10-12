import json
from datetime import datetime
from pathlib import Path

import pandas as pd


# =========================================================
# DATASENTINEL AI - DATA QUALITY & ANOMALY DETECTION
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

REPORT_DIR = BASE_DIR / "reports"

LOW_STOCK_THRESHOLD = 10
HIGH_CART_VALUE_THRESHOLD = 10000
MIN_EXPECTED_PRODUCT_COUNT = 1
MIN_EXPECTED_CART_COUNT = 1


# =========================================================
# FIND LATEST VALID RUNS
# =========================================================

def find_latest_bronze_run():
    if not BRONZE_DIR.exists():
        raise FileNotFoundError(
            f"Bronze directory not found: {BRONZE_DIR}"
        )

    runs = [
        folder
        for folder in BRONZE_DIR.iterdir()
        if folder.is_dir()
        and (folder / "products.json").exists()
        and (folder / "carts.json").exists()
    ]

    if not runs:
        raise FileNotFoundError(
            "No valid Bronze runs found."
        )

    return max(runs, key=lambda folder: folder.stat().st_mtime)


def find_latest_silver_run():
    if not SILVER_DIR.exists():
        raise FileNotFoundError(
            f"Silver directory not found: {SILVER_DIR}"
        )

    runs = [
        folder
        for folder in SILVER_DIR.iterdir()
        if folder.is_dir()
        and (
            folder / "products" / "part-00000.parquet"
        ).exists()
        and (
            folder / "carts" / "part-00000.parquet"
        ).exists()
    ]

    if not runs:
        raise FileNotFoundError(
            "No valid Silver runs found."
        )

    return max(runs, key=lambda folder: folder.stat().st_mtime)


def find_latest_gold_run():
    if not GOLD_DIR.exists():
        raise FileNotFoundError(
            f"Gold directory not found: {GOLD_DIR}"
        )

    runs = [
        folder
        for folder in GOLD_DIR.iterdir()
        if folder.is_dir()
        and (folder / "summary.json").exists()
    ]

    if not runs:
        raise FileNotFoundError(
            "No valid Gold runs found."
        )

    return max(runs, key=lambda folder: folder.stat().st_mtime)


# =========================================================
# LOAD PIPELINE DATA
# =========================================================

def load_bronze_data(bronze_run):
    with open(
        bronze_run / "products.json",
        "r",
        encoding="utf-8",
    ) as file:
        products = json.load(file)

    with open(
        bronze_run / "carts.json",
        "r",
        encoding="utf-8",
    ) as file:
        carts = json.load(file)

    return products, carts


def load_silver_data(silver_run):
    products = pd.read_parquet(
        silver_run / "products" / "part-00000.parquet"
    )

    carts = pd.read_parquet(
        silver_run / "carts" / "part-00000.parquet"
    )

    return products, carts


def load_gold_summary(gold_run):
    with open(
        gold_run / "summary.json",
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


# =========================================================
# ALERT CREATION
# =========================================================

def create_alert(
    alerts,
    severity,
    category,
    title,
    description,
    affected_records=0,
):
    alerts.append({
        "severity": severity,
        "category": category,
        "title": title,
        "description": description,
        "affected_records": int(affected_records),
        "timestamp": datetime.now().isoformat(
            timespec="seconds"
        ),
    })


# =========================================================
# BRONZE DATA QUALITY CHECKS
# =========================================================

def check_bronze_quality(products, carts, alerts):
    """
    Inspect raw Bronze records before transformation.
    """

    if not products:
        create_alert(
            alerts,
            "CRITICAL",
            "Completeness",
            "No product records received",
            "The Bronze product dataset is empty.",
        )

    if not carts:
        create_alert(
            alerts,
            "CRITICAL",
            "Completeness",
            "No cart records received",
            "The Bronze cart dataset is empty.",
        )

    # Product ID checks.
    product_ids = [
        item.get("id")
        for item in products
        if isinstance(item, dict)
        and item.get("id") is not None
    ]

    duplicate_product_ids = (
        pd.Series(product_ids).duplicated().sum()
        if product_ids
        else 0
    )

    if duplicate_product_ids:
        create_alert(
            alerts,
            "HIGH",
            "Uniqueness",
            "Duplicate product IDs detected",
            "Multiple Bronze product records share the same ID.",
            duplicate_product_ids,
        )

    # Product field checks.
    missing_product_titles = sum(
        1
        for item in products
        if not isinstance(item, dict)
        or not str(item.get("title") or "").strip()
    )

    if missing_product_titles:
        create_alert(
            alerts,
            "HIGH",
            "Completeness",
            "Missing product titles",
            "Some product records have missing or empty titles.",
            missing_product_titles,
        )

    invalid_prices = 0

    for item in products:
        if not isinstance(item, dict):
            invalid_prices += 1
            continue

        try:
            price = float(item.get("price"))

            if (
                not pd.notna(price)
                or price < 0
                or price == float("inf")
                or price == float("-inf")
            ):
                invalid_prices += 1

        except (ValueError, TypeError, OverflowError):
            invalid_prices += 1

    if invalid_prices:
        create_alert(
            alerts,
            "HIGH",
            "Validity",
            "Invalid product prices",
            "Product prices are missing, nonnumeric, negative, "
            "or non-finite.",
            invalid_prices,
        )

    # Cart ID checks.
    cart_ids = [
        item.get("id")
        for item in carts
        if isinstance(item, dict)
        and item.get("id") is not None
    ]

    duplicate_cart_ids = (
        pd.Series(cart_ids).duplicated().sum()
        if cart_ids
        else 0
    )

    if duplicate_cart_ids:
        create_alert(
            alerts,
            "HIGH",
            "Uniqueness",
            "Duplicate cart IDs detected",
            "Multiple Bronze cart records share the same ID.",
            duplicate_cart_ids,
        )

    # Cart value checks.
    invalid_cart_totals = 0

    for item in carts:
        if not isinstance(item, dict):
            invalid_cart_totals += 1
            continue

        try:
            total = float(item.get("total"))

            if (
                not pd.notna(total)
                or total < 0
                or total == float("inf")
                or total == float("-inf")
            ):
                invalid_cart_totals += 1

        except (ValueError, TypeError, OverflowError):
            invalid_cart_totals += 1

    if invalid_cart_totals:
        create_alert(
            alerts,
            "HIGH",
            "Validity",
            "Invalid cart totals",
            "Cart totals are missing, nonnumeric, negative, "
            "or non-finite.",
            invalid_cart_totals,
        )


# =========================================================
# SILVER DATA QUALITY CHECKS
# =========================================================

def check_silver_quality(products, carts, alerts):
    """
    Validate the cleaned Silver datasets.
    """

    if len(products) < MIN_EXPECTED_PRODUCT_COUNT:
        create_alert(
            alerts,
            "CRITICAL",
            "Completeness",
            "Insufficient Silver product records",
            "The Silver product dataset is empty or unexpectedly small.",
            len(products),
        )

    if len(carts) < MIN_EXPECTED_CART_COUNT:
        create_alert(
            alerts,
            "CRITICAL",
            "Completeness",
            "Insufficient Silver cart records",
            "The Silver cart dataset is empty or unexpectedly small.",
            len(carts),
        )

    # Duplicate IDs should not remain after Silver cleaning.
    product_duplicates = int(
        products["id"].duplicated().sum()
    )

    if product_duplicates:
        create_alert(
            alerts,
            "CRITICAL",
            "Uniqueness",
            "Duplicate product IDs remain in Silver",
            "Silver contains duplicate product IDs after cleaning.",
            product_duplicates,
        )

    cart_duplicates = int(
        carts["id"].duplicated().sum()
    )

    if cart_duplicates:
        create_alert(
            alerts,
            "CRITICAL",
            "Uniqueness",
            "Duplicate cart IDs remain in Silver",
            "Silver contains duplicate cart IDs after cleaning.",
            cart_duplicates,
        )

    # Check required product fields.
    required_product_columns = [
        "id",
        "title",
        "category",
        "price",
        "stock",
    ]

    for column in required_product_columns:
        if column not in products.columns:
            create_alert(
                alerts,
                "CRITICAL",
                "Schema",
                f"Missing product column: {column}",
                f"The Silver product dataset is missing '{column}'.",
            )
            continue

        missing_count = int(
            products[column].isna().sum()
        )

        if missing_count:
            create_alert(
                alerts,
                "HIGH",
                "Completeness",
                f"Missing values in product {column}",
                f"Silver products contain missing values in '{column}'.",
                missing_count,
            )

    # Check required cart fields.
    required_cart_columns = [
        "id",
        "userId",
        "total",
        "totalProducts",
        "totalQuantity",
    ]

    for column in required_cart_columns:
        if column not in carts.columns:
            create_alert(
                alerts,
                "CRITICAL",
                "Schema",
                f"Missing cart column: {column}",
                f"The Silver cart dataset is missing '{column}'.",
            )
            continue

        missing_count = int(
            carts[column].isna().sum()
        )

        if missing_count:
            create_alert(
                alerts,
                "HIGH",
                "Completeness",
                f"Missing values in cart {column}",
                f"Silver carts contain missing values in '{column}'.",
                missing_count,
            )

    # Range checks.
    if "price" in products.columns:
        negative_prices = int(
            (products["price"] < 0).sum()
        )

        if negative_prices:
            create_alert(
                alerts,
                "CRITICAL",
                "Validity",
                "Negative prices in Silver",
                "Product prices must not be negative.",
                negative_prices,
            )

    if "stock" in products.columns:
        negative_stock = int(
            (products["stock"] < 0).sum()
        )

        if negative_stock:
            create_alert(
                alerts,
                "HIGH",
                "Validity",
                "Negative inventory in Silver",
                "Product stock quantities must not be negative.",
                negative_stock,
            )

    if "total" in carts.columns:
        negative_totals = int(
            (carts["total"] < 0).sum()
        )

        if negative_totals:
            create_alert(
                alerts,
                "CRITICAL",
                "Validity",
                "Negative cart totals in Silver",
                "Cart totals must not be negative.",
                negative_totals,
            )


# =========================================================
# PIPELINE RECONCILIATION
# =========================================================

def check_pipeline_reconciliation(
    bronze_products,
    bronze_carts,
    silver_products,
    silver_carts,
    alerts,
):
    """
    Compare Bronze and Silver counts.

    Differences can be legitimate because Silver cleaning
    may reject invalid records or remove duplicates.
    """

    bronze_product_count = len(bronze_products)
    bronze_cart_count = len(bronze_carts)

    silver_product_count = len(silver_products)
    silver_cart_count = len(silver_carts)

    product_difference = (
        bronze_product_count - silver_product_count
    )

    cart_difference = (
        bronze_cart_count - silver_cart_count
    )

    if product_difference < 0:
        create_alert(
            alerts,
            "HIGH",
            "Reconciliation",
            "Silver product count exceeds Bronze",
            "The Silver product count exceeds the Bronze count. "
            "Investigate the source data and pipeline runs.",
            abs(product_difference),
        )

    elif product_difference > 0:
        create_alert(
            alerts,
            "INFO",
            "Reconciliation",
            "Product records removed during cleaning",
            f"{product_difference} product records were removed "
            "between Bronze and Silver. Verify that these "
            "removals are expected.",
            product_difference,
        )

    if cart_difference < 0:
        create_alert(
            alerts,
            "HIGH",
            "Reconciliation",
            "Silver cart count exceeds Bronze",
            "The Silver cart count exceeds the Bronze count. "
            "Investigate the source data and pipeline runs.",
            abs(cart_difference),
        )

    elif cart_difference > 0:
        create_alert(
            alerts,
            "INFO",
            "Reconciliation",
            "Cart records removed during cleaning",
            f"{cart_difference} cart records were removed "
            "between Bronze and Silver. Verify that these "
            "removals are expected.",
            cart_difference,
        )


# =========================================================
# CART ANOMALY DETECTION
# =========================================================

def detect_cart_value_anomalies(carts, alerts):
    """
    Detect unusually high cart values.

    Uses both a configurable absolute threshold and the
    interquartile range (IQR) method.

    These are screening rules, not proof of an actual error.
    """

    if carts.empty or "total" not in carts.columns:
        return pd.DataFrame()

    valid_totals = pd.to_numeric(
        carts["total"],
        errors="coerce",
    )

    valid_totals = valid_totals[
        valid_totals.notna()
        & (valid_totals >= 0)
    ]

    if valid_totals.empty:
        return pd.DataFrame()

    q1 = valid_totals.quantile(0.25)
    q3 = valid_totals.quantile(0.75)

    iqr = q3 - q1

    iqr_threshold = q3 + (1.5 * iqr)

    # Use the stricter of the IQR and configured thresholds.
    anomaly_threshold = max(
        HIGH_CART_VALUE_THRESHOLD,
        iqr_threshold,
    )

    anomalies = carts[
        pd.to_numeric(
            carts["total"],
            errors="coerce",
        ) > anomaly_threshold
    ].copy()

    if not anomalies.empty:
        create_alert(
            alerts,
            "MEDIUM",
            "Anomaly",
            "Unusually high cart values detected",
            f"{len(anomalies)} carts exceed the anomaly threshold "
            f"of {anomaly_threshold:.2f}. Review them to determine "
            "whether the values are legitimate.",
            len(anomalies),
        )

    return anomalies.sort_values(
        "total",
        ascending=False,
    )


# =========================================================
# PRODUCT INVENTORY ALERTS
# =========================================================

def detect_low_stock(products, alerts):
    """Flag products whose inventory is below the threshold."""

    if products.empty or "stock" not in products.columns:
        return pd.DataFrame()

    low_stock = products[
        products["stock"] < LOW_STOCK_THRESHOLD
    ].copy()

    if not low_stock.empty:
        create_alert(
            alerts,
            "MEDIUM",
            "Inventory",
            "Low-stock products detected",
            f"{len(low_stock)} products have stock below "
            f"{LOW_STOCK_THRESHOLD} units.",
            len(low_stock),
        )

    return low_stock.sort_values(
        "stock",
        ascending=True,
    )


# =========================================================
# SAVE REPORTS
# =========================================================

def save_report(
    report,
    anomalies,
    low_stock,
    bronze_run,
    silver_run,
    gold_run,
):
    """Save a JSON quality report and anomaly CSV files."""

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    report_path = REPORT_DIR / (
        f"data_quality_report_{timestamp}.json"
    )

    anomalies_path = REPORT_DIR / (
        f"cart_anomalies_{timestamp}.csv"
    )

    low_stock_path = REPORT_DIR / (
        f"low_stock_products_{timestamp}.csv"
    )

    report["run_timestamp"] = datetime.now().isoformat(
        timespec="seconds"
    )

    report["input_paths"] = {
        "bronze": str(bronze_run),
        "silver": str(silver_run),
        "gold": str(gold_run),
    }

    report["output_paths"] = {
        "report": str(report_path),
        "cart_anomalies": str(anomalies_path),
        "low_stock_products": str(low_stock_path),
    }

    with open(
        report_path,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            report,
            file,
            indent=4,
            allow_nan=False,
        )

    anomalies.to_csv(
        anomalies_path,
        index=False,
    )

    low_stock.to_csv(
        low_stock_path,
        index=False,
    )

    return {
        "report": report_path,
        "anomalies": anomalies_path,
        "low_stock": low_stock_path,
    }


# =========================================================
# MAIN DATA QUALITY PIPELINE
# =========================================================

def run_data_quality_pipeline():
    """Execute the complete data quality and anomaly checks."""

    print("\n" + "=" * 65)
    print("DATASENTINEL AI - DATA QUALITY & ANOMALY DETECTION")
    print("=" * 65)

    alerts = []

    # -----------------------------------------------------
    # STEP 1: FIND LATEST LAYER RUNS
    # -----------------------------------------------------

    bronze_run = find_latest_bronze_run()
    silver_run = find_latest_silver_run()
    gold_run = find_latest_gold_run()

    print(f"\nBronze input: {bronze_run}")
    print(f"Silver input: {silver_run}")
    print(f"Gold input: {gold_run}")

    # -----------------------------------------------------
    # STEP 2: LOAD DATA
    # -----------------------------------------------------

    bronze_products, bronze_carts = load_bronze_data(
        bronze_run
    )

    silver_products, silver_carts = load_silver_data(
        silver_run
    )

    gold_summary = load_gold_summary(gold_run)

    print("\nLoaded datasets:")
    print(f"Bronze products: {len(bronze_products)}")
    print(f"Bronze carts: {len(bronze_carts)}")
    print(f"Silver products: {len(silver_products)}")
    print(f"Silver carts: {len(silver_carts)}")

    # -----------------------------------------------------
    # STEP 3: BRONZE CHECKS
    # -----------------------------------------------------

    print("\nRunning Bronze data quality checks...")

    check_bronze_quality(
        bronze_products,
        bronze_carts,
        alerts,
    )

    # -----------------------------------------------------
    # STEP 4: SILVER CHECKS
    # -----------------------------------------------------

    print("Running Silver data quality checks...")

    check_silver_quality(
        silver_products,
        silver_carts,
        alerts,
    )

    # -----------------------------------------------------
    # STEP 5: RECONCILIATION
    # -----------------------------------------------------

    print("Reconciling Bronze and Silver record counts...")

    check_pipeline_reconciliation(
        bronze_products,
        bronze_carts,
        silver_products,
        silver_carts,
        alerts,
    )

    # -----------------------------------------------------
    # STEP 6: CART ANOMALIES
    # -----------------------------------------------------

    print("Detecting unusual cart values...")

    cart_anomalies = detect_cart_value_anomalies(
        silver_carts,
        alerts,
    )

    # -----------------------------------------------------
    # STEP 7: LOW-STOCK DETECTION
    # -----------------------------------------------------

    print("Checking product inventory...")

    low_stock_products = detect_low_stock(
        silver_products,
        alerts,
    )

    # -----------------------------------------------------
    # STEP 8: SUMMARIZE ALERTS
    # -----------------------------------------------------

    severity_counts = {
        "CRITICAL": 0,
        "HIGH": 0,
        "MEDIUM": 0,
        "INFO": 0,
    }

    for alert in alerts:
        severity = alert["severity"]

        severity_counts[severity] = (
            severity_counts.get(severity, 0) + 1
        )

    report = {
        "pipeline": (
            "DataSentinel AI Data Quality & Anomaly Detection"
        ),
        "status": (
            "CRITICAL"
            if severity_counts["CRITICAL"] > 0
            else "WARNING"
            if (
                severity_counts["HIGH"] > 0
                or severity_counts["MEDIUM"] > 0
            )
            else "SUCCESS"
        ),
        "record_counts": {
            "bronze_products": len(bronze_products),
            "bronze_carts": len(bronze_carts),
            "silver_products": len(silver_products),
            "silver_carts": len(silver_carts),
        },
        "gold_summary": gold_summary,
        "alert_summary": {
            "total_alerts": len(alerts),
            "by_severity": severity_counts,
        },
        "alerts": alerts,
        "anomaly_summary": {
            "high_value_cart_count": len(cart_anomalies),
            "low_stock_product_count": len(low_stock_products),
        },
    }

    # -----------------------------------------------------
    # STEP 9: SAVE REPORT
    # -----------------------------------------------------

    output_paths = save_report(
        report,
        cart_anomalies,
        low_stock_products,
        bronze_run,
        silver_run,
        gold_run,
    )

    # -----------------------------------------------------
    # STEP 10: DISPLAY RESULTS
    # -----------------------------------------------------

    print("\n" + "-" * 65)
    print("DATA QUALITY SUMMARY")
    print("-" * 65)

    print(f"Overall status: {report['status']}")
    print(f"Total alerts: {len(alerts)}")

    for severity, count in severity_counts.items():
        print(f"{severity}: {count}")

    print("\n" + "-" * 65)
    print("DETECTED ALERTS")
    print("-" * 65)

    if not alerts:
        print("No data quality issues detected.")
    else:
        for index, alert in enumerate(alerts, start=1):
            print(
                f"\n{index}. [{alert['severity']}] "
                f"{alert['title']}"
            )

            print(f"   Category: {alert['category']}")
            print(f"   Details: {alert['description']}")
            print(
                "   Affected records: "
                f"{alert['affected_records']}"
            )

    print("\n" + "-" * 65)
    print("REPORT OUTPUTS")
    print("-" * 65)

    for report_name, report_path in output_paths.items():
        print(f"{report_name}: {report_path}")

    print("\n" + "=" * 65)
    print("DATA QUALITY PIPELINE COMPLETED")
    print("=" * 65)

    return report


# =========================================================
# SCRIPT ENTRY POINT
# =========================================================

if __name__ == "__main__":
    run_data_quality_pipeline()