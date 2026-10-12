import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------
# PYSPARK PYTHON CONFIGURATION
# ---------------------------------------------------------

PYTHON_EXE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
)

# ---------------------------------------------------------
# PROJECT PATHS
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"


# ---------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------

def create_spark_session():
    """Create a local Spark session for Silver transformations."""

    spark = (
        SparkSession.builder
        .appName("DataSentinelAI-Silver")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.python.worker.reuse", "true")
        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.RawLocalFileSystem",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ---------------------------------------------------------
# FIND LATEST BRONZE RUN
# ---------------------------------------------------------

def find_latest_bronze_run():
    """Find the most recent Bronze run containing both datasets."""

    if not BRONZE_DIR.exists():
        raise FileNotFoundError(
            f"Bronze directory does not exist: {BRONZE_DIR}"
        )

    valid_runs = []

    for run_dir in BRONZE_DIR.iterdir():
        if not run_dir.is_dir():
            continue

        products_file = run_dir / "products.json"
        carts_file = run_dir / "carts.json"

        if products_file.exists() and carts_file.exists():
            valid_runs.append(run_dir)

    if not valid_runs:
        raise FileNotFoundError(
            "No valid Bronze runs found. "
            "Run the ingestion pipeline first."
        )

    latest_run = max(
        valid_runs,
        key=lambda path: path.stat().st_mtime,
    )

    return latest_run


# ---------------------------------------------------------
# LOAD BRONZE JSON
# ---------------------------------------------------------

def load_json_records(file_path):
    """Load a JSON file containing a list of records."""

    with open(file_path, "r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list):
        raise ValueError(
            f"Expected a JSON list in {file_path}"
        )

    return records


# ---------------------------------------------------------
# SAFE VALUE CONVERSION
# ---------------------------------------------------------

def to_int(value):
    """Convert a value to an integer safely."""

    if value is None or isinstance(value, bool):
        return None

    try:
        number = float(value)

        if not math.isfinite(number):
            return None

        if not number.is_integer():
            return None

        return int(number)

    except (ValueError, TypeError, OverflowError):
        return None


def to_float(value):
    """Convert a value to a finite float safely."""

    if value is None or isinstance(value, bool):
        return None

    try:
        number = float(value)

        if not math.isfinite(number):
            return None

        return number

    except (ValueError, TypeError, OverflowError):
        return None


def to_string(value):
    """Convert a value to a trimmed string safely."""

    if value is None:
        return None

    result = str(value).strip()

    return result if result else None


# ---------------------------------------------------------
# PRODUCT CLEANING
# ---------------------------------------------------------

def clean_products(spark, records):
    """
    Clean product records using PySpark.

    Operations:
    - Select required columns.
    - Normalize data types.
    - Remove duplicate product IDs.
    - Remove invalid IDs and missing descriptions.
    - Reject negative prices and stock quantities.
    """

    product_schema = StructType([
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock", LongType(), True),
        StructField("rating", DoubleType(), True),
    ])

    normalized_records = []

    for record in records:
        if not isinstance(record, dict):
            continue

        normalized_records.append({
            "id": to_int(record.get("id")),
            "title": to_string(record.get("title")),
            "category": to_string(record.get("category")),
            "price": to_float(record.get("price")),
            "stock": to_int(record.get("stock")),
            "rating": to_float(record.get("rating")),
        })

    if not normalized_records:
        return spark.createDataFrame([], product_schema)

    products_df = spark.createDataFrame(
        normalized_records,
        schema=product_schema,
    )

    # Remove duplicate product IDs.
    products_df = products_df.dropDuplicates(["id"])

    # Remove invalid records.
    products_df = products_df.filter(
        (F.col("id").isNotNull())
        & (F.col("id") > 0)
        & (F.col("title").isNotNull())
        & (F.col("category").isNotNull())
        & (F.col("price").isNotNull())
        & (F.col("price") >= 0)
        & (F.col("stock").isNotNull())
        & (F.col("stock") >= 0)
        & (
            F.col("rating").isNull()
            | (
                (F.col("rating") >= 0)
                & (F.col("rating") <= 5)
            )
        )
    )

    return products_df


# ---------------------------------------------------------
# CART CLEANING
# ---------------------------------------------------------

def clean_carts(spark, records):
    """
    Clean cart records using PySpark.

    Operations:
    - Select required cart-level columns.
    - Normalize data types.
    - Remove duplicate cart IDs.
    - Reject invalid IDs and negative totals/counts.
    """

    cart_schema = StructType([
        StructField("id", LongType(), True),
        StructField("userId", LongType(), True),
        StructField("total", DoubleType(), True),
        StructField("totalProducts", LongType(), True),
        StructField("totalQuantity", LongType(), True),
    ])

    normalized_records = []

    for record in records:
        if not isinstance(record, dict):
            continue

        normalized_records.append({
            "id": to_int(record.get("id")),
            "userId": to_int(record.get("userId")),
            "total": to_float(record.get("total")),
            "totalProducts": to_int(record.get("totalProducts")),
            "totalQuantity": to_int(record.get("totalQuantity")),
        })

    if not normalized_records:
        return spark.createDataFrame([], cart_schema)

    carts_df = spark.createDataFrame(
        normalized_records,
        schema=cart_schema,
    )

    # Remove duplicate cart IDs.
    carts_df = carts_df.dropDuplicates(["id"])

    # Remove invalid records.
    carts_df = carts_df.filter(
        (F.col("id").isNotNull())
        & (F.col("id") > 0)
        & (F.col("userId").isNotNull())
        & (F.col("userId") > 0)
        & (F.col("total").isNotNull())
        & (F.col("total") >= 0)
        & (F.col("totalProducts").isNotNull())
        & (F.col("totalProducts") >= 0)
        & (F.col("totalQuantity").isNotNull())
        & (F.col("totalQuantity") >= 0)
    )

    return carts_df


# ---------------------------------------------------------
# SAVE SPARK DATAFRAME AS PARQUET USING PYARROW
# ---------------------------------------------------------

def save_parquet_with_pyarrow(spark_df, output_path):
    """
    Convert a Spark DataFrame to Pandas and save it as Parquet.

    This avoids Spark's Hadoop filesystem writer, which can
    require winutils.exe on Windows.

    Intended for this project's small development dataset.
    For large production datasets, prefer Spark's native
    Parquet writer on a properly configured Spark environment.
    """

    import pyarrow as pa
    import pyarrow.parquet as pq

    output_path = Path(output_path)

    # Create the destination using Python's filesystem APIs.
    output_path.mkdir(parents=True, exist_ok=True)

    # Convert the cleaned Spark DataFrame to Pandas.
    pandas_df = spark_df.toPandas()

    # Convert Pandas to an Arrow table.
    arrow_table = pa.Table.from_pandas(
        pandas_df,
        preserve_index=False,
    )

    # Write Parquet directly through PyArrow.
    parquet_file = output_path / "part-00000.parquet"

    pq.write_table(
        arrow_table,
        parquet_file,
        compression="snappy",
    )

    return {
        "path": str(output_path),
        "file": str(parquet_file),
        "rows": len(pandas_df),
        "columns": list(pandas_df.columns),
    }


# ---------------------------------------------------------
# SILVER PIPELINE
# ---------------------------------------------------------

def run_silver_pipeline():
    """Execute Bronze-to-Silver data transformations."""

    print("\n" + "=" * 60)
    print("DATASENTINEL AI - SILVER PIPELINE")
    print("=" * 60)

    print(f"\nPython executable: {PYTHON_EXE}")
    print(f"Bronze directory: {BRONZE_DIR}")
    print(f"Silver directory: {SILVER_DIR}")

    spark = None

    try:
        # -------------------------------------------------
        # STEP 1: LOCATE BRONZE DATA
        # -------------------------------------------------

        bronze_run = find_latest_bronze_run()

        products_file = bronze_run / "products.json"
        carts_file = bronze_run / "carts.json"

        print(f"\nBronze input: {bronze_run}")

        products_records = load_json_records(products_file)
        carts_records = load_json_records(carts_file)

        print(f"Bronze products loaded: {len(products_records)}")
        print(f"Bronze carts loaded: {len(carts_records)}")

        # -------------------------------------------------
        # STEP 2: START SPARK
        # -------------------------------------------------

        spark = create_spark_session()

        # -------------------------------------------------
        # STEP 3: CLEAN PRODUCTS
        # -------------------------------------------------

        print("\nCleaning product data...")

        silver_products = clean_products(
            spark,
            products_records,
        )

        # -------------------------------------------------
        # STEP 4: CLEAN CARTS
        # -------------------------------------------------

        print("Cleaning cart data...")

        silver_carts = clean_carts(
            spark,
            carts_records,
        )

        # -------------------------------------------------
        # STEP 5: COUNT CLEANED RECORDS
        # -------------------------------------------------

        products_count = silver_products.count()
        carts_count = silver_carts.count()

        print("\nCleaning completed successfully.")
        print(f"Silver products: {products_count}")
        print(f"Silver carts: {carts_count}")

        # -------------------------------------------------
        # STEP 6: PREPARE OUTPUT DIRECTORY
        # -------------------------------------------------

        run_timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S_%f"
        )

        silver_run_dir = SILVER_DIR / run_timestamp

        products_output = silver_run_dir / "products"
        carts_output = silver_run_dir / "carts"

        products_output.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # -------------------------------------------------
        # STEP 7: WRITE PRODUCTS PARQUET
        # -------------------------------------------------

        print("\nWriting Silver products Parquet...")

        products_save_info = save_parquet_with_pyarrow(
            silver_products,
            products_output,
        )

        print(
            "Products Parquet saved:",
            products_save_info["file"],
        )

        # -------------------------------------------------
        # STEP 8: WRITE CARTS PARQUET
        # -------------------------------------------------

        print("\nWriting Silver carts Parquet...")

        carts_save_info = save_parquet_with_pyarrow(
            silver_carts,
            carts_output,
        )

        print(
            "Carts Parquet saved:",
            carts_save_info["file"],
        )

        # -------------------------------------------------
        # STEP 9: SAVE PIPELINE METADATA
        # -------------------------------------------------

        metadata = {
            "pipeline": "DataSentinel AI Silver Pipeline",
            "status": "SUCCESS",
            "run_timestamp": run_timestamp,
            "bronze_input_path": str(bronze_run),
            "silver_output_path": str(silver_run_dir),
            "products": {
                "bronze_count": len(products_records),
                "silver_count": products_count,
                "output_path": str(products_output),
                "parquet_file": products_save_info["file"],
            },
            "carts": {
                "bronze_count": len(carts_records),
                "silver_count": carts_count,
                "output_path": str(carts_output),
                "parquet_file": carts_save_info["file"],
            },
        }

        metadata_path = silver_run_dir / "metadata.json"

        with open(
            metadata_path,
            "w",
            encoding="utf-8",
        ) as metadata_file:
            json.dump(
                metadata,
                metadata_file,
                indent=4,
            )

        # -------------------------------------------------
        # STEP 10: DISPLAY PREVIEW
        # -------------------------------------------------

        print("\nSilver Products Preview:")
        silver_products.show(5, truncate=False)

        print("Silver Carts Preview:")
        silver_carts.show(5, truncate=False)

        # -------------------------------------------------
        # STEP 11: DISPLAY SUMMARY
        # -------------------------------------------------

        print("\n" + "=" * 60)
        print("SILVER PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)

        print(f"\nBronze products: {len(products_records)}")
        print(f"Silver products: {products_count}")

        print(f"\nBronze carts: {len(carts_records)}")
        print(f"Silver carts: {carts_count}")

        print(f"\nSilver output directory: {silver_run_dir}")
        print(f"Metadata file: {metadata_path}")

        return metadata

    except Exception as error:
        print("\nSILVER PIPELINE FAILED")
        print(f"Error: {error}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            print("\nSpark session stopped.")


# ---------------------------------------------------------
# SCRIPT ENTRY POINT
# ---------------------------------------------------------

if __name__ == "__main__":
    run_silver_pipeline()