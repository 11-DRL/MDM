# Fabric Notebook
# Name: nb_bronze_append_mcwin
# Description: Appends McWin API payload into bronze.mcwin_restaurant_masterdata.
# Parameters: run_id, tenant_name, payload_json

# ---------------------------------------------------------------------------
# CELL 1: Parameters
# ---------------------------------------------------------------------------
run_id = dbutils.widgets.get("run_id") if "dbutils" in dir() else "dev-run-001"
tenant_name = dbutils.widgets.get("tenant_name") if "dbutils" in dir() else "losteria"
payload_json = dbutils.widgets.get("payload_json") if "dbutils" in dir() else "{}"

print(f"run_id={run_id}, tenant_name={tenant_name}")

# ---------------------------------------------------------------------------
# CELL 2: Helpers
# ---------------------------------------------------------------------------
import json
from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

LOAD_TS = datetime.now(timezone.utc)

def as_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

def first_not_none(*values):
    for val in values:
        if val is not None:
            return val
    return None

def extract_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []

    candidates = []
    for key in [
        "value", "values", "items", "data", "results", "restaurants",
        "response", "Response", "body", "Body", "result", "Result"
    ]:
        obj = payload.get(key)
        if isinstance(obj, str):
            try:
                obj = json.loads(obj)
            except Exception:
                obj = None

        if isinstance(obj, list):
            candidates.extend(obj)
        elif isinstance(obj, dict):
            nested = obj.get("restaurants") or obj.get("items") or obj.get("value") or obj.get("data")
            if isinstance(nested, list):
                candidates.extend(nested)

    return candidates

# ---------------------------------------------------------------------------
# CELL 3: Parse payload
# ---------------------------------------------------------------------------
payload_obj = as_dict(payload_json)
records = extract_records(payload_obj)
print(f"Records extracted from payload: {len(records)}")

rows = []
for rec in records:
    if not isinstance(rec, dict):
        continue

    rows.append((
        first_not_none(rec.get("restaurant_id"), rec.get("restaurantId"), rec.get("id")),
        first_not_none(rec.get("restaurant_name"), rec.get("restaurantName"), rec.get("name")),
        first_not_none(rec.get("cost_center"), rec.get("costCenter"), rec.get("costcentre")),
        rec.get("region"),
        rec.get("country"),
        rec.get("city"),
        first_not_none(rec.get("zip_code"), rec.get("zipCode"), rec.get("postalCode")),
        rec.get("address"),
        str(first_not_none(rec.get("is_active"), rec.get("active"), rec.get("status"), "1")),
        "mcwin",
        LOAD_TS,
        run_id,
        None,
        tenant_name,
    ))

schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("restaurant_name", StringType(), True),
    StructField("cost_center", StringType(), True),
    StructField("region", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("address", StringType(), True),
    StructField("is_active", StringType(), True),
    StructField("_source_system", StringType(), False),
    StructField("_load_date", TimestampType(), False),
    StructField("_run_id", StringType(), False),
    StructField("_file_name", StringType(), True),
    StructField("_tenant_name", StringType(), False),
])

# ---------------------------------------------------------------------------
# CELL 4: Append to Bronze
# ---------------------------------------------------------------------------
if rows:
    df = spark.createDataFrame(rows, schema=schema)
    df.write.mode("append").saveAsTable("bronze.mcwin_restaurant_masterdata")
    print(f"Inserted rows: {df.count()}")
else:
    print("No rows parsed from payload; nothing inserted.")

