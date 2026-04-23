# Fabric Notebook
# Name: nb_bronze_append_gopos
# Description: Appends GoPOS API payload into bronze.gopos_locations.
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
    StructType, StructField, StringType, BooleanType, TimestampType
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

def to_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in ["true", "1", "yes", "active"]:
        return True
    if text in ["false", "0", "no", "inactive"]:
        return False
    return None

def extract_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []

    candidates = []
    for key in [
        "value", "values", "items", "data", "results", "locations",
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
            nested = obj.get("locations") or obj.get("items") or obj.get("value") or obj.get("data")
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
        first_not_none(rec.get("location_id"), rec.get("locationId"), rec.get("id")),
        first_not_none(rec.get("location_name"), rec.get("locationName"), rec.get("name")),
        rec.get("address"),
        rec.get("city"),
        first_not_none(rec.get("zip_code"), rec.get("zipCode"), rec.get("postalCode")),
        rec.get("country"),
        rec.get("phone"),
        to_bool(first_not_none(rec.get("is_active"), rec.get("active"), rec.get("status"))),
        "gopos",
        LOAD_TS,
        run_id,
        tenant_name,
    ))

schema = StructType([
    StructField("location_id", StringType(), True),
    StructField("location_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("_source_system", StringType(), False),
    StructField("_load_date", TimestampType(), False),
    StructField("_run_id", StringType(), False),
    StructField("_tenant_name", StringType(), False),
])

# ---------------------------------------------------------------------------
# CELL 4: Append to Bronze
# ---------------------------------------------------------------------------
if rows:
    df = spark.createDataFrame(rows, schema=schema)
    df.write.mode("append").saveAsTable("bronze.gopos_locations")
    print(f"Inserted rows: {df.count()}")
else:
    print("No rows parsed from payload; nothing inserted.")

