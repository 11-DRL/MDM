# Fabric Notebook
# Name: nb_bronze_append_lightspeed
# Description: Appends Lightspeed API payload into bronze.lightspeed_businesses.
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
    StructType, StructField, StringType, LongType, TimestampType
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

def as_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []

def first_not_none(*values):
    for val in values:
        if val is not None:
            return val
    return None

def to_int(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None

def extract_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []

    candidates = []
    for key in [
        "value", "values", "items", "data", "results", "businesses",
        "response", "Response", "body", "Body", "result", "Result"
    ]:
        if key in payload:
            obj = payload[key]
            if isinstance(obj, str):
                try:
                    obj = json.loads(obj)
                except Exception:
                    obj = None
            if isinstance(obj, list):
                candidates.extend(obj)
            elif isinstance(obj, dict):
                nested = obj.get("businesses") or obj.get("items") or obj.get("value") or obj.get("data")
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

    business_id = to_int(first_not_none(rec.get("businessId"), rec.get("business_id"), rec.get("id")))
    business_name = first_not_none(rec.get("businessName"), rec.get("business_name"), rec.get("name"))
    currency_code = first_not_none(rec.get("currencyCode"), rec.get("currency_code"), rec.get("currency"))

    locations = first_not_none(rec.get("businessLocations"), rec.get("business_locations"), rec.get("locations"))
    location_list = as_list(locations)

    if location_list:
        for loc in location_list:
            if not isinstance(loc, dict):
                continue
            rows.append((
                business_id,
                business_name,
                currency_code,
                to_int(first_not_none(loc.get("blId"), loc.get("businessLocationId"), loc.get("id"))),
                first_not_none(loc.get("blName"), loc.get("name"), loc.get("businessLocationName")),
                first_not_none(loc.get("country"), rec.get("country")),
                first_not_none(loc.get("timezone"), rec.get("timezone")),
                "lightspeed",
                LOAD_TS,
                run_id,
                tenant_name,
            ))
    else:
        rows.append((
            business_id,
            business_name,
            currency_code,
            to_int(first_not_none(rec.get("blId"), rec.get("businessLocationId"), rec.get("locationId"))),
            first_not_none(rec.get("blName"), rec.get("businessLocationName"), rec.get("locationName"), business_name),
            rec.get("country"),
            rec.get("timezone"),
            "lightspeed",
            LOAD_TS,
            run_id,
            tenant_name,
        ))

schema = StructType([
    StructField("businessId", LongType(), True),
    StructField("businessName", StringType(), True),
    StructField("currencyCode", StringType(), True),
    StructField("blId", LongType(), True),
    StructField("blName", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timezone", StringType(), True),
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
    df.write.mode("append").saveAsTable("bronze.lightspeed_businesses")
    print(f"Inserted rows: {df.count()}")
else:
    print("No rows parsed from payload; nothing inserted.")

