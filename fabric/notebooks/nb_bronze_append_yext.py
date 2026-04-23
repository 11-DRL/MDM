# Fabric Notebook
# Name: nb_bronze_append_yext
# Description: Appends Yext API payload into bronze.yext_locations.
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
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
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

def to_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
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
        "value", "values", "items", "data", "results", "entities", "locations",
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
            nested = obj.get("entities") or obj.get("locations") or obj.get("items") or obj.get("value") or obj.get("data")
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

    address = rec.get("address") if isinstance(rec.get("address"), dict) else {}
    coordinate = rec.get("displayCoordinate") if isinstance(rec.get("displayCoordinate"), dict) else {}
    if not coordinate:
        coordinate = rec.get("yextDisplayCoordinate") if isinstance(rec.get("yextDisplayCoordinate"), dict) else {}

    phone_value = rec.get("phone")
    if isinstance(phone_value, dict):
        phone_value = first_not_none(phone_value.get("display"), phone_value.get("phoneNumber"))

    rows.append((
        first_not_none(rec.get("id"), rec.get("entityId"), rec.get("locationId")),
        rec.get("name"),
        first_not_none(address.get("line1"), address.get("addressLine1"), rec.get("address_line1")),
        first_not_none(address.get("city"), rec.get("city"), rec.get("address_city")),
        first_not_none(address.get("postalCode"), address.get("zip"), rec.get("postalCode"), rec.get("address_postal_code")),
        first_not_none(address.get("countryCode"), rec.get("countryCode"), rec.get("address_country_code")),
        first_not_none(phone_value, rec.get("mainPhone")),
        first_not_none(rec.get("websiteUrl"), rec.get("website"), rec.get("website_url")),
        to_float(first_not_none(coordinate.get("latitude"), rec.get("latitude"), rec.get("display_lat"))),
        to_float(first_not_none(coordinate.get("longitude"), rec.get("longitude"), rec.get("display_lng"))),
        to_float(first_not_none(rec.get("averageRating"), rec.get("avgRating"), rec.get("avg_rating"))),
        to_int(first_not_none(rec.get("reviewCount"), rec.get("review_count"))),
        "yext",
        LOAD_TS,
        run_id,
        tenant_name,
    ))

schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("address_line1", StringType(), True),
    StructField("address_city", StringType(), True),
    StructField("address_postal_code", StringType(), True),
    StructField("address_country_code", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("display_lat", DoubleType(), True),
    StructField("display_lng", DoubleType(), True),
    StructField("avg_rating", DoubleType(), True),
    StructField("review_count", IntegerType(), True),
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
    df.write.mode("append").saveAsTable("bronze.yext_locations")
    print(f"Inserted rows: {df.count()}")
else:
    print("No rows parsed from payload; nothing inserted.")

