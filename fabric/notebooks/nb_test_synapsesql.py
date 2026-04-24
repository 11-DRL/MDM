# Fabric Notebook
# Name: nb_test_synapsesql
# Description: Minimal test for synapsesql Warehouse writer.

# ---------------------------------------------------------------------------
# CELL 1: Setup
# ---------------------------------------------------------------------------
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, TimestampType
from datetime import datetime, timezone

print("Starting synapsesql test")

# ---------------------------------------------------------------------------
# CELL 2: Build DataFrame with same schema as silver_dv.hub_location
# ---------------------------------------------------------------------------
schema = StructType([
    StructField("location_hk",   BinaryType(),    False),
    StructField("business_key",  StringType(),    False),
    StructField("load_date",     TimestampType(), False),
    StructField("record_source", StringType(),    False),
])
hk = bytes.fromhex("aa" * 32)
row = Row(location_hk=hk, business_key="test|1", load_date=datetime(2024,1,15,6,0,0,tzinfo=timezone.utc), record_source="test")
df = spark.createDataFrame([row], schema=schema)
df.show(truncate=False)
df.printSchema()

# ---------------------------------------------------------------------------
# CELL 3: synapsesql write (append)
# ---------------------------------------------------------------------------
try:
    df.write.mode("append").synapsesql("wh_mdm.silver_dv.hub_location")
    print("WRITE OK")
except Exception as e:
    import traceback
    print("WRITE FAIL:", type(e).__name__, str(e))
    traceback.print_exc()
    raise
