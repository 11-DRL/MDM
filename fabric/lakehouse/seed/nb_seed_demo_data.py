# Fabric Notebook
# Name: nb_seed_demo_data
# Description: One-time seed — wypełnia Bronze → Silver DV → Gold
#              danymi demo dla L'Osteria (~20 lokalizacji, 4 systemy źródłowe)
# WAŻNE: Uruchom JEDNORAZOWO na pustym lakehouse przed demo
#        Wymaga uruchomionego nb_seed_mdm_config_location.sql wcześniej
# =============================================================================

# ---------------------------------------------------------------------------
# CELL 1: Imports, helpers, stałe
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, BinaryType, StringType, TimestampType,
    DoubleType, IntegerType, BooleanType, LongType
)
from datetime import datetime, timezone
import hashlib, unicodedata

LAKEHOUSE = "lh_mdm"
WAREHOUSE = "wh_mdm"
RUN_ID    = "seed-demo-001"
TENANT    = "losteria"

# Stałe daty — realistyczne timestampy historyczne
BRONZE_DT = datetime(2024, 1, 15,  2, 0, 0, tzinfo=timezone.utc)  # nocny load bronze
SILVER_DT = datetime(2024, 1, 15,  6, 0, 0, tzinfo=timezone.utc)  # rano silver DV
GOLD_DT   = datetime(2024, 1, 15,  8, 0, 0, tzinfo=timezone.utc)  # gold snapshot
AUDIT_DT1 = datetime(2024, 1, 14, 14, 0, 0, tzinfo=timezone.utc)  # steward action 1
AUDIT_DT2 = datetime(2024, 1, 14, 16, 0, 0, tzinfo=timezone.utc)  # steward action 2
AUDIT_DT3 = datetime(2024, 1, 15,  9, 0, 0, tzinfo=timezone.utc)  # steward action 3

def hk(key: str) -> bytes:
    """SHA256 hash key — identyczny z logiką w nb_load_raw_vault_location.py"""
    return hashlib.sha256(key.encode("utf-8")).digest()

def hdiff(*vals) -> bytes:
    """SHA256 hash diff dla wykrywania zmian w Satellite"""
    return hashlib.sha256("|".join(str(v) if v is not None else "" for v in vals).encode()).digest()

def std(s: str) -> str:
    """Standaryzacja nazwy: ASCII + uppercase + strip"""
    if not s:
        return s
    n = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return n.upper().strip()

def uid() -> str:
    import uuid
    return str(uuid.uuid4())

print(f"Seed demo start: RUN_ID={RUN_ID}, BRONZE_DT={BRONZE_DT.date()}")


# ---------------------------------------------------------------------------
# CELL 2: Definicje 20 lokalizacji L'Osteria
# ---------------------------------------------------------------------------
# Struktura per rekord:
#   canonical_*  — pola złotego rekordu (best-source-wins)
#   ls_*         — wariant Lightspeed (zawsze obecny)
#   ye_*         — wariant Yext (None jeśli brak)
#   mc_*         — wariant McWin (None jeśli brak)
#   gp_*         — wariant GoPOS (None jeśli brak)
#   match_status — 'pending' / 'auto_accepted' / 'no_match'
#   pair_id      — ID kandydatury (None dla no_match)
#   match_score  — wynik matchingu (None dla no_match)

LOCS = [
    # ──── 7 PENDING (do review przez stewarda) ──────────────────────────────
    dict(
        id=1, bl_id=41839, cost_center="DE-MUC-001",
        canonical_name="L'Osteria München Marienplatz",
        city="München", country="DE", zip="80331",
        address="Marienplatz 8", phone="+49 89 12345678",
        lat=48.1374, lng=11.5755, tz="Europe/Berlin", currency="EUR",
        region="Germany South", avg_rating=4.3, review_count=2847,
        website="https://losteria.net/de/muc-marienplatz",
        ls_name="L'Osteria München Marienplatz",
        ye_id="yext-de-muc-001", ye_name="L'Osteria Marienplatz München", ye_city="Munich", ye_zip="80331",
        mc_id=None, gp_id=None,
        match_status="pending", pair_id="pair-001", match_score=0.94, name_score=0.96,
        city_match=True, zip_match=False, geo_score=0.88, match_type="fuzzy_name_city",
    ),
    dict(
        id=2, bl_id=41902, cost_center="DE-FRA-003",
        canonical_name="L'Osteria Frankfurt Sachsenhausen",
        city="Frankfurt", country="DE", zip="60594",
        address="Schweizer Str. 62", phone="+49 69 98765432",
        lat=50.0975, lng=8.6842, tz="Europe/Berlin", currency="EUR",
        region="Germany West", avg_rating=4.2, review_count=1934,
        website="https://losteria.net/de/fra-sachsenhausen",
        ls_name="L'Osteria Frankfurt Sachsenhausen",
        mc_id="MC-DE-FRA-003", mc_name="LOsteria Frankfurt Sachsenhausen",
            mc_city="Frankfurt am Main", mc_zip="60594",
        ye_id=None, gp_id=None,
        match_status="pending", pair_id="pair-002", match_score=0.91, name_score=0.93,
        city_match=True, zip_match=True, geo_score=0.92, match_type="composite_high",
    ),
    dict(
        id=3, bl_id=41745, cost_center="AT-VIE-002",
        canonical_name="L'Osteria Wien Naschmarkt",
        city="Wien", country="AT", zip="1060",
        address="Linke Wienzeile 4", phone="+43 1 5874523",
        lat=48.1994, lng=16.3659, tz="Europe/Vienna", currency="EUR",
        region="Austria", avg_rating=4.4, review_count=1102,
        website="https://losteria.net/at/vie-naschmarkt",
        ls_name="L'Osteria Wien Naschmarkt",
        gp_id="GP-AT-VIE-002", gp_name="L'Osteria Vienna Naschmarkt", gp_city="Vienna", gp_zip="1060",
        ye_id=None, mc_id=None,
        match_status="pending", pair_id="pair-003", match_score=0.88, name_score=0.85,
        city_match=True, zip_match=False, geo_score=0.94, match_type="fuzzy_name_city",
    ),
    dict(
        id=4, bl_id=41520, cost_center="DE-HAM-001",
        canonical_name="L'Osteria Hamburg Altona",
        city="Hamburg", country="DE", zip="22765",
        address="Große Elbstraße 145", phone="+49 40 38654321",
        lat=53.5461, lng=9.9282, tz="Europe/Berlin", currency="EUR",
        region="Germany North", avg_rating=4.1, review_count=1523,
        website="https://losteria.net/de/ham-altona",
        ls_name="L'Osteria Hamburg Altona",
        ye_id="yext-de-ham-001", ye_name="L'Osteria Altona Hamburg", ye_city="Hamburg", ye_zip="22765",
        mc_id=None, gp_id=None,
        match_status="pending", pair_id="pair-004", match_score=0.87, name_score=0.89,
        city_match=True, zip_match=True, geo_score=None, match_type="composite",
    ),
    dict(
        id=5, bl_id=41633, cost_center="DE-CGN-002",
        canonical_name="L'Osteria Köln Rudolfplatz",
        city="Köln", country="DE", zip="50674",
        address="Hahnenstrasse 16", phone="+49 221 44332211",
        lat=50.9333, lng=6.9402, tz="Europe/Berlin", currency="EUR",
        region="Germany West", avg_rating=4.0, review_count=2103,
        website="https://losteria.net/de/cgn-rudolfplatz",
        ls_name="L'Osteria Köln Rudolfplatz",
        mc_id="MC-DE-CGN-002", mc_name="L'Osteria Rudolfplatz", mc_city="Köln", mc_zip="50676",
        ye_id=None, gp_id=None,
        match_status="pending", pair_id="pair-005", match_score=0.86, name_score=0.72,
        city_match=True, zip_match=False, geo_score=0.99, match_type="geo_proximity",
    ),
    dict(
        id=6, bl_id=41234, cost_center="CH-ZRH-001",
        canonical_name="L'Osteria Zürich Langstrasse",
        city="Zürich", country="CH", zip="8004",
        address="Langstrasse 197", phone="+41 44 2413344",
        lat=47.3769, lng=8.5283, tz="Europe/Zurich", currency="CHF",
        region="Switzerland", avg_rating=4.5, review_count=892,
        website="https://losteria.net/ch/zrh-langstrasse",
        ls_name="L'Osteria Zürich Langstrasse",
        ye_id="yext-ch-zrh-001", ye_name="L'Osteria Langstrasse", ye_city="Zürich", ye_zip="8004",
        mc_id=None, gp_id=None,
        match_status="pending", pair_id="pair-006", match_score=0.85, name_score=0.86,
        city_match=True, zip_match=True, geo_score=None, match_type="fuzzy_name_city",
    ),
    dict(
        id=7, bl_id=41712, cost_center="DE-NUE-001",
        canonical_name="L'Osteria Nürnberg Königstraße",
        city="Nürnberg", country="DE", zip="90402",
        address="Königstraße 17", phone="+49 911 33445566",
        lat=49.4521, lng=11.0767, tz="Europe/Berlin", currency="EUR",
        region="Germany South", avg_rating=4.2, review_count=1445,
        website="https://losteria.net/de/nue-koenigstrasse",
        ls_name="L'Osteria Nürnberg Königstraße",
        gp_id="GP-DE-NUE-001", gp_name="L'Osteria Nuernberg Koenigstrasse",
            gp_city="Nuremberg", gp_zip="90403",
        ye_id=None, mc_id=None,
        match_status="pending", pair_id="pair-007", match_score=0.85, name_score=0.87,
        city_match=False, zip_match=False, geo_score=None, match_type="composite",
    ),
    # ──── 12 AUTO-ACCEPTED (already processed) ──────────────────────────────
    dict(
        id=8, bl_id=41855, cost_center="DE-MUC-003",
        canonical_name="L'Osteria München Schwabing",
        city="München", country="DE", zip="80803",
        address="Leopoldstraße 50", phone="+49 89 22334455",
        lat=48.1596, lng=11.5814, tz="Europe/Berlin", currency="EUR",
        region="Germany South", avg_rating=4.4, review_count=3102,
        website="https://losteria.net/de/muc-schwabing",
        ls_name="L'Osteria München Schwabing",
        mc_id="MC-DE-MUC-003", mc_name="L'Osteria Muenchen Schwabing",
            mc_city="München", mc_zip="80803",
        ye_id=None, gp_id=None,
        match_status="auto_accepted", pair_id="pair-a001", match_score=0.98, name_score=0.97,
        city_match=True, zip_match=True, geo_score=0.99, match_type="composite",
    ),
    dict(
        id=9, bl_id=41867, cost_center="DE-HAM-002",
        canonical_name="L'Osteria Hamburg HafenCity",
        city="Hamburg", country="DE", zip="20457",
        address="Am Kaiserkai 10", phone="+49 40 55667788",
        lat=53.5411, lng=10.0014, tz="Europe/Berlin", currency="EUR",
        region="Germany North", avg_rating=4.3, review_count=2201,
        website="https://losteria.net/de/ham-hafencity",
        ls_name="L'Osteria Hamburg HafenCity",
        gp_id="GP-DE-HAM-002", gp_name="L'Osteria HafenCity Hamburg",
            gp_city="Hamburg", gp_zip="20457",
        ye_id=None, mc_id=None,
        match_status="auto_accepted", pair_id="pair-a002", match_score=0.97, name_score=0.95,
        city_match=True, zip_match=True, geo_score=0.99, match_type="composite",
    ),
    dict(
        id=10, bl_id=41300, cost_center="DE-BER-001",
        canonical_name="L'Osteria Berlin Mitte",
        city="Berlin", country="DE", zip="10117",
        address="Friedrichstraße 105", phone="+49 30 99887766",
        lat=52.5166, lng=13.3882, tz="Europe/Berlin", currency="EUR",
        region="Germany East", avg_rating=4.2, review_count=4518,
        website="https://losteria.net/de/ber-mitte",
        ls_name="L'Osteria Berlin Mitte",
        ye_id="yext-de-ber-001", ye_name="L'Osteria Berlin Mitte", ye_city="Berlin", ye_zip="10117",
        mc_id="MC-DE-BER-001", mc_name="L'Osteria Berlin Mitte", mc_city="Berlin", mc_zip="10117",
        gp_id="GP-DE-BER-001", gp_name="L'Osteria Berlin Mitte", gp_city="Berlin", gp_zip="10117",
        match_status="auto_accepted", pair_id="pair-a003", match_score=0.99, name_score=1.0,
        city_match=True, zip_match=True, geo_score=1.0, match_type="exact_name",
    ),
    dict(
        id=11, bl_id=41321, cost_center="DE-BER-002",
        canonical_name="L'Osteria Berlin Prenzlauer Berg",
        city="Berlin", country="DE", zip="10437",
        address="Kastanienallee 85", phone="+49 30 11223344",
        lat=52.5387, lng=13.4163, tz="Europe/Berlin", currency="EUR",
        region="Germany East", avg_rating=4.3, review_count=2876,
        website="https://losteria.net/de/ber-prenzlauer",
        ls_name="L'Osteria Berlin Prenzlauer Berg",
        mc_id="MC-DE-BER-002", mc_name="L'Osteria Berlin Prenzlauerberg",
            mc_city="Berlin", mc_zip="10437",
        ye_id=None, gp_id=None,
        match_status="auto_accepted", pair_id="pair-a004", match_score=0.98, name_score=0.97,
        city_match=True, zip_match=True, geo_score=None, match_type="composite",
    ),
    dict(
        id=12, bl_id=41678, cost_center="DE-DUS-001",
        canonical_name="L'Osteria Düsseldorf Altstadt",
        city="Düsseldorf", country="DE", zip="40213",
        address="Berger Allee 16", phone="+49 211 44556677",
        lat=51.2254, lng=6.7763, tz="Europe/Berlin", currency="EUR",
        region="Germany West", avg_rating=4.1, review_count=1876,
        website="https://losteria.net/de/dus-altstadt",
        ls_name="L'Osteria Düsseldorf Altstadt",
        ye_id="yext-de-dus-001", ye_name="L'Osteria Altstadt Düsseldorf",
            ye_city="Düsseldorf", ye_zip="40213",
        mc_id=None, gp_id=None,
        match_status="auto_accepted", pair_id="pair-a005", match_score=0.98, name_score=0.96,
        city_match=True, zip_match=True, geo_score=0.99, match_type="composite",
    ),
    dict(
        id=13, bl_id=41790, cost_center="AT-VIE-001",
        canonical_name="L'Osteria Wien Mariahilfer",
        city="Wien", country="AT", zip="1070",
        address="Mariahilfer Str. 101", phone="+43 1 5236677",
        lat=48.1976, lng=16.3465, tz="Europe/Vienna", currency="EUR",
        region="Austria", avg_rating=4.2, review_count=1432,
        website="https://losteria.net/at/vie-mariahilfer",
        ls_name="L'Osteria Wien Mariahilfer",
        mc_id="MC-AT-VIE-001", mc_name="L'Osteria Wien Mariahilferstrasse",
            mc_city="Wien", mc_zip="1070",
        ye_id=None, gp_id=None,
        match_status="auto_accepted", pair_id="pair-a006", match_score=0.97, name_score=0.94,
        city_match=True, zip_match=True, geo_score=None, match_type="fuzzy_name_city",
    ),
    dict(
        id=14, bl_id=41289, cost_center="CH-BSL-001",
        canonical_name="L'Osteria Basel Steinenberg",
        city="Basel", country="CH", zip="4051",
        address="Steinenberg 7", phone="+41 61 2714455",
        lat=47.5553, lng=7.5890, tz="Europe/Zurich", currency="CHF",
        region="Switzerland", avg_rating=4.3, review_count=623,
        website="https://losteria.net/ch/bsl-steinenberg",
        ls_name="L'Osteria Basel Steinenberg",
        mc_id="MC-CH-BSL-001", mc_name="L'Osteria Basel", mc_city="Basel", mc_zip="4051",
        ye_id=None, gp_id=None,
        match_status="auto_accepted", pair_id="pair-a007", match_score=0.97, name_score=0.94,
        city_match=True, zip_match=True, geo_score=None, match_type="fuzzy_name_city",
    ),
    dict(
        id=15, bl_id=41122, cost_center="IT-MIL-001",
        canonical_name="L'Osteria Milano Navigli",
        city="Milano", country="IT", zip="20143",
        address="Via Vigevano 20", phone="+39 02 89015544",
        lat=45.4516, lng=9.1765, tz="Europe/Rome", currency="EUR",
        region="Italy North", avg_rating=4.6, review_count=3412,
        website="https://losteria.net/it/mil-navigli",
        ls_name="L'Osteria Milano Navigli",
        ye_id="yext-it-mil-001", ye_name="L'Osteria Navigli Milano",
            ye_city="Milano", ye_zip="20143",
        mc_id=None, gp_id=None,
        match_status="auto_accepted", pair_id="pair-a008", match_score=0.98, name_score=0.96,
        city_match=True, zip_match=True, geo_score=0.99, match_type="composite",
    ),
    dict(
        id=16, bl_id=41145, cost_center="IT-ROM-001",
        canonical_name="L'Osteria Roma Trastevere",
        city="Roma", country="IT", zip="00153",
        address="Via della Lungaretta 78", phone="+39 06 58343456",
        lat=41.8893, lng=12.4697, tz="Europe/Rome", currency="EUR",
        region="Italy Central", avg_rating=4.5, review_count=2978,
        website="https://losteria.net/it/rom-trastevere",
        ls_name="L'Osteria Roma Trastevere",
        ye_id="yext-it-rom-001", ye_name="L'Osteria Trastevere",
            ye_city="Roma", ye_zip="00153",
        mc_id=None, gp_id=None,
        match_status="auto_accepted", pair_id="pair-a009", match_score=0.97, name_score=0.91,
        city_match=True, zip_match=True, geo_score=0.99, match_type="composite",
    ),
    dict(
        id=17, bl_id=41456, cost_center="NL-AMS-001",
        canonical_name="L'Osteria Amsterdam Jordaan",
        city="Amsterdam", country="NL", zip="1015 BH",
        address="Rozengracht 40", phone="+31 20 3308899",
        lat=52.3724, lng=4.8817, tz="Europe/Amsterdam", currency="EUR",
        region="Netherlands", avg_rating=4.4, review_count=1567,
        website="https://losteria.net/nl/ams-jordaan",
        ls_name="L'Osteria Amsterdam Jordaan",
        ye_id="yext-nl-ams-001", ye_name="L'Osteria Jordaan",
            ye_city="Amsterdam", ye_zip="1015 BH",
        mc_id=None, gp_id=None,
        match_status="auto_accepted", pair_id="pair-a010", match_score=0.98, name_score=0.95,
        city_match=True, zip_match=True, geo_score=None, match_type="fuzzy_name_city",
    ),
    dict(
        id=18, bl_id=41581, cost_center="PL-WAW-001",
        canonical_name="L'Osteria Warszawa Mokotów",
        city="Warszawa", country="PL", zip="02-562",
        address="ul. Puławska 43", phone="+48 22 3345566",
        lat=52.2103, lng=21.0122, tz="Europe/Warsaw", currency="PLN",
        region="Poland", avg_rating=4.3, review_count=2145,
        website="https://losteria.net/pl/waw-mokotow",
        ls_name="L'Osteria Warszawa Mokotów",
        gp_id="GP-PL-WAW-001", gp_name="L'Osteria Warsaw Mokotow",
            gp_city="Warsaw", gp_zip="02-562",
        ye_id=None, mc_id=None,
        match_status="auto_accepted", pair_id="pair-a011", match_score=0.97, name_score=0.93,
        city_match=True, zip_match=True, geo_score=0.99, match_type="composite",
    ),
    dict(
        id=19, bl_id=41588, cost_center="PL-KRK-001",
        canonical_name="L'Osteria Kraków Stare Miasto",
        city="Kraków", country="PL", zip="31-008",
        address="ul. Grodzka 52", phone="+48 12 4456677",
        lat=50.0577, lng=19.9412, tz="Europe/Warsaw", currency="PLN",
        region="Poland", avg_rating=4.5, review_count=2889,
        website="https://losteria.net/pl/krk-staremisto",
        ls_name="L'Osteria Kraków Stare Miasto",
        mc_id="MC-PL-KRK-001", mc_name="L'Osteria Krakow Old Town",
            mc_city="Krakow", mc_zip="31-008",
        ye_id=None, gp_id=None,
        match_status="auto_accepted", pair_id="pair-a012", match_score=0.98, name_score=0.94,
        city_match=True, zip_match=True, geo_score=None, match_type="fuzzy_name_city",
    ),
    # ──── 1 NO_MATCH (tylko Lightspeed, nowe otwarcie) ─────────────────────
    dict(
        id=20, bl_id=41601, cost_center="PL-WRO-001",
        canonical_name="L'Osteria Wrocław Rynek",
        city="Wrocław", country="PL", zip="50-101",
        address="Rynek 5", phone="+48 71 3456788",
        lat=51.1099, lng=17.0318, tz="Europe/Warsaw", currency="PLN",
        region="Poland", avg_rating=None, review_count=None,
        website="https://losteria.net/pl/wro-rynek",
        ls_name="L'Osteria Wrocław Rynek",
        ye_id=None, mc_id=None, gp_id=None,
        match_status="no_match", pair_id=None, match_score=None, name_score=None,
        city_match=None, zip_match=None, geo_score=None, match_type=None,
    ),
]

# Oblicz hash keys dla każdej lokalizacji
for loc in LOCS:
    loc["ls_hk"] = hk(f"lightspeed|{loc['bl_id']}")
    loc["ye_hk"] = hk(f"yext|{loc['ye_id']}")       if loc.get("ye_id") else None
    loc["mc_hk"] = hk(f"mcwin|{loc['mc_id']}")      if loc.get("mc_id") else None
    loc["gp_hk"] = hk(f"gopos|{loc['gp_id']}")      if loc.get("gp_id") else None
    # Canonical = Lightspeed (najwyższy priorytet survivorship)
    loc["canonical_hk"] = loc["ls_hk"]

print(f"Załadowano {len(LOCS)} lokalizacji demo")
print(f"  Pending:       {sum(1 for l in LOCS if l['match_status'] == 'pending')}")
print(f"  Auto-accepted: {sum(1 for l in LOCS if l['match_status'] == 'auto_accepted')}")
print(f"  No-match:      {sum(1 for l in LOCS if l['match_status'] == 'no_match')}")


# ---------------------------------------------------------------------------
# CELL 3: Bronze — lightspeed_businesses
# ---------------------------------------------------------------------------
ls_rows = [
    (
        1000 + loc["id"],                   # businessId
        "L'Osteria",                         # businessName
        loc["currency"],                     # currencyCode
        loc["bl_id"],                        # blId
        loc["ls_name"],                      # blName
        loc["country"],                      # country
        loc["tz"],                           # timezone
        "lightspeed",                        # _source_system
        BRONZE_DT,                           # _load_date
        RUN_ID,                              # _run_id
        TENANT,                              # _tenant_name
    )
    for loc in LOCS
]

ls_schema = StructType([
    StructField("businessId",    LongType()),
    StructField("businessName",  StringType()),
    StructField("currencyCode",  StringType()),
    StructField("blId",          LongType()),
    StructField("blName",        StringType()),
    StructField("country",       StringType()),
    StructField("timezone",      StringType()),
    StructField("_source_system",StringType()),
    StructField("_load_date",    TimestampType()),
    StructField("_run_id",       StringType()),
    StructField("_tenant_name",  StringType()),
])

df_ls = spark.createDataFrame(ls_rows, ls_schema)
df_ls.write.mode("append").saveAsTable(f"{LAKEHOUSE}.bronze.lightspeed_businesses")
print(f"✓ bronze.lightspeed_businesses: {df_ls.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 4: Bronze — yext_locations
# ---------------------------------------------------------------------------
ye_rows = [
    (
        loc["ye_id"],
        loc.get("ye_name", loc["canonical_name"]),
        loc["address"],
        loc.get("ye_city", loc["city"]),
        loc.get("ye_zip",  loc["zip"]),
        loc["country"],
        loc["phone"],
        loc["website"],
        loc["lat"],
        loc["lng"],
        loc["avg_rating"],
        loc["review_count"],
        "yext",
        BRONZE_DT,
        RUN_ID,
        TENANT,
    )
    for loc in LOCS if loc.get("ye_id")
]

ye_schema = StructType([
    StructField("id",                  StringType()),
    StructField("name",                StringType()),
    StructField("address_line1",       StringType()),
    StructField("address_city",        StringType()),
    StructField("address_postal_code", StringType()),
    StructField("address_country_code",StringType()),
    StructField("phone",               StringType()),
    StructField("website_url",         StringType()),
    StructField("display_lat",         DoubleType()),
    StructField("display_lng",         DoubleType()),
    StructField("avg_rating",          DoubleType()),
    StructField("review_count",        IntegerType()),
    StructField("_source_system",      StringType()),
    StructField("_load_date",          TimestampType()),
    StructField("_run_id",             StringType()),
    StructField("_tenant_name",        StringType()),
])

df_ye = spark.createDataFrame(ye_rows, ye_schema)
df_ye.write.mode("append").saveAsTable(f"{LAKEHOUSE}.bronze.yext_locations")
print(f"✓ bronze.yext_locations: {df_ye.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 5: Bronze — mcwin_restaurant_masterdata
# ---------------------------------------------------------------------------
mc_rows = [
    (
        loc["mc_id"],
        loc.get("mc_name", loc["canonical_name"]),
        loc["cost_center"],
        loc["region"],
        loc["country"],
        loc.get("mc_city", loc["city"]),
        loc.get("mc_zip",  loc["zip"]),
        loc["address"],
        "1",  # is_active
        "mcwin",
        BRONZE_DT,
        f"mcwin_masterdata_{BRONZE_DT.date()}.csv",
        RUN_ID,
        TENANT,
    )
    for loc in LOCS if loc.get("mc_id")
]

mc_schema = StructType([
    StructField("restaurant_id",   StringType()),
    StructField("restaurant_name", StringType()),
    StructField("cost_center",     StringType()),
    StructField("region",          StringType()),
    StructField("country",         StringType()),
    StructField("city",            StringType()),
    StructField("zip_code",        StringType()),
    StructField("address",         StringType()),
    StructField("is_active",       StringType()),
    StructField("_source_system",  StringType()),
    StructField("_load_date",      TimestampType()),
    StructField("_file_name",      StringType()),
    StructField("_run_id",         StringType()),
    StructField("_tenant_name",    StringType()),
])

df_mc = spark.createDataFrame(mc_rows, mc_schema)
df_mc.write.mode("append").saveAsTable(f"{LAKEHOUSE}.bronze.mcwin_restaurant_masterdata")
print(f"✓ bronze.mcwin_restaurant_masterdata: {df_mc.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 6: Bronze — gopos_locations
# ---------------------------------------------------------------------------
gp_rows = [
    (
        loc["gp_id"],
        loc.get("gp_name", loc["canonical_name"]),
        loc["address"],
        loc.get("gp_city", loc["city"]),
        loc.get("gp_zip",  loc["zip"]),
        loc["country"],
        loc["phone"],
        True,  # is_active
        "gopos",
        BRONZE_DT,
        RUN_ID,
        TENANT,
    )
    for loc in LOCS if loc.get("gp_id")
]

gp_schema = StructType([
    StructField("location_id",    StringType()),
    StructField("location_name",  StringType()),
    StructField("address",        StringType()),
    StructField("city",           StringType()),
    StructField("zip_code",       StringType()),
    StructField("country",        StringType()),
    StructField("phone",          StringType()),
    StructField("is_active",      BooleanType()),
    StructField("_source_system", StringType()),
    StructField("_load_date",     TimestampType()),
    StructField("_run_id",        StringType()),
    StructField("_tenant_name",   StringType()),
])

df_gp = spark.createDataFrame(gp_rows, gp_schema)
df_gp.write.mode("append").saveAsTable(f"{LAKEHOUSE}.bronze.gopos_locations")
print(f"✓ bronze.gopos_locations: {df_gp.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 7: Silver DV — hub_location
# ---------------------------------------------------------------------------
hub_rows = []
for loc in LOCS:
    # Lightspeed hub (zawsze)
    hub_rows.append((loc["ls_hk"], f"lightspeed|{loc['bl_id']}", SILVER_DT, "lightspeed"))
    if loc.get("ye_id"):
        hub_rows.append((loc["ye_hk"], f"yext|{loc['ye_id']}",        SILVER_DT, "yext"))
    if loc.get("mc_id"):
        hub_rows.append((loc["mc_hk"], f"mcwin|{loc['mc_id']}",       SILVER_DT, "mcwin"))
    if loc.get("gp_id"):
        hub_rows.append((loc["gp_hk"], f"gopos|{loc['gp_id']}",       SILVER_DT, "gopos"))

hub_schema = StructType([
    StructField("location_hk",   BinaryType()),
    StructField("business_key",  StringType()),
    StructField("load_date",     TimestampType()),
    StructField("record_source", StringType()),
])

df_hub = spark.createDataFrame(hub_rows, hub_schema)
df_hub.write.mode("append").saveAsTable(f"{LAKEHOUSE}.silver_dv.hub_location")
print(f"✓ silver_dv.hub_location: {df_hub.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 8: Silver DV — sat_location_lightspeed
# ---------------------------------------------------------------------------
sat_ls_rows = [
    (
        loc["ls_hk"],
        SILVER_DT,
        None,  # load_end_date (NULL = aktualny)
        hdiff(loc["ls_name"], loc["country"], loc["city"], loc["tz"], loc["currency"]),
        "lightspeed",
        loc["ls_name"],
        loc["country"],
        loc["city"],
        loc["tz"],
        loc["currency"],
        loc["bl_id"],
        True,  # is_active
        std(loc["ls_name"]),
        loc["country"],      # country_std = ISO2
        std(loc["city"]),
    )
    for loc in LOCS
]

sat_ls_schema = StructType([
    StructField("location_hk",    BinaryType()),
    StructField("load_date",      TimestampType()),
    StructField("load_end_date",  TimestampType()),
    StructField("hash_diff",      BinaryType()),
    StructField("record_source",  StringType()),
    StructField("name",           StringType()),
    StructField("country",        StringType()),
    StructField("city",           StringType()),
    StructField("timezone",       StringType()),
    StructField("currency_code",  StringType()),
    StructField("bl_id",          LongType()),
    StructField("is_active",      BooleanType()),
    StructField("name_std",       StringType()),
    StructField("country_std",    StringType()),
    StructField("city_std",       StringType()),
])

df_sat_ls = spark.createDataFrame(sat_ls_rows, sat_ls_schema)
df_sat_ls.write.mode("append").saveAsTable(f"{LAKEHOUSE}.silver_dv.sat_location_lightspeed")
print(f"✓ silver_dv.sat_location_lightspeed: {df_sat_ls.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 9: Silver DV — sat_location_yext
# ---------------------------------------------------------------------------
sat_ye_rows = [
    (
        loc["ye_hk"],
        SILVER_DT,
        None,
        hdiff(loc.get("ye_name", loc["canonical_name"]), loc.get("ye_city", loc["city"]),
              loc.get("ye_zip", loc["zip"]), loc["avg_rating"]),
        "yext",
        loc.get("ye_name", loc["canonical_name"]),
        loc["address"],
        loc.get("ye_city", loc["city"]),
        loc.get("ye_zip",  loc["zip"]),
        loc["country"],
        loc["phone"],
        loc["website"],
        loc["lat"],
        loc["lng"],
        loc["avg_rating"],
        loc["review_count"],
        std(loc.get("ye_name", loc["canonical_name"])),
        loc["country"],
        std(loc.get("ye_city", loc["city"])),
    )
    for loc in LOCS if loc.get("ye_id")
]

sat_ye_schema = StructType([
    StructField("location_hk",  BinaryType()),
    StructField("load_date",    TimestampType()),
    StructField("load_end_date",TimestampType()),
    StructField("hash_diff",    BinaryType()),
    StructField("record_source",StringType()),
    StructField("name",         StringType()),
    StructField("address_line1",StringType()),
    StructField("city",         StringType()),
    StructField("postal_code",  StringType()),
    StructField("country_code", StringType()),
    StructField("phone",        StringType()),
    StructField("website_url",  StringType()),
    StructField("latitude",     DoubleType()),
    StructField("longitude",    DoubleType()),
    StructField("avg_rating",   DoubleType()),
    StructField("review_count", IntegerType()),
    StructField("name_std",     StringType()),
    StructField("country_std",  StringType()),
    StructField("city_std",     StringType()),
])

df_sat_ye = spark.createDataFrame(sat_ye_rows, sat_ye_schema)
df_sat_ye.write.mode("append").saveAsTable(f"{LAKEHOUSE}.silver_dv.sat_location_yext")
print(f"✓ silver_dv.sat_location_yext: {df_sat_ye.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 10: Silver DV — sat_location_mcwin
# ---------------------------------------------------------------------------
sat_mc_rows = [
    (
        loc["mc_hk"],
        SILVER_DT,
        None,
        hdiff(loc.get("mc_name", loc["canonical_name"]), loc["cost_center"],
              loc.get("mc_city", loc["city"])),
        "mcwin",
        loc.get("mc_name", loc["canonical_name"]),
        loc["cost_center"],
        loc["region"],
        loc["country"],
        loc.get("mc_city", loc["city"]),
        loc.get("mc_zip",  loc["zip"]),
        loc["address"],
        "1",
        std(loc.get("mc_name", loc["canonical_name"])),
        loc["country"],
        std(loc.get("mc_city", loc["city"])),
    )
    for loc in LOCS if loc.get("mc_id")
]

sat_mc_schema = StructType([
    StructField("location_hk",     BinaryType()),
    StructField("load_date",       TimestampType()),
    StructField("load_end_date",   TimestampType()),
    StructField("hash_diff",       BinaryType()),
    StructField("record_source",   StringType()),
    StructField("restaurant_name", StringType()),
    StructField("cost_center",     StringType()),
    StructField("region",          StringType()),
    StructField("country",         StringType()),
    StructField("city",            StringType()),
    StructField("zip_code",        StringType()),
    StructField("address",         StringType()),
    StructField("is_active",       StringType()),
    StructField("name_std",        StringType()),
    StructField("country_std",     StringType()),
    StructField("city_std",        StringType()),
])

df_sat_mc = spark.createDataFrame(sat_mc_rows, sat_mc_schema)
df_sat_mc.write.mode("append").saveAsTable(f"{LAKEHOUSE}.silver_dv.sat_location_mcwin")
print(f"✓ silver_dv.sat_location_mcwin: {df_sat_mc.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 11: Silver DV — sat_location_gopos
# ---------------------------------------------------------------------------
sat_gp_rows = [
    (
        loc["gp_hk"],
        SILVER_DT,
        None,
        hdiff(loc.get("gp_name", loc["canonical_name"]), loc.get("gp_city", loc["city"]),
              loc.get("gp_zip", loc["zip"])),
        "gopos",
        loc.get("gp_name", loc["canonical_name"]),
        loc["address"],
        loc.get("gp_city", loc["city"]),
        loc.get("gp_zip",  loc["zip"]),
        loc["country"],
        loc["phone"],
        True,
        std(loc.get("gp_name", loc["canonical_name"])),
        loc["country"],
        std(loc.get("gp_city", loc["city"])),
    )
    for loc in LOCS if loc.get("gp_id")
]

sat_gp_schema = StructType([
    StructField("location_hk",  BinaryType()),
    StructField("load_date",    TimestampType()),
    StructField("load_end_date",TimestampType()),
    StructField("hash_diff",    BinaryType()),
    StructField("record_source",StringType()),
    StructField("location_name",StringType()),
    StructField("address",      StringType()),
    StructField("city",         StringType()),
    StructField("zip_code",     StringType()),
    StructField("country",      StringType()),
    StructField("phone",        StringType()),
    StructField("is_active",    BooleanType()),
    StructField("name_std",     StringType()),
    StructField("country_std",  StringType()),
    StructField("city_std",     StringType()),
])

df_sat_gp = spark.createDataFrame(sat_gp_rows, sat_gp_schema)
df_sat_gp.write.mode("append").saveAsTable(f"{LAKEHOUSE}.silver_dv.sat_location_gopos")
print(f"✓ silver_dv.sat_location_gopos: {df_sat_gp.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 12: Business Vault — bv_location_match_candidates
# ---------------------------------------------------------------------------
import uuid as _uuid

def right_hk(loc):
    """Zwraca hash key prawej strony kandydatury (non-Lightspeed source)"""
    if loc.get("ye_id"):  return loc["ye_hk"]
    if loc.get("mc_id"):  return loc["mc_hk"]
    if loc.get("gp_id"):  return loc["gp_hk"]
    return None

cand_rows = []
for loc in LOCS:
    rhk = right_hk(loc)
    if rhk is None or loc["match_status"] == "no_match":
        continue
    cand_rows.append((
        loc["pair_id"],
        loc["ls_hk"],       # hk_left  = Lightspeed (wyższy priorytet)
        rhk,                # hk_right = inne źródło
        loc["match_score"],
        loc["match_type"],
        loc.get("name_score"),
        loc.get("city_match"),
        loc.get("zip_match"),
        loc.get("geo_score"),
        loc["match_status"],          # pending | auto_accepted
        SILVER_DT,                    # created_at
        "auto_matcher" if loc["match_status"] == "auto_accepted" else None,
        SILVER_DT if loc["match_status"] == "auto_accepted" else None,
        "Score ≥ 0.97 — auto-accepted" if loc["match_status"] == "auto_accepted" else None,
    ))

cand_schema = StructType([
    StructField("pair_id",     StringType()),
    StructField("hk_left",     BinaryType()),
    StructField("hk_right",    BinaryType()),
    StructField("match_score", DoubleType()),
    StructField("match_type",  StringType()),
    StructField("name_score",  DoubleType()),
    StructField("city_match",  BooleanType()),
    StructField("zip_match",   BooleanType()),
    StructField("geo_score",   DoubleType()),
    StructField("status",      StringType()),
    StructField("created_at",  TimestampType()),
    StructField("reviewed_by", StringType()),
    StructField("reviewed_at", TimestampType()),
    StructField("review_note", StringType()),
])

df_cand = spark.createDataFrame(cand_rows, cand_schema)
df_cand.write.mode("append").saveAsTable(f"{LAKEHOUSE}.silver_dv.bv_location_match_candidates")
print(f"✓ silver_dv.bv_location_match_candidates: {df_cand.count()} wierszy")
print(f"  pending={sum(1 for r in cand_rows if r[9]=='pending')}, auto_accepted={sum(1 for r in cand_rows if r[9]=='auto_accepted')}")


# ---------------------------------------------------------------------------
# CELL 13: Business Vault — bv_location_key_resolution
# ---------------------------------------------------------------------------
# Dla każdego auto_accepted: źródłowy hk_right → canonical hk_left (Lightspeed)
res_rows = []
for loc in LOCS:
    if loc["match_status"] != "auto_accepted":
        continue
    rhk = right_hk(loc)
    if rhk is None:
        continue
    res_rows.append((
        rhk,              # source_hk (zostaje wchłonięty)
        loc["ls_hk"],     # canonical_hk (Lightspeed = canonical)
        "auto_matcher",   # resolved_by
        SILVER_DT,        # resolved_at
        loc["pair_id"],   # pair_id
        "auto",           # resolution_type
    ))

res_schema = StructType([
    StructField("source_hk",       BinaryType()),
    StructField("canonical_hk",    BinaryType()),
    StructField("resolved_by",     StringType()),
    StructField("resolved_at",     TimestampType()),
    StructField("pair_id",         StringType()),
    StructField("resolution_type", StringType()),
])

df_res = spark.createDataFrame(res_rows, res_schema)
df_res.write.mode("append").saveAsTable(f"{LAKEHOUSE}.silver_dv.bv_location_key_resolution")
print(f"✓ silver_dv.bv_location_key_resolution: {df_res.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 14: Silver DV — pit_location (Point-In-Time)
# ---------------------------------------------------------------------------
pit_rows = []
for loc in LOCS:
    pit_rows.append((
        loc["ls_hk"],                                    # location_hk (canonical)
        GOLD_DT,                                         # snapshot_date
        SILVER_DT,                                       # sat_lightspeed_ld (always)
        SILVER_DT if loc.get("ye_id") else None,         # sat_yext_ld
        SILVER_DT if loc.get("mc_id") else None,         # sat_mcwin_ld
        SILVER_DT if loc.get("gp_id") else None,         # sat_gopos_ld
    ))

pit_schema = StructType([
    StructField("location_hk",       BinaryType()),
    StructField("snapshot_date",     TimestampType()),
    StructField("sat_lightspeed_ld", TimestampType()),
    StructField("sat_yext_ld",       TimestampType()),
    StructField("sat_mcwin_ld",      TimestampType()),
    StructField("sat_gopos_ld",      TimestampType()),
])

df_pit = spark.createDataFrame(pit_rows, pit_schema)
df_pit.write.mode("append").saveAsTable(f"{LAKEHOUSE}.silver_dv.pit_location")
print(f"✓ silver_dv.pit_location: {df_pit.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 15: Gold — dim_location (Golden Records, SCD2 current row)
# ---------------------------------------------------------------------------
# Survivorship: name/city/country/zip/address/phone z Lightspeed
#               lat/lng/avg_rating/review_count/website z Yext (jeśli dostępne)
#               cost_center/region z McWin (jeśli dostępne), fallback Lightspeed seed

gold_rows = []
for i, loc in enumerate(LOCS, start=1):
    # Wybierz źródło dla pól wg priorytetu
    name_src   = "lightspeed"
    geo_src    = "yext" if loc.get("ye_id") else "lightspeed"
    fin_src    = "mcwin" if loc.get("mc_id") else "lightspeed"

    gold_rows.append((
        i,                             # location_sk (manual seq)
        loc["canonical_hk"],           # location_hk
        GOLD_DT,                       # valid_from
        None,                          # valid_to (current)
        True,                          # is_current
        loc["canonical_name"],         # name
        loc["country"],                # country
        loc["city"],                   # city
        loc["zip"],                    # zip_code
        loc["address"],                # address
        loc["phone"],                  # phone
        loc["lat"],                    # latitude
        loc["lng"],                    # longitude
        loc["website"],                # website_url
        loc["tz"],                     # timezone
        loc["currency"],               # currency_code
        loc["avg_rating"],             # avg_rating (Yext)
        loc["review_count"],           # review_count (Yext)
        loc["cost_center"],            # cost_center (McWin/seed)
        loc["region"],                 # region (McWin/seed)
        name_src,                      # name_source
        "lightspeed",                  # country_source
        "lightspeed",                  # city_source
        GOLD_DT,                       # created_at
        GOLD_DT,                       # updated_at
        loc["bl_id"],                  # lightspeed_bl_id
        loc.get("ye_id"),              # yext_id
        loc.get("mc_id"),              # mcwin_restaurant_id
        loc.get("gp_id"),              # gopos_location_id
    ))

gold_schema = StructType([
    StructField("location_sk",         LongType()),
    StructField("location_hk",         BinaryType()),
    StructField("valid_from",          TimestampType()),
    StructField("valid_to",            TimestampType()),
    StructField("is_current",          BooleanType()),
    StructField("name",                StringType()),
    StructField("country",             StringType()),
    StructField("city",                StringType()),
    StructField("zip_code",            StringType()),
    StructField("address",             StringType()),
    StructField("phone",               StringType()),
    StructField("latitude",            DoubleType()),
    StructField("longitude",           DoubleType()),
    StructField("website_url",         StringType()),
    StructField("timezone",            StringType()),
    StructField("currency_code",       StringType()),
    StructField("avg_rating",          DoubleType()),
    StructField("review_count",        IntegerType()),
    StructField("cost_center",         StringType()),
    StructField("region",              StringType()),
    StructField("name_source",         StringType()),
    StructField("country_source",      StringType()),
    StructField("city_source",         StringType()),
    StructField("created_at",          TimestampType()),
    StructField("updated_at",          TimestampType()),
    StructField("lightspeed_bl_id",    LongType()),
    StructField("yext_id",             StringType()),
    StructField("mcwin_restaurant_id", StringType()),
    StructField("gopos_location_id",   StringType()),
])

df_gold = spark.createDataFrame(gold_rows, gold_schema)
df_gold.write.mode("append").saveAsTable(f"{LAKEHOUSE}.gold.dim_location")
print(f"✓ gold.dim_location: {df_gold.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 16: Gold — dim_location_quality
# ---------------------------------------------------------------------------
KEY_FIELDS = ["name", "country", "city", "zip", "address", "phone",
              "lat", "lng", "website", "cost_center"]

def completeness(loc) -> float:
    filled = sum(1 for f in KEY_FIELDS if loc.get(f) is not None and loc.get(f) != "")
    return round(filled / len(KEY_FIELDS), 2)

def sources_count(loc) -> int:
    return sum([
        1,                              # Lightspeed always
        1 if loc.get("ye_id") else 0,
        1 if loc.get("mc_id") else 0,
        1 if loc.get("gp_id") else 0,
    ])

qual_rows = [
    (
        loc["canonical_hk"],
        GOLD_DT,
        sources_count(loc),
        completeness(loc),
        True,
        bool(loc.get("ye_id")),
        bool(loc.get("mc_id")),
        bool(loc.get("gp_id")),
        loc.get("match_score"),
    )
    for loc in LOCS
]

qual_schema = StructType([
    StructField("location_hk",       BinaryType()),
    StructField("snapshot_date",     TimestampType()),
    StructField("sources_count",     IntegerType()),
    StructField("completeness_score",DoubleType()),
    StructField("has_lightspeed",    BooleanType()),
    StructField("has_yext",          BooleanType()),
    StructField("has_mcwin",         BooleanType()),
    StructField("has_gopos",         BooleanType()),
    StructField("last_match_score",  DoubleType()),
])

df_qual = spark.createDataFrame(qual_rows, qual_schema)
df_qual.write.mode("append").saveAsTable(f"{LAKEHOUSE}.gold.dim_location_quality")
print(f"✓ gold.dim_location_quality: {df_qual.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 17: Silver DV — stewardship_log (historia decyzji stewarda)
# ---------------------------------------------------------------------------
# Kilka realistycznych akcji: akceptacje, override pola, jeden reject

# Canonical HK dla demo lokalizacji na potrzeby logu
muc_hk   = LOCS[0]["canonical_hk"]   # München Marienplatz
fra_hk   = LOCS[1]["canonical_hk"]   # Frankfurt Sachsenhausen
ber_hk   = LOCS[9]["canonical_hk"]   # Berlin Mitte
waw_hk   = LOCS[17]["canonical_hk"]  # Warszawa Mokotów

log_rows = [
    # Auto-accepted (12 wpisów — jeden na każdą auto_accepted parę)
    *[
        (
            str(_uuid.uuid4()),
            loc["canonical_hk"],
            "accept_match",
            None, None, None,
            "auto_matcher",
            SILVER_DT,
            loc["pair_id"],
            "Score ≥ 0.97 — automatic acceptance",
        )
        for loc in LOCS if loc["match_status"] == "auto_accepted"
    ],
    # Ręczna akceptacja przez stewarda (dwa przykłady z historii)
    (
        str(_uuid.uuid4()), muc_hk, "accept_match",
        None, None, None,
        "anna.nowak@losteria.com", AUDIT_DT1,
        "pair-001-prev",
        "Address and phone confirmed — same location",
    ),
    (
        str(_uuid.uuid4()), fra_hk, "accept_match",
        None, None, None,
        "jan.kowalski@losteria.com", AUDIT_DT1,
        "pair-002-prev",
        "Verified with Frankfurt regional manager",
    ),
    # Korekta pola przez stewarda
    (
        str(_uuid.uuid4()), muc_hk, "override_field",
        "phone", "+49 89 12345600", "+49 89 12345678",
        "anna.nowak@losteria.com", AUDIT_DT2,
        None,
        "Confirmed with restaurant manager",
    ),
    (
        str(_uuid.uuid4()), ber_hk, "override_field",
        "zip_code", "10115", "10117",
        "jan.kowalski@losteria.com", AUDIT_DT3,
        None,
        "Postal code corrected after site visit",
    ),
    # Odrzucenie fałszywej pary (różne restauracje z podobną nazwą)
    (
        str(_uuid.uuid4()), waw_hk, "reject_match",
        None, None, None,
        "anna.nowak@losteria.com", AUDIT_DT3,
        "pair-false-001",
        "Different concept restaurant — confirmed not L'Osteria franchise",
    ),
]

log_schema = StructType([
    StructField("log_id",       StringType()),
    StructField("canonical_hk", BinaryType()),
    StructField("action",       StringType()),
    StructField("field_name",   StringType()),
    StructField("old_value",    StringType()),
    StructField("new_value",    StringType()),
    StructField("changed_by",   StringType()),
    StructField("changed_at",   TimestampType()),
    StructField("pair_id",      StringType()),
    StructField("reason",       StringType()),
])

df_log = spark.createDataFrame(log_rows, log_schema)
df_log.write.mode("append").saveAsTable(f"{LAKEHOUSE}.silver_dv.stewardship_log")
print(f"✓ silver_dv.stewardship_log: {df_log.count()} wierszy")


# ---------------------------------------------------------------------------
# CELL 18: Podsumowanie i weryfikacja
# ---------------------------------------------------------------------------
print("\n" + "="*60)
print("  DEMO DATA SEED — PODSUMOWANIE")
print("="*60)

tables = [
    (f"{LAKEHOUSE}.bronze.lightspeed_businesses",           "Bronze / Lightspeed"),
    (f"{LAKEHOUSE}.bronze.yext_locations",                  "Bronze / Yext"),
    (f"{LAKEHOUSE}.bronze.mcwin_restaurant_masterdata",     "Bronze / McWin"),
    (f"{LAKEHOUSE}.bronze.gopos_locations",                 "Bronze / GoPOS"),
    (f"{LAKEHOUSE}.silver_dv.hub_location",                 "Silver / Hub"),
    (f"{LAKEHOUSE}.silver_dv.sat_location_lightspeed",      "Silver / Sat LS"),
    (f"{LAKEHOUSE}.silver_dv.sat_location_yext",            "Silver / Sat Yext"),
    (f"{LAKEHOUSE}.silver_dv.sat_location_mcwin",           "Silver / Sat McWin"),
    (f"{LAKEHOUSE}.silver_dv.sat_location_gopos",           "Silver / Sat GoPOS"),
    (f"{LAKEHOUSE}.silver_dv.bv_location_match_candidates", "Business Vault / Candidates"),
    (f"{LAKEHOUSE}.silver_dv.bv_location_key_resolution",   "Business Vault / Resolution"),
    (f"{LAKEHOUSE}.silver_dv.pit_location",                 "Silver / PIT"),
    (f"{LAKEHOUSE}.gold.dim_location",                      "Gold / dim_location"),
    (f"{LAKEHOUSE}.gold.dim_location_quality",              "Gold / Quality"),
    (f"{LAKEHOUSE}.silver_dv.stewardship_log",              "Audit / Stewardship Log"),
]

for tbl, label in tables:
    cnt = spark.table(tbl).count()
    print(f"  {label:<40} {cnt:>4} wierszy")

n_golden   = spark.table(f"{LAKEHOUSE}.gold.dim_location").filter("is_current = true").count()
n_pending  = spark.table(f"{LAKEHOUSE}.silver_dv.bv_location_match_candidates").filter("status = 'pending'").count()
n_auto     = spark.table(f"{LAKEHOUSE}.silver_dv.bv_location_match_candidates").filter("status = 'auto_accepted'").count()
avg_compl  = spark.table(f"{LAKEHOUSE}.gold.dim_location_quality").agg({"completeness_score": "avg"}).collect()[0][0]
print("="*60)
print(f"  Lokalizacje złote (current): {n_golden}")
print(f"  Kandydatury pending:         {n_pending}")
print(f"  Kandydatury auto-accepted:   {n_auto}")
print(f"  Avg completeness score:      {avg_compl:.2f}")
print("="*60)
print("  ✅  Demo seed zakończony pomyślnie!")
print("  Otwórz Stewardship UI → zobaczysz 7 par do review")
print("  Gold layer ma 20 rekordów z 6 krajów (DE, AT, CH, IT, NL, PL)")
print("="*60)
