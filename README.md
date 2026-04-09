# MDM Stewardship App — L'Osteria

Fabric-native MDM stewardship application dla sieci restauracji L'Osteria.

## Architektura

```
Bronze (raw Delta) → Silver Data Vault Lite (Hub + Satellites) → Gold (dim_location SCD2)
                          ↑ MDM matching + steward review
                     Stewardship UI (React + Azure AD)
```

## Struktura repozytorium

```
fabric/
├── lakehouse/
│   ├── ddl/
│   │   ├── bronze/          # Tabele landing zone
│   │   ├── silver_dv/       # Data Vault: Hub, Satellites, BV
│   │   ├── gold/            # Golden records SCD2
│   │   └── mdm_config/      # Tabele konfiguracyjne MDM
│   └── seed/                # Dane inicjalne (priorytety, config)
├── notebooks/
│   ├── nb_load_raw_vault_location.py    # Bronze → DV loader
│   ├── nb_match_location.py             # Matching + Business Vault
│   └── nb_derive_gold_location.py       # PIT + Survivorship → Gold
└── pipelines/
    └── PL_MDM_Master_Location.json      # Orchestration pipeline

stewardship-ui/                          # React + TypeScript
├── src/
│   ├── api/mdmApi.ts          # Fabric SQL Endpoint + Azure Function client
│   ├── hooks/useMdm.ts        # TanStack Query hooks
│   ├── types/mdm.types.ts     # Domain types
│   └── components/
│       ├── ReviewQueue/       # Lista pending match pairs
│       ├── PairDetail/        # Side-by-side comparison + accept/reject
│       └── GoldenViewer/      # Golden record + audit log + editable fields
```

## Pierwsze uruchomienie

### 1. Fabric Lakehouse setup
```sql
-- Uruchom w kolejności w Fabric Notebook (SQL cell):
-- 1. fabric/lakehouse/ddl/mdm_config/create_mdm_config_tables.sql
-- 2. fabric/lakehouse/ddl/bronze/create_bronze_tables.sql
-- 3. fabric/lakehouse/ddl/silver_dv/create_silver_dv_tables.sql
-- 4. fabric/lakehouse/ddl/gold/create_gold_tables.sql
-- 5. fabric/lakehouse/seed/seed_mdm_config_location.sql
```

### 2. Pierwsze załadowanie danych
```
Uruchom w Fabric: PL_MDM_Master_Location z __paramFullLoad = true
```

### 3. React UI
```bash
cd stewardship-ui
cp .env.example .env  # wypełnij zmienne środowiskowe
npm install
npm run dev
```

## Zmienne środowiskowe (stewardship-ui/.env)

```env
VITE_FABRIC_SQL_ENDPOINT=https://{workspace}.{region}.pbidedicated.windows.net
VITE_WRITE_API_URL=https://{function-app}.azurewebsites.net
VITE_TENANT_ID=<azure-tenant-id>
VITE_CLIENT_ID=<app-registration-client-id>
```

## Data Vault — kluczowy wzorzec

```
Hub key = SHA256("lightspeed|41839-1")  ← jedna restauracja = jeden Hub key
                    ↓
sat_location_lightspeed  ← historyzowane atrybuty Lightspeed
sat_location_yext        ← historyzowane atrybuty Yext
(po accept match pair):
bv_location_key_resolution: source_hk(yext) → canonical_hk(lightspeed)
→ obie Satellites pod tym samym Hub key
```

## Dodanie nowej encji (np. Item)

1. Dodaj DDL do `ddl/bronze/` i `ddl/silver_dv/`
2. Dodaj seed do `seed/seed_mdm_config_item.sql`
3. Skopiuj notebooki z `_location` → `_item`, zmień nazwy tabel
4. Zarejestruj entity w `mdm_config.entity_config`
5. Pipeline automatycznie obsłuży przez `__paramEntityId`

## Źródła wzorców

| Wzorzec | Źródło w losadf/ |
|---------|-----------------|
| `__param` naming | Wszystkie ADF pipelines |
| Logging framework | `adf-master-dwh-dev-gwc/pipeline/PL_Master_Execution.json` |
| Error cascade | `PL_Data_ErrorReporting.json` |
| Watermark / incremental | `adf-yext-dwh-dev-gwc` (tblExtractionLog pattern) |
| Config-driven execution | `adf-manage-dwh-gwc` (tblDwhMigrationTable) |
