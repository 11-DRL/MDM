# MDM Stewardship App — L'Osteria

Fabric-natywna aplikacja do zarządzania danymi referencyjnymi (MDM) dla sieci restauracji L'Osteria (~200 lokalizacji, 6 krajów). Unifikuje dane biznesowych lokalizacji z 4 systemów źródłowych przez Data Vault Lite na Fabric Lakehouse, z interfejsem React do przeglądania i zatwierdzania rekordów.

---

## Spis treści

1. [Architektura systemu](#1-architektura-systemu)
2. [Struktura repozytorium](#2-struktura-repozytorium)
3. [Warstwa danych — Fabric Lakehouse](#3-warstwa-danych--fabric-lakehouse)
4. [Przetwarzanie danych — Notebooki](#4-przetwarzanie-danych--notebooki)
5. [Orchestracja — Pipeline](#5-orchestracja--pipeline)
6. [Stewardship UI — React](#6-stewardship-ui--react)
7. [Azure Function — Write Proxy](#7-azure-function--write-proxy)
8. [Fabric Workload — natywna integracja](#8-fabric-workload--natywna-integracja)
9. [Pierwsze uruchomienie](#9-pierwsze-uruchomienie)
10. [Wdrożenie produkcyjne](#10-wdrożenie-produkcyjne)
11. [Rozszerzanie systemu](#11-rozszerzanie-systemu)
12. [Wzorce i decyzje architektoniczne](#12-wzorce-i-decyzje-architektoniczne)

---

## 1. Architektura systemu

```
Źródła danych                  Fabric Lakehouse (lh_mdm)               UI / Fabric
─────────────────              ─────────────────────────────────────    ────────────
Lightspeed (POS)  ──┐
Yext (locations)  ──┤── Bronze ──▶ Silver Data Vault Lite ──▶ Gold ──▶ Stewardship
McWin (finance)   ──┤   (raw)      (Hub + Satellites + BV)   (SCD2)    React App
GoPOS (POS)       ──┘                      ▲
                                     MDM Matching
                                   (Jaro-Winkler + Geo)
                                           │
                                   Steward Review Queue
                                   (accept / reject pairs)
```

### Przepływ danych krok po kroku

| Krok | Notebook / Pipeline | Input → Output |
|------|--------------------|-|
| 1. Extract | PL_MDM_Master_Location | Systemy źródłowe → Bronze Delta tables |
| 2. Load Raw Vault | nb_load_raw_vault_location | Bronze → hub_location + sat_location_* |
| 3. Match | nb_match_location | Hub records → bv_location_match_candidates |
| 4. Review | Stewardship UI | Steward accept/reject → bv_location_key_resolution |
| 5. Derive Gold | nb_derive_gold_location | PIT + Satellites → gold.dim_location (SCD2) |

---

## 2. Struktura repozytorium

```
MDM/
├── fabric/
│   ├── lakehouse/
│   │   ├── ddl/
│   │   │   ├── mdm_config/          # 6 tabel konfiguracyjnych
│   │   │   │   └── create_mdm_config_tables.sql
│   │   │   ├── bronze/              # 4 landing tables (append-only)
│   │   │   │   └── create_bronze_tables.sql
│   │   │   ├── silver_dv/           # Data Vault: Hub, 4×Sat, BV, PIT, audit
│   │   │   │   └── create_silver_dv_tables.sql
│   │   │   └── gold/                # Golden records SCD2 + quality
│   │   │       └── create_gold_tables.sql
│   │   └── seed/
│   │       └── seed_mdm_config_location.sql   # Priorytety, wagi, blocking keys
│   ├── notebooks/
│   │   ├── nb_load_raw_vault_location.py      # Bronze → Data Vault
│   │   ├── nb_match_location.py               # Matching + BV candidates
│   │   └── nb_derive_gold_location.py         # PIT → Survivorship → Gold
│   ├── pipelines/
│   │   └── PL_MDM_Master_Location.json        # Master orchestration pipeline
│   └── workload/                              # Fabric Extensibility Toolkit
│       ├── BE/
│       │   ├── WorkloadManifest.xml           # Workload identity + URL frontendu
│       │   └── MDMStewardship.xml             # Item Type + AAD scopes
│       └── FE/
│           ├── Product.json                   # Portal metadata (nazwa, opis)
│           ├── MDMStewardship.json            # Frontend item config
│           └── assets/                        # Ikony PNG 32×32 i 44×44
│
├── stewardship-ui/                            # React 18 + TypeScript + Vite
│   ├── src/
│   │   ├── api/
│   │   │   ├── mdmApi.ts                      # HTTP klient + auth routing
│   │   │   └── mockData.ts                    # Demo dane (7 L'Osteria par)
│   │   ├── hooks/useMdm.ts                    # TanStack Query hooks
│   │   ├── lib/
│   │   │   ├── utils.ts                       # cn() helper
│   │   │   └── fabricHost.ts                  # Fabric iFrame token bridge
│   │   ├── types/mdm.types.ts                 # Domain types (DV, BV, Gold)
│   │   ├── components/
│   │   │   ├── ReviewQueue/                   # Lista pending par + stats
│   │   │   ├── PairDetail/                    # Side-by-side + score breakdown
│   │   │   └── GoldenViewer/                  # Edytowalny golden record + audit
│   │   └── App.tsx                            # Router + 3-tryb auth
│   ├── staticwebapp.config.json               # SPA routing + CSP headers
│   └── .env.example
│
├── azure-function/                            # TypeScript Azure Function v4
│   └── src/functions/mdmWrite.ts             # POST review + POST override
│
└── .github/workflows/
    └── deploy-ui.yml                          # GitHub Actions → Azure SWA
```

---

## 3. Warstwa danych — Fabric Lakehouse

Lakehouse: **`lh_mdm`** | Schematy: `mdm_config`, `bronze`, `silver_dv`, `gold`

### 3.1 Schemat `mdm_config` — konfiguracja

| Tabela | Rola |
|--------|------|
| `entity_config` | Lista aktywnych encji MDM; progi matchingu i auto-accept |
| `field_config` | Wagi pól do scoringu; flaga blocking key; standaryzator |
| `source_priority` | Survivorship: priorytet źródła per encja i pole (wildcard `*`) |
| `hash_config` | Template business_key per źródło (np. `lightspeed|{bl_id}`) |
| `source_watermark` | Watermark incremental load (wzorzec `tblExtractionLog`) |
| `execution_log` | Log każdego uruchomienia notebooka/pipeline |

### 3.2 Schemat `bronze` — landing zone

Cztery tabele **append-only** (bez transformacji, pełna historia ładowań):

| Tabela | Źródło |
|--------|--------|
| `bronze_location_lightspeed` | Lightspeed POS API |
| `bronze_location_yext` | Yext Location Cloud |
| `bronze_location_mcwin` | McWin Finance |
| `bronze_location_gopos` | GoPOS POS System |

### 3.3 Schemat `silver_dv` — Data Vault Lite

```
hub_location (1 wiersz = 1 unikalna restauracja)
  location_hk = SHA256("lightspeed|41839-1")
  │
  ├── sat_location_lightspeed   ← atrybuty POS (name, country, city, timezone…)
  ├── sat_location_yext         ← atrybuty lokalizacji (phone, geo, rating…)
  ├── sat_location_mcwin        ← atrybuty finansowe (cost_center, region…)
  └── sat_location_gopos        ← atrybuty POS (name, address, phone…)

bv_location_match_candidates   ← pary (hk_left, hk_right) + score + status
bv_location_key_resolution     ← decyzje stewarda: source_hk → canonical_hk
pit_location                   ← Point-In-Time snapshot (przyspiesza Gold derive)
stewardship_log                ← append-only audit każdej akcji
```

**Kluczowy wzorzec — key resolution:**

```
Przed: hub(lightspeed|41839) i hub(yext|muc-marienplatz) = 2 osobne Hub keys
                                      ↓ Steward klika Accept
bv_location_key_resolution: source_hk=yext-hash → canonical_hk=lightspeed-hash
                                      ↓ Następny DV load
Satelita Yext trafia pod Hub key Lightspeed → jeden rekord, dwa źródła
```

### 3.4 Schemat `gold` — Golden Records

| Tabela | Zawartość |
|--------|-----------|
| `dim_location` | SCD2 golden record (is_current + valid_from/to + crosswalk IDs) |
| `dim_location_quality` | Completeness score per lokalizacja per źródło |

---

## 4. Przetwarzanie danych — Notebooki

### `nb_load_raw_vault_location.py` — Bronze → Data Vault

**Co robi:**
1. Czyta watermark z `mdm_config.source_watermark`
2. Ładuje nowe rekordy z Bronze (filtr `load_timestamp > watermark`)
3. Oblicza `location_hk = SHA256(business_key_template)` per rekord
4. Sprawdza `bv_location_key_resolution` → mapuje source_hk na canonical_hk
5. MERGE do `hub_location` (nowe klucze)
6. Oblicza `hash_diff = SHA256(wszystkie atrybuty)` → MERGE do właściwego Satellite
7. Aktualizuje watermark

**Parametry notebooka:**
```python
run_id             # UUID uruchomienia (z pipeline)
source_system      # 'lightspeed' | 'yext' | 'mcwin' | 'gopos'
full_load          # true = ignoruj watermark, ładuj wszystko
```

### `nb_match_location.py` — Matching + Business Vault

**Algorytm:**

```
Blocking: GROUP BY (country_std, city_std)
    → eliminuje O(n²) par z różnych miast

Scoring per para w tym samym bloku:
  score = 0.50 × jaro_winkler(name_std_L, name_std_R)
        + 0.30 × exact(zip_code)
        + 0.20 × geo_score(lat/lon < 0.5km → 1.0, < 2km → 0.5)

Auto-accept: score ≥ 0.97 → INSERT do bv_location_key_resolution (auto)
Pending:     score ≥ 0.85 → INSERT do bv_location_match_candidates (status=pending)
Ignoruj:     score < 0.85
```

**Biblioteki:** `jellyfish` (Jaro-Winkler), `math` (Haversine geo distance)

**Zależność:** Wymaga `pip install jellyfish` w Fabric environment.

### `nb_derive_gold_location.py` — PIT → Survivorship → Gold

**Co robi:**
1. Buduje PIT snapshot: najnowszy `load_date` z każdego Satellite per Hub key
2. Joinnuje PIT z wszystkimi Satellites na `(location_hk, load_date)`
3. Stosuje survivorship przez COALESCE w kolejności priorytetów:
   - Nazwa: Lightspeed(1) > McWin(2) > Yext(3) > GoPOS(4)
   - Geo (lat/lon): Yext(1) > Lightspeed(2) > GoPOS(3)
   - Cost center: McWin(1) > Lightspeed(2)
4. MERGE do `gold.dim_location` (SCD2: nowy wiersz gdy zmiana, `valid_to` na starym)
5. Oblicza `completeness_score` → INSERT do `dim_location_quality`

---

## 5. Orchestracja — Pipeline

**`PL_MDM_Master_Location.json`** — Fabric Data Pipeline

```
Start
  │
  ├─ [Parallel] Extract Lightspeed ──┐
  ├─ [Parallel] Extract Yext        ├──▶ Bronze tables
  ├─ [Parallel] Extract McWin       │
  └─ [Parallel] Extract GoPOS ──────┘
  │
  ▼
nb_load_raw_vault_location
  │
  ▼
nb_match_location
  │
  ├─ [If pending_count > 0] ──▶ Teams Webhook: "7 par czeka na review"
  │
  ▼
nb_derive_gold_location
  │
  ▼ (on failure at any step)
Error Handler → execution_log (status=Failed)
```

**Parametry pipeline:**
```json
"__paramTenantName":   "losteria"
"__paramEntityId":     "business_location"
"__paramFullLoad":     false
```

---

## 6. Stewardship UI — React

### Tryby pracy (auto-detekcja)

| Tryb | Warunek | Auth |
|------|---------|------|
| **Mock** | `VITE_MOCK_MODE=true` | brak — 7 przykładowych par L'Osteria |
| **Fabric iFrame** | `window.self !== window.top` | token z Fabric hosta (FabricHostBridge) |
| **Standalone** | poza Fabric | Azure AD login przez MSAL |

### Ekrany

**Review Queue** (`/queue`)
- Stats bar: pending / auto-accepted / golden records / avg completeness
- Tabela par z inline Accept ✓ / Reject ✗
- Score badge: zielony ≥97%, niebieski ≥90%, amber ≥85%
- Paginacja; odświeżanie co 30s

**Pair Detail** (`/pairs/:pairId`)
- Side-by-side porównanie atrybutów Lewej i Prawej lokalizacji
- Score breakdown (name/zip/geo)
- Accept z wyborem canonical HK / Reject z polem reason

**Golden Viewer** (`/golden/:locationHk`)
- Wszystkie atrybuty golden record z badge źródła (Lightspeed / Yext / etc.)
- Edycja inline: hover → pencil icon → modal z polem reason
- Audit log (append-only, od najnowszego)
- Crosswalk IDs (lightspeed_bl_id, yext_id, mcwin_restaurant_id, gopos_location_id)

### Wzorzec komunikacji UI

```
UI → GET/POST /api/mdm/* → Azure Function
                             ↓
                    SELECT/UPDATE/INSERT przez MSI do Fabric SQL Endpoint
```

UI **nie łączy się bezpośrednio** z SQL Analytics Endpoint. Odczyt i zapis idą przez Azure Function.

### Zmienne środowiskowe

```env
VITE_MOCK_MODE=true                   # local dev bez konfiguracji
VITE_WRITE_API_URL=https://...        # Azure Function App URL
VITE_TENANT_ID=<guid>                 # Azure AD tenant
VITE_CLIENT_ID=<guid>                 # App Registration client ID
```

---

## 7. Azure Function — Read/Write Proxy

**`azure-function/src/functions/mdmWrite.ts`** — TypeScript Azure Function v4

### Endpointy

```
POST /api/mdm/location/review
Body: { pairId, action: "accept"|"reject", canonicalHk?, reason? }

  1. UPDATE bv_location_match_candidates SET status = action, reviewed_by, reviewed_at
  2. (jeśli accept) INSERT bv_location_key_resolution (source_hk → canonical_hk)
  3. INSERT stewardship_log

POST /api/mdm/location/override
Body: { locationHk, fieldName, newValue, reason }

  Whitelist pól: name, city, zip_code, country, phone, website_url,
                 timezone, currency_code, cost_center, region
  INSERT stewardship_log (gold re-derivowany przez kolejny pipeline run)

GET /api/mdm/queue/stats
GET /api/mdm/location/candidates?page=1&pageSize=25&status=pending
GET /api/mdm/location/pair/{pairId}
GET /api/mdm/location/golden
GET /api/mdm/location/golden/{locationHk}
GET /api/mdm/location/log/{locationHk}
GET /api/mdm/config/field-config?entityId=business_location
GET /api/mdm/config/source-priority?entityId=business_location

GET /api/health → { status: "ok" }
```

### Auth i bezpieczeństwo

- **Managed Identity** — Function App łączy się do Fabric SQL Endpoint bez haseł
- Tożsamość MSI wymaga roli **Contributor** w Fabric Workspace
- Field whitelist w `overrideField` — zapobiega injection przez nazwę pola
- Caller email z headera `x-ms-client-principal` (Azure AD Easy Auth)

### Zmienne środowiskowe (Azure Portal → Configuration)

```
FABRIC_SQL_SERVER=<workspace-id>.<region>.pbidedicated.windows.net
FABRIC_DATABASE=lh_mdm
```

---

## 8. Fabric Workload — natywna integracja

Apka integruje się z Fabric przez **Extensibility Toolkit** — pojawia się w workspace obok Lakehouse i Notebooków.

### Struktura manifestu

```
fabric/workload/
├── BE/
│   ├── WorkloadManifest.xml    # WorkloadName, HostingType=FERemote, AADFEApp, URL
│   └── MDMStewardship.xml      # ItemType + RequiredScopes (PowerBI API)
└── FE/
    ├── Product.json            # Nazwa, opis, ikony dla Fabric portal
    ├── MDMStewardship.json     # createExperience, editorExperience
    └── assets/
        ├── mdm-icon-32.png     # ← DODAJ (32×32 px PNG)
        └── mdm-icon-44.png     # ← DODAJ (44×44 px PNG)
```

### Jak działa auth w iFrame

```
Fabric host
  1. Pobiera token z Azure AD (scopes z MDMStewardship.xml)
  2. window.postMessage({ type: 'FABRIC_AUTH_TOKEN', token, expiresAt })
React app (src/lib/fabricHost.ts)
  3. Odbiera token przez FabricHostBridge
  4. getAccessToken() w mdmApi.ts zwraca ten token (priorytet nad MSAL)
  5. Użytkownik nie widzi żadnego ekranu logowania
```

### Wdrożenie lokalne (DEV)

```bash
# 1. Uzupełnij WorkloadManifest.xml: __VITE_CLIENT_ID__ + __FRONTEND_URL__=https://localhost:3000

# 2. Uruchom UI
cd stewardship-ui && npm run dev

# 3. Uruchom DevGateway (rejestruje workload w Fabric lokalnie)
npm install -g @ms-fabric/workload-devgateway
workload-devgateway start --manifest fabric/workload/BE/WorkloadManifest.xml

# 4. W Fabric Portal: Settings → Admin → Developer features → włącz
# 5. W workspace: + New item → MDM Stewardship
```

### Wdrożenie produkcyjne

```powershell
# 1. Zmień __FRONTEND_URL__ na URL Azure Static Web Apps
# 2. Spakuj manifest
Compress-Archive -Path fabric/workload/* -DestinationPath MDMStewardship.1.0.0.nupkg

# 3. Fabric Admin Portal → Workloads → Upload workload → wgraj .nupkg
# 4. Aktywuj workload dla tenanta
```

---

## 9. Pierwsze uruchomienie

### Krok 1 — Fabric Lakehouse

W Fabric: utwórz Lakehouse o nazwie `lh_mdm`, a następnie w Notebook (SQL cell) uruchom DDL w kolejności:

```sql
-- Kolejność ma znaczenie (zależności między tabelami)
-- 1.
<zawartość fabric/lakehouse/ddl/mdm_config/create_mdm_config_tables.sql>
-- 2.
<zawartość fabric/lakehouse/ddl/bronze/create_bronze_tables.sql>
-- 3.
<zawartość fabric/lakehouse/ddl/silver_dv/create_silver_dv_tables.sql>
-- 4.
<zawartość fabric/lakehouse/ddl/gold/create_gold_tables.sql>
-- 5.
<zawartość fabric/lakehouse/seed/seed_mdm_config_location.sql>
```

### Krok 2 — Fabric Environment (dependency `jellyfish`)

Notebook `nb_match_location.py` wymaga biblioteki `jellyfish` (Jaro-Winkler). W Fabric:

1. Workspace → **Environments** → New environment (np. `mdm-env`).
2. Public libraries → Add from PyPI → `jellyfish==1.0.3` (patrz [fabric/notebooks/requirements.txt](fabric/notebooks/requirements.txt)).
3. Publish środowiska.
4. W każdym notebooku MDM (lub globalnie w workspace) ustaw **Environment = mdm-env**.

Alternatywa ad-hoc — w samej komórce notebooka:
```python
%pip install jellyfish==1.0.3
```

**Uwaga:** od fazy 1 notebook rzuca `ImportError` (fail-fast) zamiast cicho degradować do Levenshteina.

### Krok 3 — Pierwsze ładowanie danych

```
Fabric Data Pipeline: PL_MDM_Master_Location
Parametry: __paramFullLoad = true
```

### Krok 4 — UI lokalnie (mock mode)

```bash
cd stewardship-ui
npm install
# .env z VITE_MOCK_MODE=true już istnieje
npm run dev
# → http://localhost:3000
```

### Krok 5 — UI produkcyjnie

```bash
# 1. Azure Portal: utwórz Static Web App
#    - repo: ten repo, branch: main, folder: stewardship-ui
#    - skopiuj API token do GitHub Secrets: AZURE_STATIC_WEB_APPS_API_TOKEN

# 2. GitHub Secrets (Settings → Secrets):
VITE_WRITE_API_URL=https://...
VITE_TENANT_ID=<guid>
VITE_CLIENT_ID=<guid>

# 3. Push do main → GitHub Actions deploy automatycznie
```

---

## 10. Wdrożenie produkcyjne

### Checklist

- [ ] Fabric Lakehouse `lh_mdm` — DDL + seed
- [ ] Fabric **Environment** z `jellyfish==1.0.3` podłączony do notebooków MDM
- [ ] Azure AD App Registration — redirect URI na prod URL + `localhost:3000`
- [ ] Azure AD App Registration — **Expose an API**: ustaw `Application ID URI` (`api://<clientId>`) + dodaj scope `access_as_user`
- [ ] Azure Function App — Managed Identity włączona + **AZURE_TENANT_ID** + **EXPECTED_AUDIENCE** (`api://<clientId>`) w App Settings (wymagane dla JWT verify)
- [ ] MSI rola **Contributor** w Fabric Workspace
- [ ] Azure Function code deployed (`deploy-function.yml` lub ręcznie `az functionapp deployment source config-zip`)
- [ ] Fabric Workload manifest — uzupełnione placeholdery (`__VITE_CLIENT_ID__`, `__FRONTEND_URL__`) + ikony PNG (`fabric/workload/FE/assets/mdm-icon-{32,44}.png`)
- [ ] VNet Data Gateway lub Managed Private Endpoint — łączność do systemów źródłowych

### GitHub Secrets — pełna lista

| Secret | Workflow | Gdzie wziąć |
|---|---|---|
| `AZURE_STATIC_WEB_APPS_API_TOKEN` | `deploy-ui.yml` | Azure Portal → Static Web App → Manage deployment token |
| `AZURE_FUNCTIONAPP_PUBLISH_PROFILE` | `deploy-function.yml` | `az functionapp deployment list-publishing-profiles -g rg-fabric-poc-sdc -n func-mdm-stewardship --xml` |
| `VITE_WRITE_API_URL` | `deploy-ui.yml` | URL Function App, np. `https://func-mdm-stewardship.azurewebsites.net` |
| `VITE_TENANT_ID` | `deploy-ui.yml` | Azure AD tenant GUID |
| `VITE_CLIENT_ID` | `deploy-ui.yml` | App Registration client ID |
| `VITE_API_SCOPE` (opcjonalny) | `deploy-ui.yml` | Pełny scope API, np. `api://<clientId>/access_as_user`. Pomiń, jeśli identyczny z domyślnym `api://${VITE_CLIENT_ID}/access_as_user` |

**Function App Settings** (`az functionapp config appsettings set`):

| Setting | Wartość |
|---|---|
| `AZURE_TENANT_ID` | Azure AD tenant GUID |
| `EXPECTED_AUDIENCE` | `api://<clientId>` — musi być zgodne z `Application ID URI` w App Registration; bez tego Function zwraca 500 (fail-closed) |

### Uwaga: Łączność Fabric → Źródła danych

Istniejący ADF używa Self-Hosted Integration Runtime. Fabric wymaga jednego z:
- **VNet Data Gateway** (zalecane — zarządzany przez Microsoft)
- **Managed Private Endpoint** (w ramach Fabric Capacity)

---

## 10b. Onboarding nowego klienta (replikowalna instalacja)

Cały deployment jest sterowany przez **jeden plik**: [deploy/mdm.config.json](deploy/mdm.config.json) (gitignored). Skopiuj `deploy/mdm.config.example.json`, wypełnij wartości i uruchom `pwsh deploy/install.ps1`.

### Krok po kroku

```powershell
# 1. Skopiuj template
Copy-Item deploy/mdm.config.example.json deploy/mdm.config.json

# 2. Wypełnij wartości w deploy/mdm.config.json (subscription, RG, tenant, clientId, workspace itp.)

# 3. Walidacja przed deployem
pwsh deploy/install.ps1 -WhatIf

# 4. Pełny deploy
pwsh deploy/install.ps1

# (opcjonalnie) tylko Fabric, jeżeli Azure resources już istnieją:
pwsh deploy/install.ps1 -SkipAzure

# (opcjonalnie) tylko Function App settings:
pwsh deploy/install.ps1 -SkipAzure -SkipFabric
```

### Skąd brać wartości do `mdm.config.json`

| Pole | Skąd |
|---|---|
| `azure.subscriptionId` | `az account show --query id -o tsv` |
| `azure.resourceGroup` | Nazwa istniejącego RG (lub nowego do utworzenia) |
| `azure.location` | Region — np. `swedencentral`, `westeurope` |
| `azure.functionAppName` | Globalnie unikalna nazwa (max 60 znaków, [a-z0-9-]) |
| `azure.staticWebAppName` | Nazwa SWA |
| `fabric.workspaceId` | URL workspace'a w Fabric Portal: `/groups/<guid>` |
| `fabric.lakehouseName` | Np. `lh_mdm` (jeśli nie istnieje, `install.ps1` go utworzy) |
| `fabric.warehouseName` | Np. `wh_mdm` (j.w.) — albo nazwa **istniejącego warehouse klienta** jeśli wpinasz się w gotowe |
| `fabric.environmentName` | Fabric Environment z `jellyfish==1.0.3` (utwórz ręcznie w Fabric UI) |
| `mdm.schemaPrefix` | `""` dla nowego workspace; `"mdm_"` jeżeli warehouse klienta ma już własne `silver_dv`/`gold` |
| `auth.tenantId` | `az account show --query tenantId -o tsv` |
| `auth.clientId` | App Registration → Overview → Application (client) ID |
| `auth.apiScope` | `"auto"` = `api://<clientId>/access_as_user` (default); zostaw `auto` chyba że masz custom scope |

### Co `install.ps1` robi automatycznie

1. Waliduje `mdm.config.json` przeciw `mdm.config.schema.json` (JSON Schema)
2. Wypisuje plan + prosi o potwierdzenie (`y`)
3. Provisioning Azure: App Reg + Storage + Function + SWA + CORS (przez `create-azure-resources.ps1`)
4. Provisioning Fabric: Lakehouse + Warehouse + DDL + seed `mdm_config` (przez `deploy-fabric.ps1` z `--schemas` JSON)
5. Function App settings: `AZURE_TENANT_ID`, `EXPECTED_AUDIENCE`, `MDM_SCHEMA_BRONZE/SILVER/GOLD/CONFIG`
6. Generuje `stewardship-ui/.env.production.local` (gitignored)
7. Zapisuje `deploy/.deploy-output.json` z wartościami do GitHub Secrets (gitignored)
8. Smoke test: `GET /api/health`

### Schema prefix — co działa, co nie

W obecnej wersji `schemaPrefix` jest **w pełni** przepuszczony do:

✅ **Azure Function** — przez env vars `MDM_SCHEMA_*` (lib `azure-function/src/lib/schemas.ts`)
✅ **Warehouse DDL** — przez `apply-warehouse-ddl.js --schemas` (placeholdery `{{SCHEMA_*}}` w `fabric/warehouse/ddl/**` i `fabric/warehouse/seed/**`)

⚠️ **Nie jest jeszcze przepuszczony** do:
- Lakehouse DDL w notebooku `nb_bootstrap_ddl.py` (hardcoded `CREATE SCHEMA bronze` itd.)
- Pozostałe notebooki (bronze append, raw vault load, derive gold, match)
- Pipelines Fabric (`PL_MDM_*.json`)

**Dlatego:**
- Jeżeli wpinasz się tylko w **istniejący Warehouse klienta** (typowy use-case "easy plug-in"), schema prefix działa poprawnie — Function + Warehouse są spójne.
- Jeżeli wdrażasz **pełny stack z Lakehouse**, użyj `schemaPrefix: ""` (default). Notebooki utworzą `bronze`/`silver_dv`/`gold` w lakehouse.
- Jeżeli klient ma już `silver_dv`/`gold` w lakehouse i chcesz uniknąć kolizji — zaktualizuj notebooki ręcznie (zmień nazwy schematów na linii `CREATE SCHEMA IF NOT EXISTS X`) lub deployuj lakehouse w osobnym workspace.

### Pliki gitignored

`mdm.config.json`, `.deploy-output.json`, `stewardship-ui/.env.production.local`, `azure-function/local.settings.json`, `deploy/.env.deploy` — żaden z tych plików nie powinien trafić do git.

---

## 11. Rozszerzanie systemu

### Dodanie nowej encji (np. `item`, `employee`)

```
1. DDL
   fabric/lakehouse/ddl/bronze/create_bronze_tables.sql     → dodaj bronze_item_*
   fabric/lakehouse/ddl/silver_dv/create_silver_dv_tables.sql → hub_item + sat_item_*
   fabric/lakehouse/ddl/gold/create_gold_tables.sql          → dim_item

2. Config
   fabric/lakehouse/seed/seed_mdm_config_item.sql
   → INSERT entity_config (entity_id='item', ...)
   → INSERT source_priority per pole
   → INSERT field_config (match_weight, blocking_key)

3. Notebooki
   Skopiuj nb_*_location.py → nb_*_item.py
   Zamień nazwy tabel (hub_location → hub_item, sat_location_* → sat_item_*)

4. Pipeline
   PL_MDM_Master_Location.json → PL_MDM_Master_Item.json
   Zmień __paramEntityId = "item"

5. UI
   mdm.types.ts → dodaj ItemAttributes, GoldenItem
   Notebooki dostarczą dane przez ten sam SQL Endpoint
   ReviewQueue/GoldenViewer działają bez zmian (używają config-driven queries)
```

### Dodanie nowego źródła do istniejącej encji

```
1. Dodaj tabelę bronze_location_<source>.sql
2. Dodaj sat_location_<source> do silver_dv DDL
3. Dodaj Cell w nb_load_raw_vault_location.py (wzorzec: Cell 7 dla Yext)
4. INSERT do mdm_config.source_priority (priorytet dla każdego pola)
5. INSERT do mdm_config.hash_config (business_key_template)
```

---

## 12. Wzorce i decyzje architektoniczne

### Dlaczego Data Vault Lite (nie pełny DV ani płaskie tabele)?

| Podejście | Pro | Con |
|-----------|-----|-----|
| Płaskie tabele | Proste | Brak historii; scalanie niszczy linię danych |
| Pełny Data Vault | Kompletny | Links, Bridge tables — zbędne dla 1 encji |
| **DV Lite (Hub + Sat)** | Historia + prostota | Brak Links (dodamy gdy potrzeba) |

### Dlaczego SHA256 jako Hub key?

- Deterministyczny: ten sam `"lightspeed|41839"` zawsze daje ten sam hash
- Umożliwia MERGE bez sekwencji (Fabric Delta nie ma auto-increment)
- Pozwala na `bv_location_key_resolution` bez FK — hash jest samowystarczalnym ID

### Dlaczego Azure Function jako read/write proxy?

Jedno API upraszcza auth i CORS (Fabric iFrame + standalone), a MSI daje bezhasłowy dostęp do SQL Endpoint dla odczytu i zapisu.

### Wzorce z losadf/ zachowane w MDM

| Wzorzec | Źródło w losadf/ | Implementacja w MDM |
|---------|-----------------|---------------------|
| `__param` naming | Wszystkie ADF pipelines | `__paramEntityId`, `__paramFullLoad` |
| Logging framework | `PL_Master_Execution.json` | `mdm_config.execution_log` |
| Error cascade | `PL_Data_ErrorReporting.json` | Error Handler w pipeline |
| Watermark / incremental | `tblExtractionLog` | `mdm_config.source_watermark` |
| Config-driven execution | `tblDwhMigrationTable` | `mdm_config.entity_config` |

