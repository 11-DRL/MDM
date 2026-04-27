# MDM Stewardship App — Status & Backlog

> **Stan na:** 2026-04-25
> **Wersja:** Faza 1–5 zakończone. **F0 (generalizacja) + F1 (legal_entity)** — implementacja w toku.
> **Plik źródłowy planu:** ten dokument jest jedynym źródłem prawdy o tym co zostało do zrobienia. Przy zamykaniu zadań — zaznacz `[x]` i dopisz commit/PR.

---

## 📊 Status fazowy

| Faza | Zakres | Status |
|------|--------|--------|
| **F1** — Blockery produkcyjne | Deploy Function z optimistic concurrency, 409 w UI, jellyfish fail-fast, ikony workloadu, `deploy-function.yml` | ✅ Done (commit `9e3de71`) |
| **F2** — Concurrency end-to-end | ETag/`expectedOldValue` w `overrideField` (412), atomowa transakcja w `reviewPair`, polling kolejki 20s | ✅ Done (commit `9e3de71`) |
| **F3** — Higiena, testy, security | JWT JWKS verify (jose), 25 testów Function + 11 testów UI, `ci.yml`, README updates | ✅ Done (commit `d0ccec9`) |
| **F4** — MUST: hardening (transakcje, 404, audience, gitignore) | createLocation w transakcji, NotFound page, `deploy/build-*` w gitignore, JWT audience validation | ✅ Done (Sprint 1) |
| **F5** — Replikowalna instalacja | `mdm.config.json` + JSON Schema + `install.ps1` + schemas.ts + DDL templating; Function + Warehouse w pełni prefix-aware | ✅ Done (Sprint 2 partial) |
| **F0** — Generalizacja (multi-entity) | mdm_config rozszerzenie, entity_id, generic notebooks, v2 API, EntitySelector/EntityForm w UI | ✅ Implemented |
| **F1-new** — Legal Entity (pilot) | DDL bronze/silver/gold, seed, pipeline, warehouse hierarchy view | ✅ Implemented |
| **F4-rest** — SHOULD/COULD | reszta z poniższego backlogu | 🟡 Backlog |

---

## 🚦 Backlog — uporządkowane

### 🔴 MUST (zrób przed prawdziwym GA)

- [x] **`createLocation` w transakcji** — [azure-function/src/functions/mdmWrite.ts](azure-function/src/functions/mdmWrite.ts) ~L1200–L1400
  - 5 osobnych `execSql` (hub_location → sat_location_manual → dim_location → dim_location_quality → stewardship_log)
  - Awaria w środku zostawiała osierocone rekordy w Hub/Sat
  - **Done**: jeden batch z `SET XACT_ABORT ON; BEGIN TRAN … COMMIT TRAN`, wszystkie 5 INSERTów atomowo (rollback przy dowolnym błędzie)

- [x] **404 page w UI** — [stewardship-ui/src/App.tsx](stewardship-ui/src/App.tsx)
  - Wcześniej `<Route path="*" element={<Navigate to="/queue" replace />}/>` — silently zjadało literówki
  - **Done**: nowy komponent `NotFound` ([stewardship-ui/src/components/NotFound/NotFound.tsx](stewardship-ui/src/components/NotFound/NotFound.tsx)) z `pathname` z `useLocation()` + linkiem powrotu; podłączony we wszystkich 3 trybach (Mock/Fabric/Standalone) + 3 testy jednostkowe

- [x] **`deploy/build-cf6ad95e/` cleanup** — artefakt builda w gicie
  - **Done**: dopisano `deploy/build-*/` do [.gitignore](.gitignore). `git rm --cached` zwrócił `pathspec did not match` — folder nie był trackowany, ale rule chroni przed przyszłymi commitami z `package-workload.ps1`

- [x] **JWT audience validation** — [azure-function/src/functions/mdmWrite.ts](azure-function/src/functions/mdmWrite.ts) `validateBearerToken`
  - Wcześniej tylko issuer + tenant + signature; `aud` claim nie sprawdzany
  - **Done**: `EXPECTED_AUDIENCE` env (fail-closed gdy brak), przekazywane do `jwtVerify` jako `audience`. UI: nowy `API_SCOPE` (default `api://<clientId>/access_as_user`, override przez `VITE_API_SCOPE`) zamiast `User.Read` w MSAL `acquireTokenSilent` i `loginRedirect`. Nowe testy: missing audience → 500, mismatched audience → 401. README sekcja 10 zaktualizowana o App Registration `Expose an API` + Function App settings.

### 🟠 SHOULD (jakość prod, niewymagające pełnej transformacji)

- [ ] **`overrideField` w transakcji** — symetria z `reviewPair`
  - Obecnie SELECT (precondition) + INSERT do log osobno
  - Fix: pre-check + INSERT log w jednym batchu (uwaga: właściwy UPDATE pola jest już atomowy bo to single-statement)

- [ ] **Health endpoint sprawdza DB** — [azure-function/src/functions/mdmWrite.ts](azure-function/src/functions/mdmWrite.ts) `app.http('health'…)`
  - Teraz hardcoded `{ status: 'ok' }` — nie wykrywa odcięcia od Fabric SQL ani wygasłego MSI tokena
  - Fix: lekki `SELECT 1` z timeoutem 5s; przy błędzie → 503 z błędem (bez detali)

- [ ] **GDPR / retention dla `stewardship_log`** — `silver_dv.stewardship_log`
  - Tabela append-only zawiera `changed_by` (e-mail) → PII bez polityki retencji
  - Fix: udokumentować retencję (np. 365 dni dla regulatorów, indefinite dla audytu wewnętrznego), opcjonalnie dodać scheduler/cleanup notebook
  - Dodać sekcję "Privacy / data retention" do README

- [ ] **Lint + typecheck w CI** — [.github/workflows/ci.yml](.github/workflows/ci.yml)
  - Brak `npm run lint` i `tsc --noEmit` jako gate
  - Fix: dodać kroki przed testami; eslint dla UI już skonfigurowany w package.json

- [ ] **`npm audit` w CI** — security scan
  - Fix: krok `npm audit --audit-level=high` w job `function` i `ui`; failure → blok merge

- [ ] **CORS Function App** — sprawdzić w Azure Portal
  - Brak konfiguracji w repo; CORS musi być whitelistą tylko prod SWA + `localhost:3000`
  - Fix: `az functionapp cors add -g rg-fabric-poc-sdc -n func-mdm-stewardship --allowed-origins https://ashy-bush-00879c703.7.azurestaticapps.net http://localhost:3000`

- [ ] **Component testy UI** — vitest + @testing-library/react już zainstalowane
  - 0 testów komponentów; minimum: `ReviewQueue` (renderuje stats), `PairDetail` (handler 409 → toast), `GoldenViewer` (handler 412 + expectedOldValue payload), `NewLocationForm` (walidacja kroków)
  - Fix: 4 pliki `*.test.tsx`, ~30 testów

- [ ] **Hook testy** — `useMdm` mutacje (mock axios + sprawdzenie toasts)
  - Wymaga MSW lub `vi.mock('../api/mdmApi')`

- [ ] **Code-splitting routes** — [stewardship-ui/src/App.tsx](stewardship-ui/src/App.tsx)
  - Bundle JS = 633 kB (vite ostrzega). Wszystkie 6 ekranów ładuje się eagerly
  - Fix: `const ReviewQueue = React.lazy(() => import('...'))` + `<Suspense fallback={<Spinner/>}>`
  - Powinno spaść ~50% initial JS

### 🟢 COULD (nice-to-have, bez konsekwencji)

- [ ] **Application Insights — weryfikacja** — [azure-function/host.json](azure-function/host.json) ma sampling skonfigurowany, ale nie potwierdziliśmy że resource w Azure istnieje i logi tam trafiają
- [ ] **Custom metrics**: `mdm.review.conflict` (count 409), `mdm.review.latency` (p95) — dashboard w Portal
- [ ] **Soft lock / presence** — tylko jeśli >3 stewardów jednocześnie (pomijamy w POC)
- [ ] **Rate limiting** — średnio Function App ma platform throttling; własny per-user middleware tylko jeśli pojawi się abuse
- [ ] **E2E testy** (Playwright) — pełen flow: extract → match → review → derive → check golden
- [ ] **ADR** (Architecture Decision Records) — dlaczego Data Vault Lite, dlaczego Function proxy, dlaczego Jaro-Winkler+blocking
- [ ] **DDL drift Lakehouse vs Warehouse** — jedno źródło prawdy (yaml→generator) lub udokumentowana ręczna sync; obecnie [fabric/lakehouse/ddl/](fabric/lakehouse/ddl/) i [fabric/warehouse/ddl/](fabric/warehouse/ddl/) ewoluują niezależnie
- [ ] **`pages/` vs `components/`** — folder `stewardship-ui/src/pages/` jest pusty; przenieś route-level komponenty (kosmetyka)
- [ ] **Disaster recovery runbook** — odtworzenie `stewardship_log`, snapshoty Fabric workspace SLA
- [ ] **`__TODO_FILL_AFTER_FABRIC_SETUP__`** w [deploy/create-azure-resources.ps1#L219](deploy/create-azure-resources.ps1#L219) — dodać prompt zamiast placeholdera

---

## ✅ Co JUŻ jest gotowe (referencja)

### Multi-Entity Generalization (F0)
- **mdm_config rozszerzenie**: `entity_config` + `field_config` dodatkowe kolumny (ui_widget, validators_json, is_overridable, is_golden_field, group_name), `entity_relationship`, `coa_mapping_rule` ([alter_mdm_config_multi_entity.sql](fabric/lakehouse/ddl/mdm_config/alter_mdm_config_multi_entity.sql))
- **entity_id migracja**: Silver DV tabele (hub_location, bv_*, pit_location, stewardship_log) rozszerzone o `entity_id` z backfill 'business_location' ([alter_silver_dv_add_entity_id.sql](fabric/lakehouse/ddl/silver_dv/alter_silver_dv_add_entity_id.sql))
- **Generic notebooks**: `nb_load_raw_vault.py`, `nb_match.py`, `nb_derive_gold.py` — config-driven, parametryzowane `entity_id` ([fabric/notebooks/](fabric/notebooks/))
- **V2 API** (8 endpoints): `GET/POST /api/v2/entities/{entityId}/...` — schema, golden records, queue stats, override, create ([v2Routes.ts](azure-function/src/functions/v2Routes.ts))
- **Entity metadata service**: 5-min cache, loads entity_config + field_config + source_priority ([entityMeta.ts](azure-function/src/lib/entityMeta.ts))
- **UI multi-entity**: EntityProvider/Selector dropdown, generic EntityForm, v2Api client, v2 types ([stewardship-ui/src/](stewardship-ui/src/))

### Legal Entity (F1-new — pilot entity)
- **DDL**: bronze (`legal_entity_manual`), silver_dv (`hub_legal_entity`, `sat_legal_entity_manual`), gold (`dim_legal_entity` SCD2, `dim_legal_entity_quality`)
- **Warehouse**: `vw_legal_entity_hierarchy` — recursive CTE ownership tree ([create_vw_legal_entity_hierarchy.sql](fabric/warehouse/ddl/create_vw_legal_entity_hierarchy.sql))
- **Seed**: entity_config (match_engine='none', icon='Building2'), 11 field_config entries, source_priority, hash_config, entity_relationship ([seed_mdm_config_legal_entity.sql](fabric/lakehouse/seed/seed_mdm_config_legal_entity.sql))
- **Pipeline**: `PL_MDM_Master_LegalEntity` — nb_load_raw_vault → nb_derive_gold (no matching) ([PL_MDM_Master_LegalEntity.json](fabric/pipelines/PL_MDM_Master_LegalEntity.json))

### Backend (Azure Function)
- 13 endpointów: 8 read + 4 write + health ([mdmWrite.ts](azure-function/src/functions/mdmWrite.ts))
- **Optimistic concurrency**:
  - `reviewPair`: pre-check + guard `WHERE status='pending'` + rowCount + RAISERROR `CONCURRENCY_CONFLICT` w transakcji → 409
  - `overrideField`: porównanie `expectedOldValue` z aktualną wartością → 412
- **Transakcja w `reviewPair`** (UPDATE + INSERT resolution + INSERT log atomowo, `XACT_ABORT ON`)
- **JWT verification**: jose `createRemoteJWKSet`, v1+v2 issuer, exp/clock tolerance, fail-closed na brak `AZURE_TENANT_ID`
- **Helpery exportowane** dla testów: `sanitizeHex32`, `parsePositiveInt`, `sanitizeEntityId`, `toMatchSource`, `parseStatus`
- Managed Identity → Fabric SQL (tedious + token cache 5min refresh)
- Field whitelist w `overrideField` (10 dozwolonych pól)

### UI (Stewardship)
- 6 komponentów: ReviewQueue, PairDetail, GoldenList, GoldenViewer (z editem inline), ConfigViewer (read-only), NewLocationForm (4-step wizard)
- 3-tryb auth: Mock / Fabric iFrame / MSAL standalone
- **Typed errors**: `ApiConflictError` (409), `ApiPreconditionError` (412) z metadanymi (`reviewedBy`, `reviewedAt`, `currentValue`)
- **Toast system** (sonner) z `onError` w mutacjach + invalidate query
- **Polling**: queue stats co 30s, candidates co 20s (oba `refetchIntervalInBackground: false`)
- `expectedOldValue` wysyłane z GoldenViewer

### Fabric
- 4 schematy Lakehouse: `mdm_config`, `bronze`, `silver_dv`, `gold`
- 7 notebooków (bootstrap, 4× bronze, raw vault, match z **fail-fast jellyfish**, derive gold)
- 6 pipelines (master + 4 extract + error reporting)
- Ikony workloadu: `mdm-icon-32.png`, `mdm-icon-44.png`

### Tests + CI
- **Function**: 25 testów (helpers + auth scenariusze incl. **forged JWT signature rejection**)
- **UI**: 11 testów (ApiConflictError, ApiPreconditionError, cn utility)
- **Workflows**: `ci.yml` (PR + push), `deploy-ui.yml`, `deploy-function.yml`, `deploy-fabric.yml`

### Docs
- README sekcje 1–12 (architektura → rozszerzanie)
- Sekcja 9: Fabric Environment + jellyfish step-by-step
- Sekcja 10: pełna checklist deploy + tabela GitHub Secrets (5 pozycji)
- [docs/komunikacja-ui-fabric.html](docs/komunikacja-ui-fabric.html): protokół iFrame postMessage

---

## 🎯 Rekomendacja kolejności (jeśli wracasz do projektu)

1. **Sprint 1 (MUST):** wszystkie 4 pozycje MUST — to są ostatnie blockery prod-grade
2. **Sprint 2 (SHOULD-Quality):** lint+audit w CI, health DB check, CORS, component testy, code-split → bezpieczna baza dla skalowania
3. **Sprint 3 (SHOULD-Compliance):** GDPR retention, `overrideField` transakcja, hooks tests
4. **Backlog (COULD):** zostaw na okazję; nic z tego nie blokuje produkcji

---

## 📌 Decyzje świadomie pominięte

- **Pełny RBAC** (Steward/Admin role split) — POC ma jedną rolę
- **Soft lock / presence** — overkill dla <3 stewardów
- **Migracja tedious → mssql/node-mssql** — tedious wystarcza, brak ROI
- **Single source of truth dla DDL Lakehouse↔Warehouse** — ręczna synchronizacja wystarcza dla obecnej skali
- **Przepisywanie Data Vault** — działa, brak powodu

---

## 🔗 Linki ważnych plików

- Backend: [mdmWrite.ts](azure-function/src/functions/mdmWrite.ts) · [tests/](azure-function/tests/)
- UI: [App.tsx](stewardship-ui/src/App.tsx) · [mdmApi.ts](stewardship-ui/src/api/mdmApi.ts) · [useMdm.ts](stewardship-ui/src/hooks/useMdm.ts)
- Fabric: [nb_match_location.py](fabric/notebooks/nb_match_location.py) · [DDL](fabric/lakehouse/ddl/)
- CI/CD: [.github/workflows/](.github/workflows/)
- Docs: [README](README.md) · [docs/](docs/)
