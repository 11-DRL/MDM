# Fabric Workload Manifest — MDM Stewardship

## Struktura paczki

```
fabric/workload/
├── BE/
│   ├── WorkloadManifest.xml   ← tożsamość workloadu + URL frontendu
│   └── MDMStewardship.xml     ← definicja Item Type + wymagane scopes
└── FE/
    ├── Product.json           ← metadane w Fabric portal (nazwa, opis, ikony)
    ├── MDMStewardship.json    ← front-end config dla Item Type
    └── assets/
        ├── mdm-icon-32.png    ← ikona 32×32 px (DODAJ!)
        └── mdm-icon-44.png    ← ikona 44×44 px (DODAJ!)
```

## Ikony

Wygeneruj dwa pliki PNG (np. logo L'Osteria lub pin MDM):
- `assets/mdm-icon-32.png` — 32×32 px
- `assets/mdm-icon-44.png` — 44×44 px

## Konfiguracja

W `WorkloadManifest.xml` zastąp placeholder'y:

| Placeholder         | Wartość                                         |
|---------------------|-------------------------------------------------|
| `__VITE_CLIENT_ID__` | Client ID z Azure AD App Registration          |
| `__FRONTEND_URL__`   | `https://localhost:3000` (dev) lub prod URL    |

## Wdrożenie DEV (lokalnie w Fabric)

1. Zainstaluj DevGateway:
   ```bash
   npm install -g @ms-fabric/workload-devgateway
   ```

2. Uruchom apkę:
   ```bash
   cd stewardship-ui && npm run dev
   ```

3. Uruchom DevGateway wskazując na manifest:
   ```bash
   workload-devgateway start --manifest fabric/workload/BE/WorkloadManifest.xml
   ```

4. W Fabric Portal → włącz **Developer Mode** (Settings → Admin → Developer features)

5. W workspace pojawi się opcja **+ MDM Stewardship** — kliknij aby otworzyć apkę

## Wdrożenie PROD

1. Zamień `__FRONTEND_URL__` na URL Azure Static Web Apps
2. Spakuj `BE/` i `FE/` do pliku `.nupkg`:
   ```bash
   # Plik .nupkg to zwykłe ZIP z rozszerzeniem .nupkg
   Compress-Archive -Path fabric/workload/* -DestinationPath MDMStewardship.1.0.0.nupkg
   ```
3. Fabric Admin Portal → **Workloads** → **Upload workload** → wgraj `.nupkg`
4. Aktywuj workload dla swojego tenanta

## Jak działa auth w iFrame

Fabric host automatycznie:
1. Pobiera token z Azure AD (scopes z `MDMStewardship.xml`)
2. Wstrzykuje token do iFrame przez Host SDK (`window.postMessage`)
3. Nasza apka odbiera token przez `FabricHostBridge` (patrz `stewardship-ui/src/lib/fabricHost.ts`)
4. Token zastępuje MSAL — nie trzeba logowania w Fabric context
