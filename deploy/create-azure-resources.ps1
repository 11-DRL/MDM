<#
.SYNOPSIS
    Tworzy wszystkie zasoby Azure potrzebne do deploymentu MDM Stewardship App.

.DESCRIPTION
    Skrypt jednorazowy — uruchom raz, na końcu wypisze wszystkie wartości
    potrzebne do uzupełnienia .env i WorkloadManifest.xml.

.NOTES
    Wymagania:
      - az CLI zainstalowane: https://aka.ms/installazurecliwindows
      - Zalogowany: az login
      - Uprawnienia: Contributor na subscription + Application Administrator w Azure AD

.EXAMPLE
    .\deploy\create-azure-resources.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ─── KONFIGURACJA ────────────────────────────────────────────────────────────
$SUBSCRIPTION_ID = "077ce12c-c878-44cc-818d-f8f6723d1665"
$TENANT_ID       = "5d842dfd-009e-4f2f-bb85-78670fa303bb"
$RESOURCE_GROUP  = "rg-fabric-poc-sdc"
$LOCATION        = "swedencentral"         # Function App + Storage
$SWA_LOCATION    = "westeurope"            # Static Web Apps (ograniczone regiony)
$APP_NAME        = "MDM Stewardship"       # Nazwa App Registration

# Nazwy zasobów (zmień jeśli chcesz)
$STORAGE_NAME    = "stmdmpoc077ce12c"      # max 24 znaków, tylko [a-z0-9]
$FUNC_NAME       = "func-mdm-stewardship"
$SWA_NAME        = "swa-mdm-stewardship"

# ─── HELPER ──────────────────────────────────────────────────────────────────
function Step([string]$msg) {
    Write-Host "`n▶ $msg" -ForegroundColor Cyan
}
function OK([string]$msg) {
    Write-Host "  ✓ $msg" -ForegroundColor Green
}
function Info([string]$key, [string]$val) {
    Write-Host "  $key" -ForegroundColor Gray -NoNewline
    Write-Host " $val" -ForegroundColor Yellow
}

# ─── 0. Ustawienie subskrypcji ────────────────────────────────────────────────
Step "Ustawianie subskrypcji..."
az account set --subscription $SUBSCRIPTION_ID
OK "Subskrypcja: $SUBSCRIPTION_ID"

# ─── 1. App Registration (Azure AD) ──────────────────────────────────────────
Step "Tworzenie App Registration '$APP_NAME'..."

# Sprawdź czy już istnieje
$existingApp = az ad app list --display-name $APP_NAME --query "[0].appId" -o tsv 2>$null
if ($existingApp) {
    $CLIENT_ID = $existingApp.Trim()
    OK "App Registration już istnieje: $CLIENT_ID"
} else {
    $appJson = az ad app create `
        --display-name $APP_NAME `
        --sign-in-audience "AzureADMyOrg" `
        --web-redirect-uris "http://localhost:3000" `
        --enable-access-token-issuance true `
        --enable-id-token-issuance true | ConvertFrom-Json
    $CLIENT_ID = $appJson.appId
    OK "Utworzono App Registration: $CLIENT_ID"

    # Service Principal (wymagany do logowania)
    az ad sp create --id $CLIENT_ID | Out-Null
    OK "Service Principal utworzony"
}

# ─── 2. Storage Account (wymagany przez Function App) ────────────────────────
Step "Tworzenie Storage Account '$STORAGE_NAME'..."
$storageExists = az storage account show --name $STORAGE_NAME --resource-group $RESOURCE_GROUP 2>$null
if ($storageExists) {
    OK "Storage Account już istnieje"
} else {
    az storage account create `
        --name $STORAGE_NAME `
        --resource-group $RESOURCE_GROUP `
        --location $LOCATION `
        --sku Standard_LRS `
        --kind StorageV2 | Out-Null
    OK "Storage Account '$STORAGE_NAME' utworzony"
}

# ─── 3. Azure Function App ────────────────────────────────────────────────────
Step "Tworzenie Function App '$FUNC_NAME'..."
$funcExists = az functionapp show --name $FUNC_NAME --resource-group $RESOURCE_GROUP 2>$null
if ($funcExists) {
    OK "Function App już istnieje"
} else {
    az functionapp create `
        --name $FUNC_NAME `
        --resource-group $RESOURCE_GROUP `
        --consumption-plan-location $LOCATION `
        --runtime node `
        --runtime-version 20 `
        --functions-version 4 `
        --storage-account $STORAGE_NAME `
        --assign-identity "[system]" | Out-Null
    OK "Function App '$FUNC_NAME' utworzony z Managed Identity"
}

# Pobierz Managed Identity Principal ID (potrzebny do roli w Fabric)
$PRINCIPAL_ID = az functionapp identity show `
    --name $FUNC_NAME `
    --resource-group $RESOURCE_GROUP `
    --query "principalId" -o tsv
OK "Managed Identity Principal ID: $PRINCIPAL_ID"

# Pobierz URL Function App
$FUNC_URL = "https://$(az functionapp show --name $FUNC_NAME --resource-group $RESOURCE_GROUP --query 'defaultHostName' -o tsv)"
OK "Function App URL: $FUNC_URL"

# Skonfiguruj CORS — zezwól na lokalny dev + SWA (dodamy pełny URL po kroku 4)
az functionapp cors add `
    --name $FUNC_NAME `
    --resource-group $RESOURCE_GROUP `
    --allowed-origins "http://localhost:3000" | Out-Null
OK "CORS: localhost:3000 dodany"

# ─── 4. Azure Static Web App ─────────────────────────────────────────────────
Step "Tworzenie Static Web App '$SWA_NAME'..."
$swaExists = az staticwebapp show --name $SWA_NAME --resource-group $RESOURCE_GROUP 2>$null
if ($swaExists) {
    OK "Static Web App już istnieje"
} else {
    az staticwebapp create `
        --name $SWA_NAME `
        --resource-group $RESOURCE_GROUP `
        --location $SWA_LOCATION `
        --sku "Free" | Out-Null
    OK "Static Web App '$SWA_NAME' utworzony"
}

# Pobierz URL SWA
$SWA_URL = "https://$(az staticwebapp show --name $SWA_NAME --resource-group $RESOURCE_GROUP --query 'defaultHostname' -o tsv)"
OK "SWA URL: $SWA_URL"

# Pobierz deployment token (do GitHub Actions)
$DEPLOY_TOKEN = az staticwebapp secrets list `
    --name $SWA_NAME `
    --resource-group $RESOURCE_GROUP `
    --query "properties.apiKey" -o tsv
OK "Deployment token pobrany (nie pokazuję — zapisz poniżej)"

# ─── 5. Dodaj SWA URL do App Registration redirect URIs ──────────────────────
Step "Aktualizacja redirect URIs w App Registration..."
az ad app update `
    --id $CLIENT_ID `
    --web-redirect-uris "http://localhost:3000" $SWA_URL | Out-Null
OK "Redirect URIs: localhost:3000 + $SWA_URL"

# ─── 6. Dodaj SWA URL do CORS w Function App ─────────────────────────────────
Step "Aktualizacja CORS w Function App..."
az functionapp cors add `
    --name $FUNC_NAME `
    --resource-group $RESOURCE_GROUP `
    --allowed-origins $SWA_URL | Out-Null
OK "CORS: $SWA_URL dodany"

# ─── 7. App Settings dla Function App ────────────────────────────────────────
Step "Konfiguracja zmiennych środowiskowych Function App..."
# FABRIC_SQL_SERVER zostanie uzupełniony po podaniu SQL Analytics Endpoint z Fabric
az functionapp config appsettings set `
    --name $FUNC_NAME `
    --resource-group $RESOURCE_GROUP `
    --settings `
        "AZURE_TENANT_ID=$TENANT_ID" `
        "VITE_CLIENT_ID=$CLIENT_ID" `
        "ALLOWED_ORIGINS=$SWA_URL" `
        "FABRIC_SQL_SERVER=__TODO_FILL_AFTER_FABRIC_SETUP__" | Out-Null
OK "App Settings zaktualizowane"

# ─── 8. Zapisz wyniki do pliku .env.deploy ────────────────────────────────────
Step "Zapisywanie wyników..."

$envContent = @"
# ============================================================
# Wyniki create-azure-resources.ps1
# Wygenerowano: $(Get-Date -Format "yyyy-MM-dd HH:mm")
# UWAGA: Ten plik zawiera sekret — NIE commituj do git!
# ============================================================

# Azure AD
VITE_TENANT_ID=$TENANT_ID
VITE_CLIENT_ID=$CLIENT_ID

# Azure Function App
FUNC_APP_NAME=$FUNC_NAME
FUNC_APP_URL=$FUNC_URL
FUNC_MANAGED_IDENTITY_PRINCIPAL_ID=$PRINCIPAL_ID

# Azure Static Web App
SWA_NAME=$SWA_NAME
SWA_URL=$SWA_URL
SWA_DEPLOY_TOKEN=$DEPLOY_TOKEN

# Do uzupełnienia po konfiguracji Fabric:
FABRIC_SQL_SERVER=<workspace>.datawarehouse.fabric.microsoft.com
FABRIC_WORKSPACE_ID=<workspace-id>

# ============================================================
# NASTĘPNE KROKI:
# 1. Skopiuj SWA_DEPLOY_TOKEN jako GitHub Secret:
#    GitHub repo → Settings → Secrets → AZURE_STATIC_WEB_APPS_API_TOKEN
#
# 2. Uzupełnij stewardship-ui/.env:
#    VITE_TENANT_ID, VITE_CLIENT_ID, VITE_API_BASE_URL (= FUNC_APP_URL)
#
# 3. Zaktualizuj WorkloadManifest.xml:
#    Zamień __VITE_CLIENT_ID__ → CLIENT_ID
#    Zamień __FRONTEND_URL__  → SWA_URL
#
# 4. W Fabric Admin Portal:
#    Nadaj Function App Managed Identity (PRINCIPAL_ID) rolę
#    "Contributor" w workspace lh_mdm
#
# 5. Wpisz FABRIC_SQL_SERVER do Function App settings:
#    az functionapp config appsettings set \
#      --name $FUNC_NAME \
#      --resource-group $RESOURCE_GROUP \
#      --settings "FABRIC_SQL_SERVER=<endpoint>"
# ============================================================
"@

$envContent | Out-File -FilePath ".\deploy\.env.deploy" -Encoding UTF8
OK "Wyniki zapisane → deploy\.env.deploy"

# ─── PODSUMOWANIE ─────────────────────────────────────────────────────────────
Write-Host "`n$('='*65)" -ForegroundColor Magenta
Write-Host "  ZASOBY AZURE — GOTOWE" -ForegroundColor Magenta
Write-Host "$('='*65)" -ForegroundColor Magenta
Info "Tenant ID:           " $TENANT_ID
Info "Client ID:           " $CLIENT_ID
Info "Function App URL:    " $FUNC_URL
Info "Static Web App URL:  " $SWA_URL
Info "Managed Identity ID: " $PRINCIPAL_ID
Write-Host ""
Write-Host "  Deploy token zapisany w: deploy\.env.deploy" -ForegroundColor Yellow
Write-Host "  (skopiuj SWA_DEPLOY_TOKEN jako GitHub Secret)" -ForegroundColor Yellow
Write-Host "$('='*65)" -ForegroundColor Magenta
Write-Host ""
Write-Host "  Co zrobić teraz:" -ForegroundColor White
Write-Host "  1. GitHub → Settings → Secrets → dodaj AZURE_STATIC_WEB_APPS_API_TOKEN" -ForegroundColor White
Write-Host "  2. Podaj mi FABRIC_SQL_SERVER (SQL Analytics Endpoint z Fabric)" -ForegroundColor White
Write-Host "  3. Powiem Ci jak zaktualizować pliki konfiguracyjne" -ForegroundColor White
Write-Host "$('='*65)" -ForegroundColor Magenta
