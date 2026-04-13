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
# Używamy Continue — az.exe zwraca błędy jako stderr, nie jako PowerShell exceptions
$ErrorActionPreference = "Continue"

# ─── KONFIGURACJA ────────────────────────────────────────────────────────────
$SUBSCRIPTION_ID = "077ce12c-c878-44cc-818d-f8f6723d1665"
$TENANT_ID       = "5d842dfd-009e-4f2f-bb85-78670fa303bb"
$RESOURCE_GROUP  = "rg-fabric-poc-sdc"
$LOCATION        = "swedencentral"         # Function App + Storage
$SWA_LOCATION    = "westeurope"            # Static Web Apps (ograniczone regiony)
$APP_NAME        = "MDM Stewardship"       # Nazwa App Registration

# Nazwy zasobów
$STORAGE_NAME    = "stmdmpoc077ce12c"      # max 24 znaków, tylko [a-z0-9]
$FUNC_NAME       = "func-mdm-stewardship"
$SWA_NAME        = "swa-mdm-stewardship"

# ─── HELPERS ─────────────────────────────────────────────────────────────────
function Step([string]$msg) { Write-Host "`n▶ $msg" -ForegroundColor Cyan }
function OK([string]$msg)   { Write-Host "  ✓ $msg" -ForegroundColor Green }
function Fail([string]$msg) {
    Write-Host "  ✗ BŁĄD: $msg" -ForegroundColor Red
    Write-Host "  Zatrzymuję — popraw błąd i uruchom skrypt ponownie." -ForegroundColor Yellow
    exit 1
}
function Info([string]$key, [string]$val) {
    Write-Host "  $key" -ForegroundColor Gray -NoNewline
    Write-Host " $val" -ForegroundColor Yellow
}

# Uruchamia az i zatrzymuje skrypt jeśli coś pójdzie nie tak
function Invoke-Az {
    param([string[]]$Arguments, [string]$ErrorMsg)
    $output = az @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        $details = ($output | Where-Object { "$_" -match "ERROR|Message|error" }) -join "`n"
        Fail "$ErrorMsg`n    $details"
    }
    return $output
}

# ─── 0. Subskrypcja + rejestracja resource providerów ───────────────────────
Step "Ustawianie subskrypcji..."
Invoke-Az @("account", "set", "--subscription", $SUBSCRIPTION_ID) `
    -ErrorMsg "Nie można ustawić subskrypcji $SUBSCRIPTION_ID"
OK "Subskrypcja: $SUBSCRIPTION_ID"

Step "Rejestracja Microsoft.Web provider (wymagany przez Function App i SWA)..."
$webState = (az provider show --namespace Microsoft.Web --query "registrationState" -o tsv 2>$null)
if ($webState -ne "Registered") {
    Write-Host "  Rejestrowanie Microsoft.Web — może potrwać 1-2 minuty..." -ForegroundColor Yellow
    Invoke-Az @("provider", "register", "--namespace", "Microsoft.Web", "--wait") `
        -ErrorMsg "Nie można zarejestrować Microsoft.Web provider"
    OK "Microsoft.Web provider zarejestrowany"
} else {
    OK "Microsoft.Web provider już zarejestrowany"
}

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
$funcExists = (az functionapp show --name $FUNC_NAME --resource-group $RESOURCE_GROUP --query "name" -o tsv 2>$null)
if ($funcExists) {
    OK "Function App już istnieje"
} else {
    Invoke-Az @(
        "functionapp", "create",
        "--name", $FUNC_NAME,
        "--resource-group", $RESOURCE_GROUP,
        "--consumption-plan-location", $LOCATION,
        "--runtime", "node",
        "--runtime-version", "22",
        "--functions-version", "4",
        "--storage-account", $STORAGE_NAME,
        "--assign-identity", "[system]"
    ) -ErrorMsg "Nie można utworzyć Function App"
    OK "Function App '$FUNC_NAME' utworzony z Managed Identity"
}

# Pobierz Managed Identity Principal ID
$PRINCIPAL_ID = (az functionapp identity show `
    --name $FUNC_NAME `
    --resource-group $RESOURCE_GROUP `
    --query "principalId" -o tsv 2>$null)
OK "Managed Identity Principal ID: $PRINCIPAL_ID"

# Pobierz URL Function App
$FUNC_HOST = (az functionapp show `
    --name $FUNC_NAME `
    --resource-group $RESOURCE_GROUP `
    --query "defaultHostName" -o tsv 2>$null)
$FUNC_URL = "https://$FUNC_HOST"
OK "Function App URL: $FUNC_URL"

# CORS — localhost
Invoke-Az @(
    "functionapp", "cors", "add",
    "--name", $FUNC_NAME,
    "--resource-group", $RESOURCE_GROUP,
    "--allowed-origins", "http://localhost:3000"
) -ErrorMsg "Nie można dodać CORS"
OK "CORS: localhost:3000 dodany"

# ─── 4. Azure Static Web App ─────────────────────────────────────────────────
Step "Tworzenie Static Web App '$SWA_NAME'..."
$swaExists = (az staticwebapp show --name $SWA_NAME --resource-group $RESOURCE_GROUP --query "name" -o tsv 2>$null)
if ($swaExists) {
    OK "Static Web App już istnieje"
} else {
    Invoke-Az @(
        "staticwebapp", "create",
        "--name", $SWA_NAME,
        "--resource-group", $RESOURCE_GROUP,
        "--location", $SWA_LOCATION,
        "--sku", "Free"
    ) -ErrorMsg "Nie można utworzyć Static Web App"
    OK "Static Web App '$SWA_NAME' utworzony"
}

# Pobierz URL SWA
$SWA_HOST = (az staticwebapp show `
    --name $SWA_NAME `
    --resource-group $RESOURCE_GROUP `
    --query "defaultHostname" -o tsv 2>$null)
$SWA_URL = "https://$SWA_HOST"
OK "SWA URL: $SWA_URL"

# Pobierz deployment token
$DEPLOY_TOKEN = (az staticwebapp secrets list `
    --name $SWA_NAME `
    --resource-group $RESOURCE_GROUP `
    --query "properties.apiKey" -o tsv 2>$null)
OK "Deployment token pobrany"

# ─── 5. Redirect URIs w App Registration ─────────────────────────────────────
Step "Aktualizacja redirect URIs w App Registration..."
Invoke-Az @(
    "ad", "app", "update",
    "--id", $CLIENT_ID,
    "--web-redirect-uris", "http://localhost:3000", $SWA_URL
) -ErrorMsg "Nie można zaktualizować redirect URIs"
OK "Redirect URIs: localhost:3000 + $SWA_URL"

# ─── 6. CORS w Function App — dodaj SWA URL ──────────────────────────────────
Step "Aktualizacja CORS w Function App (dodaję SWA URL)..."
Invoke-Az @(
    "functionapp", "cors", "add",
    "--name", $FUNC_NAME,
    "--resource-group", $RESOURCE_GROUP,
    "--allowed-origins", $SWA_URL
) -ErrorMsg "Nie można dodać CORS dla SWA"
OK "CORS: $SWA_URL dodany"

# ─── 7. App Settings dla Function App ────────────────────────────────────────
Step "Konfiguracja zmiennych środowiskowych Function App..."
Invoke-Az @(
    "functionapp", "config", "appsettings", "set",
    "--name", $FUNC_NAME,
    "--resource-group", $RESOURCE_GROUP,
    "--settings",
    "AZURE_TENANT_ID=$TENANT_ID",
    "VITE_CLIENT_ID=$CLIENT_ID",
    "ALLOWED_ORIGINS=$SWA_URL",
    "FABRIC_SQL_SERVER=__TODO_FILL_AFTER_FABRIC_SETUP__"
) -ErrorMsg "Nie można ustawić App Settings"
OK "App Settings zaktualizowane"

# ─── 8. Zapisz wyniki ─────────────────────────────────────────────────────────
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
#    Nadaj Managed Identity (PRINCIPAL_ID) rolę Contributor w workspace
#
# 5. Wpisz FABRIC_SQL_SERVER do Function App settings:
#    az functionapp config appsettings set
#      --name func-mdm-stewardship
#      --resource-group rg-fabric-poc-sdc
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
