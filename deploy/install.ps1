<#
.SYNOPSIS
    One-shot installer dla MDM Stewardship — czyta deploy/mdm.config.json i orkiestruje
    bootstrap Azure + Fabric + Function App settings + UI .env.

.DESCRIPTION
    Krok po kroku:
      1. Walidacja pliku konfiguracyjnego (JSON Schema)
      2. (opcjonalnie) Provisioning zasobów Azure
      3. (opcjonalnie) Provisioning artefaktów Fabric (Lakehouse + Warehouse + DDL z prefix)
      4. Konfiguracja Function App settings (AZURE_TENANT_ID, EXPECTED_AUDIENCE, MDM_SCHEMA_*)
      5. Generowanie stewardship-ui/.env.production.local
      6. Wypisanie wartości do GitHub Secrets + .deploy-output.json
      7. Smoke test (curl /health)

    Idempotentny — można uruchamiać wielokrotnie.

.PARAMETER ConfigPath
    Ścieżka do pliku konfiguracyjnego. Default: deploy/mdm.config.json

.PARAMETER WhatIf
    Wypisz plan bez wykonywania (walidacja konfiguracji + summary).

.PARAMETER SkipAzure
    Pomiń krok provisioning Azure (np. zasoby już istnieją).

.PARAMETER SkipFabric
    Pomiń krok provisioning Fabric.

.PARAMETER SkipFunctionConfig
    Pomiń ustawianie Function App settings.

.EXAMPLE
    pwsh deploy/install.ps1                                   # full install
    pwsh deploy/install.ps1 -WhatIf                           # tylko walidacja + plan
    pwsh deploy/install.ps1 -SkipAzure                        # tylko Fabric + ustawienia
    pwsh deploy/install.ps1 -ConfigPath ./deploy/prod.json    # custom config
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'mdm.config.json'),
    [switch]$SkipAzure,
    [switch]$SkipFabric,
    [switch]$SkipFunctionConfig
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. "$PSScriptRoot/lib/Load-MdmConfig.ps1"

function Step([string]$msg) { Write-Host "`n▶ $msg" -ForegroundColor Cyan }
function OK([string]$msg)   { Write-Host "  ✓ $msg" -ForegroundColor Green }
function Warn([string]$msg) { Write-Host "  ⚠ $msg" -ForegroundColor Yellow }
function Fail([string]$msg) { Write-Host "  ✗ $msg" -ForegroundColor Red; exit 1 }

# ─── 1. Załaduj + zwaliduj config ─────────────────────────────────────────────
Step "Wczytywanie konfiguracji: $ConfigPath"
try {
    $cfg = Get-MdmConfig -ConfigPath $ConfigPath
} catch {
    Fail $_.Exception.Message
}
OK "Konfiguracja wczytana i zwalidowana"
Show-MdmConfigSummary -Config $cfg

if ($WhatIfPreference) {
    Write-Host "── PLAN ────────────────────────────────────────────────────" -ForegroundColor Yellow
    Write-Host "  1. Provision Azure: $(if ($SkipAzure) { 'SKIP' } else { 'YES' })"
    Write-Host "  2. Provision Fabric: $(if ($SkipFabric) { 'SKIP' } else { 'YES (lakehouse + warehouse + DDL z prefix)' })"
    Write-Host "  3. Function settings: $(if ($SkipFunctionConfig) { 'SKIP' } else { 'YES (AZURE_TENANT_ID, EXPECTED_AUDIENCE, MDM_SCHEMA_*)' })"
    Write-Host "  4. UI .env.production.local"
    Write-Host "  5. Smoke test /health"
    Write-Host "────────────────────────────────────────────────────────────" -ForegroundColor Yellow
    Write-Host "`n(Nie wykonano żadnych zmian — to był -WhatIf)" -ForegroundColor Yellow
    exit 0
}

# Confirm before proceeding
$confirm = Read-Host "`nKontynuować deployment? (y/N)"
if ($confirm -ne 'y' -and $confirm -ne 'Y') { Write-Host 'Anulowano.'; exit 0 }

# ─── 2. (opcjonalnie) Azure resources ─────────────────────────────────────────
if (-not $SkipAzure) {
    Step "Provisioning Azure resources..."
    $azScript = Join-Path $PSScriptRoot 'create-azure-resources.ps1'
    if (Test-Path $azScript) {
        & $azScript -Config $cfg
        if ($LASTEXITCODE -ne 0) { Fail "create-azure-resources.ps1 failed with exit code $LASTEXITCODE" }
    } else {
        Warn "create-azure-resources.ps1 nie znaleziony — pomijam (custom Azure provisioning?)"
    }
} else {
    Warn "SkipAzure — pomijam provisioning Azure"
}

# ─── 3. (opcjonalnie) Fabric ──────────────────────────────────────────────────
if (-not $SkipFabric) {
    Step "Provisioning Fabric (Lakehouse + Warehouse + DDL)..."
    $fabricScript = Join-Path $PSScriptRoot 'deploy-fabric.ps1'
    if (-not (Test-Path $fabricScript)) { Fail "Missing $fabricScript" }
    & $fabricScript -WorkspaceId $cfg.Raw.fabric.workspaceId `
                    -LakehouseName $cfg.Raw.fabric.lakehouseName `
                    -WarehouseName $cfg.Raw.fabric.warehouseName `
                    -SchemasJson $cfg.Effective.SchemasJson
    if ($LASTEXITCODE -ne 0) { Fail "deploy-fabric.ps1 failed with exit code $LASTEXITCODE" }
} else {
    Warn "SkipFabric — pomijam Fabric"
}

# ─── 4. Function App settings ─────────────────────────────────────────────────
if (-not $SkipFunctionConfig) {
    Step "Konfiguracja Function App settings..."
    $rg   = $cfg.Raw.azure.resourceGroup
    $func = $cfg.Raw.azure.functionAppName

    $settings = @(
        "AZURE_TENANT_ID=$($cfg.Raw.auth.tenantId)",
        "EXPECTED_AUDIENCE=$($cfg.Effective.ExpectedAudience)",
        "MDM_SCHEMA_BRONZE=$($cfg.Effective.Schemas.Bronze)",
        "MDM_SCHEMA_SILVER=$($cfg.Effective.Schemas.Silver)",
        "MDM_SCHEMA_GOLD=$($cfg.Effective.Schemas.Gold)",
        "MDM_SCHEMA_CONFIG=$($cfg.Effective.Schemas.Config)"
    )

    Write-Host "  → az functionapp config appsettings set -g $rg -n $func"
    foreach ($s in $settings) { Write-Host "      $s" -ForegroundColor DarkGray }

    $azArgs = @('functionapp', 'config', 'appsettings', 'set',
                '-g', $rg, '-n', $func, '--settings') + $settings
    az @azArgs --output none
    if ($LASTEXITCODE -ne 0) { Fail "az functionapp config appsettings set failed" }
    OK "Function App settings ustawione"
} else {
    Warn "SkipFunctionConfig — pomijam"
}

# ─── 5. UI .env.production.local ──────────────────────────────────────────────
Step "Generowanie stewardship-ui/.env.production.local..."
$repoRoot = Split-Path $PSScriptRoot -Parent
$uiEnvPath = Join-Path $repoRoot 'stewardship-ui/.env.production.local'

$funcUrl = "https://$($cfg.Raw.azure.functionAppName).azurewebsites.net"
@(
    "VITE_TENANT_ID=$($cfg.Raw.auth.tenantId)",
    "VITE_CLIENT_ID=$($cfg.Raw.auth.clientId)",
    "VITE_API_SCOPE=$($cfg.Effective.ApiScope)",
    "VITE_WRITE_API_URL=$funcUrl",
    "VITE_MOCK_MODE=false"
) | Set-Content -Path $uiEnvPath -Encoding utf8
OK "Zapisano $uiEnvPath"

# ─── 6. Output summary ────────────────────────────────────────────────────────
$outputPath = Join-Path $PSScriptRoot '.deploy-output.json'
$output = [ordered]@{
    timestamp = (Get-Date -Format 'o')
    azure     = $cfg.Raw.azure
    fabric    = $cfg.Raw.fabric
    auth      = @{
        tenantId         = $cfg.Raw.auth.tenantId
        clientId         = $cfg.Raw.auth.clientId
        apiScope         = $cfg.Effective.ApiScope
        expectedAudience = $cfg.Effective.ExpectedAudience
    }
    schemas        = $cfg.Effective.Schemas
    functionAppUrl = $funcUrl
    ghSecrets      = @{
        VITE_TENANT_ID     = $cfg.Raw.auth.tenantId
        VITE_CLIENT_ID     = $cfg.Raw.auth.clientId
        VITE_API_SCOPE     = $cfg.Effective.ApiScope
        VITE_WRITE_API_URL = $funcUrl
    }
}
$output | ConvertTo-Json -Depth 10 | Set-Content -Path $outputPath -Encoding utf8
OK "Zapisano $outputPath (sekrety + summary, NIE commituj)"

# ─── 7. Smoke test ────────────────────────────────────────────────────────────
Step "Smoke test (GET $funcUrl/api/health)..."
try {
    $resp = Invoke-WebRequest -Uri "$funcUrl/api/health" -UseBasicParsing -TimeoutSec 30 -ErrorAction Stop
    if ($resp.StatusCode -eq 200) {
        OK "Health check zielony: $($resp.Content)"
    } else {
        Warn "Health check zwrócił HTTP $($resp.StatusCode)"
    }
} catch {
    Warn "Smoke test failed: $($_.Exception.Message). Function App może jeszcze się rozgrzewać — spróbuj za 30s."
}

# ─── Final summary ────────────────────────────────────────────────────────────
Write-Host "`n╔══════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║  Deploy zakończony.                                      ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "Następne kroki:" -ForegroundColor Cyan
Write-Host "  1. App Registration → 'Expose an API':"
Write-Host "     - Application ID URI = $($cfg.Effective.ExpectedAudience)"
Write-Host "     - Add scope: 'access_as_user'"
Write-Host "  2. Skopiuj wartości z $outputPath do GitHub Secrets (sekcja ghSecrets)"
Write-Host "  3. Fabric Environment '$($cfg.Raw.fabric.environmentName)' z biblioteką jellyfish==1.0.3"
Write-Host "  4. Po deploy'u UI: otwórz $funcUrl/api/health (powinno zwrócić 200)"
Write-Host ""
