# Loader dla mdm.config.json — czyta plik, waliduje przez JSON Schema (Test-Json)
# i zwraca obiekt z efektywnymi nazwami schematów (prefix + base).
#
# Użycie:
#   . "$PSScriptRoot\Load-MdmConfig.ps1"
#   $cfg = Get-MdmConfig -ConfigPath ./deploy/mdm.config.json
#   $cfg.Effective.Schemas.Silver  # np. "mdm_silver_dv"
#   $cfg.Effective.ApiScope        # np. "api://abc.../access_as_user"

Set-StrictMode -Version Latest

function Get-MdmConfig {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ConfigPath,
        [string]$SchemaPath = $null
    )

    if (-not (Test-Path $ConfigPath)) {
        throw "Config file not found: $ConfigPath. Skopiuj deploy/mdm.config.example.json -> deploy/mdm.config.json i wypełnij."
    }

    $configRaw = Get-Content $ConfigPath -Raw

    # Walidacja JSON Schema (PS 7.4+ wspiera -Schema)
    if (-not $SchemaPath) {
        $SchemaPath = Join-Path (Split-Path $ConfigPath -Parent) "mdm.config.schema.json"
    }
    if (Test-Path $SchemaPath) {
        try {
            $valid = Test-Json -Json $configRaw -SchemaFile $SchemaPath -ErrorAction Stop
            if (-not $valid) { throw "Config did not match schema." }
        } catch {
            throw "Walidacja schema dla $ConfigPath nie powiodła się: $($_.Exception.Message)"
        }
    } else {
        Write-Warning "JSON Schema not found at $SchemaPath — pomijam walidację."
    }

    $cfg = $configRaw | ConvertFrom-Json

    # Efektywne nazwy schematów
    $prefix = $cfg.mdm.schemaPrefix
    $effSchemas = [ordered]@{
        Bronze = "$prefix$($cfg.mdm.schemas.bronze)"
        Silver = "$prefix$($cfg.mdm.schemas.silver)"
        Gold   = "$prefix$($cfg.mdm.schemas.gold)"
        Config = "$prefix$($cfg.mdm.schemas.config)"
    }

    # Ostrzeżenie gdy prefix jest niepusty — w obecnej wersji nie wszystkie warstwy
    # są w pełni prefix-aware. Function + Warehouse DDL: tak. Lakehouse notebooki: nie.
    if ($prefix) {
        Write-Warning @"
schemaPrefix='$prefix' jest niepusty.
W tej wersji prefix działa pełnoetapowo dla:
  - Azure Function (przez MDM_SCHEMA_* env vars)
  - Warehouse DDL i seed (przez apply-warehouse-ddl.js --schemas)
NIE jest w pełni przepuszczony przez:
  - Lakehouse DDL w notebookach (nb_bootstrap_ddl.py i bronze/raw_vault/derive)
  - Pipelines Fabric (PL_MDM_*.json)
Jeżeli klient ma już istniejący Warehouse z konfliktującymi schematami `silver_dv`/`gold`/`mdm_config`/`bronze`,
ten use-case zadziała (Function + Warehouse). Jeżeli wdrażasz pełny stack (z Lakehouse), zostaw schemaPrefix='' albo zaktualizuj notebooki ręcznie.
"@
    }

    # Efektywny apiScope ('auto' -> api://<clientId>/access_as_user)
    $apiScope = $cfg.auth.apiScope
    if ([string]::IsNullOrWhiteSpace($apiScope) -or $apiScope -eq 'auto') {
        $apiScope = "api://$($cfg.auth.clientId)/access_as_user"
    }

    # Expected audience (dla EXPECTED_AUDIENCE w Function)
    $expectedAudience = "api://$($cfg.auth.clientId)"

    # JSON do przekazania do apply-warehouse-ddl.js --schemas
    $schemasJson = ($effSchemas.GetEnumerator() | ForEach-Object {
        @{ ('SCHEMA_' + $_.Key.ToUpper()) = $_.Value }
    } | ForEach-Object { $_.GetEnumerator() } | ForEach-Object {
        [PSCustomObject]@{ Key = $_.Key; Value = $_.Value }
    })
    $schemasJsonString = ($schemasJson | ForEach-Object {
        '"' + $_.Key + '":"' + $_.Value + '"'
    }) -join ','
    $schemasJsonString = "{$schemasJsonString}"

    return [PSCustomObject]@{
        Raw       = $cfg
        Effective = [PSCustomObject]@{
            Schemas          = [PSCustomObject]$effSchemas
            ApiScope         = $apiScope
            ExpectedAudience = $expectedAudience
            SchemasJson      = $schemasJsonString
        }
    }
}

function Show-MdmConfigSummary {
    param([Parameter(Mandatory = $true)][object]$Config)

    $c = $Config.Raw
    $e = $Config.Effective

    Write-Host ""
    Write-Host "── MDM Config Summary ──────────────────────────────────────" -ForegroundColor Cyan
    Write-Host "  Azure"
    Write-Host "    Subscription : $($c.azure.subscriptionId)"
    Write-Host "    Resource grp : $($c.azure.resourceGroup) ($($c.azure.location))"
    Write-Host "    Function     : $($c.azure.functionAppName)"
    Write-Host "    Static Web   : $($c.azure.staticWebAppName)"
    Write-Host "  Fabric"
    Write-Host "    Workspace    : $($c.fabric.workspaceId)"
    Write-Host "    Lakehouse    : $($c.fabric.lakehouseName)"
    Write-Host "    Warehouse    : $($c.fabric.warehouseName)"
    Write-Host "    Environment  : $($c.fabric.environmentName)"
    Write-Host "  MDM schemas (efektywne, prefix='$($c.mdm.schemaPrefix)')"
    Write-Host "    Bronze       : $($e.Schemas.Bronze)"
    Write-Host "    Silver       : $($e.Schemas.Silver)"
    Write-Host "    Gold         : $($e.Schemas.Gold)"
    Write-Host "    Config       : $($e.Schemas.Config)"
    Write-Host "  Auth"
    Write-Host "    Tenant       : $($c.auth.tenantId)"
    Write-Host "    Client (App) : $($c.auth.clientId)"
    Write-Host "    API scope    : $($e.ApiScope)"
    Write-Host "    Aud (Func)   : $($e.ExpectedAudience)"
    Write-Host "────────────────────────────────────────────────────────────" -ForegroundColor Cyan
    Write-Host ""
}
