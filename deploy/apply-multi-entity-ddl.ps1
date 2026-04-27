<#
.SYNOPSIS
    Deploy MDM multi-entity DDL to Fabric Lakehouse + Warehouse.
    Run after the base MDM schema already exists (nb_bootstrap_ddl.py).

.DESCRIPTION
    Executes DDL scripts in the correct dependency order:
      1. mdm_config extensions (entity_config + field_config new columns)
      2. Silver DV entity_id migration
      3. Legal Entity bronze / silver / gold tables
      4. Seed mdm_config for legal_entity
      5. Warehouse views (hierarchy)

.PARAMETER ConfigPath
    Path to mdm.config.json. Default: deploy/mdm.config.json

.EXAMPLE
    .\deploy\apply-multi-entity-ddl.ps1
#>
param(
    [string]$ConfigPath = "$PSScriptRoot\mdm.config.json"
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Load config
if (-not (Test-Path $ConfigPath)) {
    Write-Warning "Config not found at $ConfigPath - using defaults"
    $config = @{
        lakehouse = @{ name = "lh_mdm" }
        warehouse = @{ name = "wh_mdm" }
    }
} else {
    $config = Get-Content $ConfigPath -Raw | ConvertFrom-Json
}

$repoRoot = Split-Path $PSScriptRoot -Parent
$ddlRoot  = Join-Path $repoRoot "fabric\lakehouse\ddl"
$seedRoot = Join-Path $repoRoot "fabric\lakehouse\seed"
$whRoot   = Join-Path $repoRoot "fabric\warehouse\ddl"

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  MDM Multi-Entity DDL Deployment"          -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# DDL execution order
$steps = @(
    @{ Label = "1/6  mdm_config extensions";        File = "$ddlRoot\mdm_config\alter_mdm_config_multi_entity.sql" }
    @{ Label = "2/6  Silver DV entity_id migration"; File = "$ddlRoot\silver_dv\alter_silver_dv_add_entity_id.sql" }
    @{ Label = "3/6  Legal Entity bronze";           File = "$ddlRoot\bronze\create_bronze_legal_entity.sql" }
    @{ Label = "4/6  Legal Entity silver DV";        File = "$ddlRoot\silver_dv\create_legal_entity_dv.sql" }
    @{ Label = "5/6  Legal Entity gold";             File = "$ddlRoot\gold\create_dim_legal_entity.sql" }
    @{ Label = "6/6  Seed mdm_config legal_entity";  File = "$seedRoot\seed_mdm_config_legal_entity.sql" }
)

$whSteps = @(
    @{ Label = "WH 1/1  Legal Entity hierarchy view"; File = "$whRoot\create_vw_legal_entity_hierarchy.sql" }
)

Write-Host "Lakehouse DDL scripts:" -ForegroundColor Yellow
foreach ($step in $steps) {
    $exists = Test-Path $step.File
    $status = if ($exists) { "[OK]" } else { "[MISSING]" }
    $color  = if ($exists) { "Green" } else { "Red" }
    Write-Host "  $($step.Label): $status" -ForegroundColor $color
}

Write-Host ""
Write-Host "Warehouse DDL scripts:" -ForegroundColor Yellow
foreach ($step in $whSteps) {
    $exists = Test-Path $step.File
    $status = if ($exists) { "[OK]" } else { "[MISSING]" }
    $color  = if ($exists) { "Green" } else { "Red" }
    Write-Host "  $($step.Label): $status" -ForegroundColor $color
}

Write-Host ""
Write-Host "To execute against Fabric Lakehouse SQL endpoint:" -ForegroundColor Cyan
Write-Host ""
Write-Host "  # Option A: Run via Fabric Notebook (recommended)"
Write-Host '  # Upload scripts and execute via spark.sql(open("file.sql").read())'
Write-Host ""
Write-Host "  # Option B: Run via sqlcmd / sql-query.js"
Write-Host "  foreach (`$step in steps) {"
Write-Host "    node deploy/sql-query.js --file `$step.File"
Write-Host "  }"
Write-Host ""
Write-Host "  # Warehouse (T-SQL endpoint, for hierarchy views):"
Write-Host "  node deploy/sql-query.js --target warehouse --file $($whSteps[0].File)"
Write-Host ""

# Print the SQL content for easy copy-paste to Fabric notebook
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  SQL Content Preview" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

foreach ($step in $steps) {
    if (Test-Path $step.File) {
        Write-Host ""
        Write-Host "--- $($step.Label) ---" -ForegroundColor Yellow
        $content = Get-Content $step.File -Raw
        # Show first 5 lines as preview
        $lines = $content -split "`n"
        $preview = ($lines | Select-Object -First 5) -join "`n"
        Write-Host $preview -ForegroundColor DarkGray
        if ($lines.Count -gt 5) {
            Write-Host "  ... ($($lines.Count) total lines)" -ForegroundColor DarkGray
        }
    }
}

Write-Host ""
Write-Host "Done. Review scripts above before executing." -ForegroundColor Green
