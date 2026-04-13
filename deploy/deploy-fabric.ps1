<#
.SYNOPSIS
    Deployuje MDM Stewardship na Microsoft Fabric przez REST API.
    Nie wymaga klikania w portal — wszystko przez API.

.DESCRIPTION
    1. Tworzy Lakehouse lh_mdm (jeśli nie istnieje)
    2. Uploaduje notebooki z fabric/notebooks/ do workspace
    3. Uruchamia nb_bootstrap_ddl (tworzy tabele Delta)
    4. Uruchamia nb_seed_demo_data (dane demo)

.PARAMETER WorkspaceId
    Fabric Workspace ID — portal.fabric.microsoft.com → workspace → URL: /groups/{WorkspaceId}

.PARAMETER SkipSeed
    Pomiń seed danych demo (dla production deploy)

.EXAMPLE
    .\deploy\deploy-fabric.ps1 -WorkspaceId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    .\deploy\deploy-fabric.ps1 -WorkspaceId "..." -SkipSeed
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [switch]$SkipSeed
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

$FABRIC_API = "https://api.fabric.microsoft.com/v1"
$REPO_ROOT  = Split-Path $PSScriptRoot -Parent

function Step([string]$msg) { Write-Host "`n▶ $msg" -ForegroundColor Cyan }
function OK([string]$msg)   { Write-Host "  ✓ $msg" -ForegroundColor Green }
function Warn([string]$msg) { Write-Host "  ⚠ $msg" -ForegroundColor Yellow }
function Fail([string]$msg) { Write-Host "  ✗ $msg" -ForegroundColor Red; exit 1 }

# ─── AUTH ─────────────────────────────────────────────────────────────────────
Step "Pobieranie tokenu Fabric API..."
$TOKEN = (az account get-access-token `
    --resource "https://api.fabric.microsoft.com" `
    --query "accessToken" -o tsv 2>$null)

if (-not $TOKEN) {
    Fail "Brak tokenu — upewnij się że jesteś zalogowany: az login"
}
OK "Token pobrany"

$HEADERS = @{
    "Authorization" = "Bearer $TOKEN"
    "Content-Type"  = "application/json"
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────

function Invoke-Fabric {
    param([string]$Method, [string]$Path, [object]$Body = $null)
    $uri  = "$FABRIC_API$Path"
    $args = @{ Method = $Method; Uri = $uri; Headers = $HEADERS }
    if ($Body) { $args.Body = ($Body | ConvertTo-Json -Depth 20 -Compress) }
    try {
        return Invoke-RestMethod @args
    } catch {
        $status = $_.Exception.Response.StatusCode.value__
        $msg    = $_.ErrorDetails.Message
        if ($status -eq 409) { return $null }   # Already exists — OK
        Fail "API $Method $Path → HTTP $status`: $msg"
    }
}

# Konwertuje .py notebook (z komentarzami # CELL N:) na base64 ipynb
function ConvertTo-FabricNotebookB64 {
    param([string]$PythonFile, [string]$LakehouseName = "lh_mdm")

    $raw   = Get-Content $PythonFile -Raw -Encoding UTF8
    $lines = $raw -split "`r?`n"

    $cells = [System.Collections.Generic.List[object]]::new()
    $buf   = [System.Collections.Generic.List[string]]::new()

    foreach ($line in $lines) {
        if ($line -match "^# -{10,}$") { continue }            # separator line
        if ($line -match "^# CELL \d+") {                      # new cell marker
            if ($buf.Count -gt 0) {
                $cells.Add(@{
                    cell_type        = "code"
                    source           = ($buf -join "`n").Trim()
                    metadata         = @{ microsoft = @{ language = "python" } }
                    outputs          = @()
                    execution_count  = $null
                })
                $buf.Clear()
            }
            continue
        }
        $buf.Add($line)
    }
    if ($buf.Count -gt 0) {
        $cells.Add(@{
            cell_type = "code"; source = ($buf -join "`n").Trim()
            metadata = @{ microsoft = @{ language = "python" } }; outputs = @()
        })
    }

    $ipynb = @{
        nbformat       = 4
        nbformat_minor = 5
        metadata       = @{
            kernelspec    = @{ display_name = "Synapse PySpark"; language = "Python"; name = "synapse_pyspark" }
            language_info = @{ name = "python" }
            microsoft     = @{
                default_lakehouse = @{ default_lakehouse_name = $LakehouseName }
            }
        }
        cells = $cells.ToArray()
    }

    $json  = $ipynb | ConvertTo-Json -Depth 20 -Compress
    return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($json))
}

# Tworzy notebook w Fabric lub aktualizuje jeśli istnieje
function Deploy-Notebook {
    param([string]$Name, [string]$PythonFile)

    $b64 = ConvertTo-FabricNotebookB64 -PythonFile $PythonFile

    # Sprawdź czy już istnieje
    $existing = (Invoke-Fabric GET "/workspaces/$WorkspaceId/notebooks").value `
        | Where-Object { $_.displayName -eq $Name }

    if ($existing) {
        # Zaktualizuj definicję
        Invoke-Fabric POST "/workspaces/$WorkspaceId/notebooks/$($existing.id)/updateDefinition" -Body @{
            definition = @{
                format = "ipynb"
                parts  = @(@{ path = "notebook-content.ipynb"; payload = $b64; payloadType = "InlineBase64" })
            }
        } | Out-Null
        OK "Notebook zaktualizowany: $Name (id=$($existing.id))"
        return $existing.id
    } else {
        $result = Invoke-Fabric POST "/workspaces/$WorkspaceId/notebooks" -Body @{
            displayName = $Name
            definition  = @{
                format = "ipynb"
                parts  = @(@{ path = "notebook-content.ipynb"; payload = $b64; payloadType = "InlineBase64" })
            }
        }
        OK "Notebook utworzony: $Name (id=$($result.id))"
        return $result.id
    }
}

# Uruchamia notebook i czeka na zakończenie
function Run-Notebook {
    param([string]$NotebookId, [string]$Name, [hashtable]$Params = @{})

    $body = @{}
    if ($Params.Count -gt 0) {
        $body.configuration = @{ parameters = $Params }
    }

    $job = Invoke-Fabric POST "/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances?jobType=RunNotebook" -Body $body
    if (-not $job) { Fail "Nie można uruchomić: $Name" }

    $jobId = $job.id
    Write-Host "  ⏳ Uruchomiono $Name (jobId=$jobId) — czekam na zakończenie..." -ForegroundColor Gray

    # Polling co 10 sekund, timeout 15 min
    $deadline = (Get-Date).AddMinutes(15)
    do {
        Start-Sleep 10
        $status = (Invoke-Fabric GET "/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances/$jobId").status
        Write-Host "  ... status: $status" -ForegroundColor DarkGray
    } while ($status -notin @("Succeeded","Failed","Cancelled","DeadLettered") -and (Get-Date) -lt $deadline)

    if ($status -eq "Succeeded") {
        OK "$Name zakończony pomyślnie"
    } else {
        Fail "$Name zakończył się z statusem: $status"
    }
}

# ─── 1. LAKEHOUSE ─────────────────────────────────────────────────────────────
Step "Tworzenie Lakehouse 'lh_mdm'..."
$lh = Invoke-Fabric POST "/workspaces/$WorkspaceId/lakehouses" -Body @{
    displayName = "lh_mdm"
    description = "MDM Stewardship — Bronze / Silver DV / Gold"
}
if ($lh) {
    OK "Lakehouse 'lh_mdm' utworzony (id=$($lh.id))"
} else {
    OK "Lakehouse 'lh_mdm' już istnieje"
}

# ─── 2. UPLOAD NOTEBOOKÓW ─────────────────────────────────────────────────────
Step "Upload notebooków do Fabric workspace..."
$nbDir = Join-Path $REPO_ROOT "fabric\notebooks"

$NB_IDS = @{}
foreach ($file in Get-ChildItem $nbDir -Filter "*.py" | Where-Object { $_.Name -ne "requirements.txt" }) {
    $name = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
    $NB_IDS[$name] = Deploy-Notebook -Name $name -PythonFile $file.FullName
}

# Upload seed notebook (jest w innym katalogu)
$seedFile = Join-Path $REPO_ROOT "fabric\lakehouse\seed\nb_seed_demo_data.py"
$NB_IDS["nb_seed_demo_data"] = Deploy-Notebook -Name "nb_seed_demo_data" -PythonFile $seedFile
OK "Wszystkie notebooki gotowe"

# ─── 3. DDL — BOOTSTRAP ───────────────────────────────────────────────────────
Step "Tworzenie tabel Delta (DDL bootstrap)..."

# Tworzymy inline bootstrap notebook z DDL embedded
$ddlFiles = @(
    "mdm_config\create_mdm_config_tables.sql",
    "bronze\create_bronze_tables.sql",
    "silver_dv\create_silver_dv_tables.sql",
    "gold\create_gold_tables.sql"
)

$ddlCells = @("# Bootstrap DDL — uruchom jednorazowo na pustym Lakehouse`nfrom pyspark.sql import functions as F`nprint('Starting DDL bootstrap...')")

foreach ($rel in $ddlFiles) {
    $sqlPath = Join-Path $REPO_ROOT "fabric\lakehouse\ddl\$rel"
    $sql     = Get-Content $sqlPath -Raw -Encoding UTF8
    # Zamień CREATE TABLE na spark.sql() — Spark SQL
    $cell = "# DDL: $rel`n"
    foreach ($stmt in ($sql -split ";\s*`n" | Where-Object { $_.Trim() -and $_ -notmatch "^--" })) {
        $clean = $stmt.Trim()
        if ($clean) {
            $escaped = $clean -replace '"""', "'''"
            $cell += "spark.sql(`"`"`"`n$escaped`n`"`"`")`n"
        }
    }
    $cell += "print('OK: $rel')"
    $ddlCells += $cell
}

$ddlCells += "print('✅ DDL bootstrap zakończony!')"

# Buduj ipynb inline
$cells = $ddlCells | ForEach-Object {
    @{ cell_type = "code"; source = $_; metadata = @{ microsoft = @{ language = "python" } }; outputs = @(); execution_count = $null }
}
$ddlIpynb = @{
    nbformat = 4; nbformat_minor = 5
    metadata = @{
        kernelspec    = @{ display_name = "Synapse PySpark"; language = "Python"; name = "synapse_pyspark" }
        language_info = @{ name = "python" }
        microsoft     = @{ default_lakehouse = @{ default_lakehouse_name = "lh_mdm" } }
    }
    cells = $cells
}
$ddlB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($ddlIpynb | ConvertTo-Json -Depth 20 -Compress)))

$existing = (Invoke-Fabric GET "/workspaces/$WorkspaceId/notebooks").value | Where-Object { $_.displayName -eq "nb_bootstrap_ddl" }
if ($existing) {
    $ddlNbId = $existing.id
    Invoke-Fabric POST "/workspaces/$WorkspaceId/notebooks/$ddlNbId/updateDefinition" -Body @{
        definition = @{ format = "ipynb"; parts = @(@{ path = "notebook-content.ipynb"; payload = $ddlB64; payloadType = "InlineBase64" }) }
    } | Out-Null
} else {
    $ddlNb   = Invoke-Fabric POST "/workspaces/$WorkspaceId/notebooks" -Body @{
        displayName = "nb_bootstrap_ddl"
        definition  = @{ format = "ipynb"; parts = @(@{ path = "notebook-content.ipynb"; payload = $ddlB64; payloadType = "InlineBase64" }) }
    }
    $ddlNbId = $ddlNb.id
}
OK "nb_bootstrap_ddl gotowy (id=$ddlNbId)"

Run-Notebook -NotebookId $ddlNbId -Name "nb_bootstrap_ddl"

# Seed MDM config
Step "Seeding MDM config tables..."
$seedConfigFile = Join-Path $REPO_ROOT "fabric\lakehouse\seed\seed_mdm_config_location.sql"
$seedConfig = Get-Content $seedConfigFile -Raw -Encoding UTF8
$configCells = @(
    "from pyspark.sql import SparkSession`nprint('Seeding MDM config...')",
    "# seed_mdm_config_location.sql`n" + ($seedConfig -split ";\s*`n" | Where-Object { $_.Trim() -and $_ -notmatch "^--" } | ForEach-Object {
        "spark.sql(`"`"`"`n$($_.Trim())`n`"`"`")"
    } | Out-String),
    "print('✅ MDM config seed zakończony!')"
)
$configNbId = (Deploy-Notebook -Name "nb_seed_mdm_config" -PythonFile (New-TemporaryFile).FullName)
# Przepisz z właściwymi cells
$cfgCells = $configCells | ForEach-Object { @{ cell_type = "code"; source = $_; metadata = @{ microsoft = @{ language = "python" } }; outputs = @() } }
$cfgIpynb = @{
    nbformat = 4; nbformat_minor = 5
    metadata = @{
        kernelspec = @{ display_name = "Synapse PySpark"; language = "Python"; name = "synapse_pyspark" }
        language_info = @{ name = "python" }
        microsoft = @{ default_lakehouse = @{ default_lakehouse_name = "lh_mdm" } }
    }
    cells = $cfgCells
}
$cfgB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($cfgIpynb | ConvertTo-Json -Depth 20 -Compress)))
$existingCfg = (Invoke-Fabric GET "/workspaces/$WorkspaceId/notebooks").value | Where-Object { $_.displayName -eq "nb_seed_mdm_config" }
if ($existingCfg) { $configNbId = $existingCfg.id; Invoke-Fabric POST "/workspaces/$WorkspaceId/notebooks/$configNbId/updateDefinition" -Body @{ definition = @{ format = "ipynb"; parts = @(@{ path = "notebook-content.ipynb"; payload = $cfgB64; payloadType = "InlineBase64" }) } } | Out-Null }
else { $cfgNb = Invoke-Fabric POST "/workspaces/$WorkspaceId/notebooks" -Body @{ displayName = "nb_seed_mdm_config"; definition = @{ format = "ipynb"; parts = @(@{ path = "notebook-content.ipynb"; payload = $cfgB64; payloadType = "InlineBase64" }) } }; $configNbId = $cfgNb.id }
Run-Notebook -NotebookId $configNbId -Name "nb_seed_mdm_config"

# ─── 4. SEED DEMO DATA ────────────────────────────────────────────────────────
if (-not $SkipSeed) {
    Step "Ładowanie danych demo (20 lokalizacji L'Osteria)..."
    Run-Notebook -NotebookId $NB_IDS["nb_seed_demo_data"] -Name "nb_seed_demo_data"
} else {
    Warn "Pominięto seed demo (flaga -SkipSeed)"
}

# ─── PODSUMOWANIE ─────────────────────────────────────────────────────────────
Write-Host "`n$('='*65)" -ForegroundColor Magenta
Write-Host "  FABRIC DEPLOY — GOTOWE" -ForegroundColor Magenta
Write-Host "$('='*65)" -ForegroundColor Magenta
Write-Host "  Workspace ID:  $WorkspaceId" -ForegroundColor Yellow
Write-Host "  Lakehouse:     lh_mdm" -ForegroundColor Yellow
Write-Host "  Notebooki:     $($NB_IDS.Count + 2) wgranych" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Następny krok:" -ForegroundColor White
Write-Host "  Fabric Portal → lh_mdm → SQL Analytics Endpoint" -ForegroundColor White
Write-Host "  Skopiuj 'Server' i wklej tutaj:" -ForegroundColor White
Write-Host "  → az functionapp config appsettings set \" -ForegroundColor Gray
Write-Host "      --name func-mdm-stewardship \" -ForegroundColor Gray
Write-Host "      --resource-group rg-fabric-poc-sdc \" -ForegroundColor Gray
Write-Host "      --settings `"FABRIC_SQL_SERVER=<endpoint>`"" -ForegroundColor Gray
Write-Host "$('='*65)" -ForegroundColor Magenta
