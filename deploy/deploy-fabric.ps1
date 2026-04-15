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
    $uri   = "$FABRIC_API$Path"
    $splat = @{ Method = $Method; Uri = $uri; Headers = $HEADERS }
    if ($Body) { $splat.Body = ($Body | ConvertTo-Json -Depth 20 -Compress) }
    try {
        return Invoke-RestMethod @splat
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
$ddlNbId = $NB_IDS["nb_bootstrap_ddl"]
Run-Notebook -NotebookId $ddlNbId -Name "nb_bootstrap_ddl"

# Seed MDM config
Step "Seeding MDM config tables..."
$configNbId = $NB_IDS["nb_seed_mdm_config"]
Run-Notebook -NotebookId $configNbId -Name "nb_seed_mdm_config"

# ─── 4. PIPELINES ─────────────────────────────────────────────────────────────
Step "Upload Data Pipelines do Fabric workspace..."

function Deploy-Pipeline {
    param([string]$Name, [string]$JsonFile)

    $content = Get-Content $JsonFile -Raw -Encoding UTF8
    $b64     = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($content))

    $existing = (Invoke-Fabric GET "/workspaces/$WorkspaceId/dataPipelines").value `
        | Where-Object { $_.displayName -eq $Name }

    if ($existing) {
        Invoke-Fabric POST "/workspaces/$WorkspaceId/dataPipelines/$($existing.id)/updateDefinition" -Body @{
            definition = @{
                parts = @(@{ path = "pipeline-content.json"; payload = $b64; payloadType = "InlineBase64" })
            }
        } | Out-Null
        OK "Pipeline zaktualizowany: $Name"
        return $existing.id
    } else {
        $result = Invoke-Fabric POST "/workspaces/$WorkspaceId/dataPipelines" -Body @{
            displayName = $Name
            definition  = @{
                parts = @(@{ path = "pipeline-content.json"; payload = $b64; payloadType = "InlineBase64" })
            }
        }
        OK "Pipeline utworzony: $Name (id=$($result.id))"
        return $result.id
    }
}

$pipelineDir = Join-Path $REPO_ROOT "fabric\pipelines"
foreach ($file in Get-ChildItem $pipelineDir -Filter "*.json") {
    $pName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
    Deploy-Pipeline -Name $pName -JsonFile $file.FullName
}
OK "Wszystkie pipelines gotowe"

# ─── 5. SEED DEMO DATA ────────────────────────────────────────────────────────
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
