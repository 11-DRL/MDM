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
$TOKEN = $null
try {
    $TOKEN = (az account get-access-token `
        --resource "https://api.fabric.microsoft.com" `
        --query "accessToken" -o tsv 2>$null)
} catch {
    $TOKEN = $null
}

if ([string]::IsNullOrWhiteSpace($TOKEN)) {
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
        $status = $null
        $msg = $null
        if ($_.Exception -and $_.Exception.Response) {
            $status = $_.Exception.Response.StatusCode.value__
        }
        if ($_.ErrorDetails) {
            $msg = $_.ErrorDetails.Message
        }
        if (-not $msg) {
            $msg = $_.Exception.Message
        }
        if ($status -eq 409) { return $null }   # Already exists — OK
        if (-not $status) { $status = "N/A" }
        Fail "API $Method $Path → HTTP $status`: $msg"
    }
}

# Wariant zwracający pełną odpowiedź (z nagłówkami) — potrzebne do 202 Accepted + Location
function Invoke-FabricRaw {
    param([string]$Method, [string]$Path, [object]$Body = $null)
    $uri   = "$FABRIC_API$Path"
    $splat = @{ Method = $Method; Uri = $uri; Headers = $HEADERS; UseBasicParsing = $true }
    if ($Body) { $splat.Body = ($Body | ConvertTo-Json -Depth 20 -Compress) }
    try {
        return Invoke-WebRequest @splat
    } catch {
        $status = $null; $msg = $null
        if ($_.Exception -and $_.Exception.Response) {
            $status = $_.Exception.Response.StatusCode.value__
        }
        if ($_.ErrorDetails) { $msg = $_.ErrorDetails.Message }
        if (-not $msg)       { $msg = $_.Exception.Message }
        if (-not $status)    { $status = "N/A" }
        Fail "API $Method $Path → HTTP $status`: $msg"
    }
}

# Konwertuje .py notebook (z komentarzami # CELL N:) na base64 w formacie fabricGitSource
# Natywny format Fabric — lakehouse binding (dependencies.lakehouse) jest respektowany
# (w odróżnieniu od ipynb, gdzie Fabric stripuje custom metadata).
function ConvertTo-FabricNotebookB64 {
    param(
        [string]$PythonFile,
        [string]$LakehouseName = "lh_mdm",
        [Parameter(Mandatory = $true)][string]$LakehouseId,
        [Parameter(Mandatory = $true)][string]$LakehouseWorkspaceId
    )

    $raw   = Get-Content $PythonFile -Raw -Encoding UTF8
    $lines = $raw -split "`r?`n"

    # Rozdziel źródło na komórki po markerach "# CELL N:"
    $cellBodies = [System.Collections.Generic.List[string]]::new()
    $buf        = [System.Collections.Generic.List[string]]::new()

    foreach ($line in $lines) {
        if ($line -match "^# -{10,}$") { continue }
        if ($line -match "^# CELL \d+") {
            if ($buf.Count -gt 0) {
                $cellBodies.Add((($buf -join "`n").TrimEnd()))
                $buf.Clear()
            }
            continue
        }
        $buf.Add($line)
    }
    if ($buf.Count -gt 0) {
        $cellBodies.Add((($buf -join "`n").TrimEnd()))
    }

    # Top-level metadata block (kernel + lakehouse dependencies)
    $topMeta = [ordered]@{
        kernel_info  = @{ name = "synapse_pyspark" }
        dependencies = @{
            lakehouse = [ordered]@{
                default_lakehouse              = $LakehouseId
                default_lakehouse_name         = $LakehouseName
                default_lakehouse_workspace_id = $LakehouseWorkspaceId
                known_lakehouses               = @(@{ id = $LakehouseId })
            }
        }
    }
    $topMetaJson = $topMeta | ConvertTo-Json -Depth 20

    # Per-cell metadata (identyczne dla wszystkich — python/synapse_pyspark)
    $cellMeta = @{ language = "python"; language_group = "synapse_pyspark" }
    $cellMetaJson = $cellMeta | ConvertTo-Json -Depth 10

    function Format-MetaBlock {
        param([string]$Json)
        $ls = $Json -split "`r?`n"
        $out = [System.Collections.Generic.List[string]]::new()
        $out.Add("# METADATA ********************")
        $out.Add("")  # Pusta linia MUSI być — bez niej Fabric parser gubi blok META
        foreach ($l in $ls) { $out.Add("# META " + $l) }
        return ($out -join "`n")
    }

    # WAŻNE: Fabric parser akceptuje tylko LF. Wszystkie CRLF → LF, używamy tylko "`n".
    # Sekcje rozdzielamy pustą linią — zgodnie z przykładem w docs REST API Fabric.
    $sb = [System.Collections.Generic.List[string]]::new()
    $sb.Add("# Fabric notebook source")
    $sb.Add("")
    $sb.Add((Format-MetaBlock -Json $topMetaJson))

    foreach ($body in $cellBodies) {
        # Normalizacja CRLF → LF wewnątrz ciała komórki
        $bodyLf = $body -replace "`r`n", "`n" -replace "`r", "`n"
        $sb.Add("")
        $sb.Add("# CELL ********************")
        $sb.Add("")
        $sb.Add($bodyLf)
        $sb.Add("")
        $sb.Add((Format-MetaBlock -Json $cellMetaJson))
    }

    # Końcowy newline + gwarancja LF only
    $src = (($sb -join "`n") + "`n") -replace "`r", ""
    return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($src))
}

# Tworzy notebook w Fabric lub aktualizuje jeśli istnieje
function Deploy-Notebook {
    param(
        [string]$Name,
        [string]$PythonFile,
        [Parameter(Mandatory = $true)][string]$LakehouseId,
        [Parameter(Mandatory = $true)][string]$LakehouseWorkspaceId
    )

    $b64 = ConvertTo-FabricNotebookB64 `
        -PythonFile $PythonFile `
        -LakehouseId $LakehouseId `
        -LakehouseWorkspaceId $LakehouseWorkspaceId

    $definition = @{
        format = "fabricGitSource"
        parts  = @(@{ path = "notebook-content.py"; payload = $b64; payloadType = "InlineBase64" })
    }

    # Sprawdź czy już istnieje
    $existing = (Invoke-Fabric GET "/workspaces/$WorkspaceId/notebooks").value `
        | Where-Object { $_.displayName -eq $Name }

    if ($existing) {
        Invoke-Fabric POST "/workspaces/$WorkspaceId/notebooks/$($existing.id)/updateDefinition" -Body @{
            definition = $definition
        } | Out-Null
        OK "Notebook zaktualizowany: $Name (id=$($existing.id))"
        return $existing.id
    } else {
        $result = Invoke-Fabric POST "/workspaces/$WorkspaceId/notebooks" -Body @{
            displayName = $Name
            definition  = $definition
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

    $resp = Invoke-FabricRaw POST "/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances?jobType=RunNotebook" -Body $body

    # Fabric zwraca 202 Accepted z pustym body i Location: /workspaces/{ws}/items/{item}/jobs/instances/{jobId}
    $jobId = $null
    $loc   = $null
    if ($resp.Headers.Location)    { $loc = [string]$resp.Headers.Location }
    elseif ($resp.Headers.location) { $loc = [string]$resp.Headers.location }
    if ($loc) {
        $jobId = ($loc -split '/')[-1]
    } elseif ($resp.Content) {
        try { $jobId = ($resp.Content | ConvertFrom-Json).id } catch {}
    }
    if (-not $jobId) { Fail "Nie można uruchomić: $Name (HTTP $($resp.StatusCode), brak jobId)" }

    Write-Host "  ⏳ Uruchomiono $Name (jobId=$jobId) — czekam na zakończenie..." -ForegroundColor Gray

    # Polling co 10 sekund, timeout 15 min
    $status = $null
    $deadline = (Get-Date).AddMinutes(15)
    do {
        Start-Sleep 10
        $inst = Invoke-Fabric GET "/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances/$jobId"
        $status = $inst.status
        Write-Host "  ... status: $status" -ForegroundColor DarkGray
    } while ($status -notin @("Completed","Succeeded","Failed","Cancelled","DeadLettered") -and (Get-Date) -lt $deadline)

    if ($status -in @("Completed","Succeeded")) {
        OK "$Name zakończony pomyślnie"
    } else {
        $detail = $null
        if ($inst.failureReason) {
            try { $detail = ($inst.failureReason | ConvertTo-Json -Depth 10 -Compress) } catch { $detail = [string]$inst.failureReason }
        }
        Fail "$Name zakończył się z statusem: $status $(if ($detail) { "| $detail" })"
    }
}

# ─── 1. LAKEHOUSE ─────────────────────────────────────────────────────────────
Step "Tworzenie Lakehouse 'lh_mdm'..."
# Sprawdź czy już istnieje
$existingLh = (Invoke-Fabric GET "/workspaces/$WorkspaceId/lakehouses").value `
    | Where-Object { $_.displayName -eq "lh_mdm" } | Select-Object -First 1

if ($existingLh) {
    $LAKEHOUSE_ID = $existingLh.id
    OK "Lakehouse 'lh_mdm' już istnieje (id=$LAKEHOUSE_ID)"
    Warn "  Jeśli lakehouse był utworzony bez enableSchemas, CREATE SCHEMA nie zadziała — usuń i redeploy."
} else {
    # Create z enableSchemas — operacja jest async (202 + operation-id)
    $body = @{
        displayName     = "lh_mdm"
        description     = "MDM Stewardship - Bronze / Silver DV / Gold"
        creationPayload = @{ enableSchemas = $true }
    } | ConvertTo-Json -Depth 10 -Compress

    $resp = Invoke-WebRequest -Method POST `
        -Uri "$FABRIC_API/workspaces/$WorkspaceId/lakehouses" `
        -Headers $HEADERS -Body $body -UseBasicParsing

    if ($resp.StatusCode -eq 201) {
        $LAKEHOUSE_ID = ($resp.Content | ConvertFrom-Json).id
        OK "Lakehouse utworzony (sync, id=$LAKEHOUSE_ID)"
    } elseif ($resp.StatusCode -eq 202) {
        $opId = $resp.Headers.'x-ms-operation-id' | Select-Object -First 1
        if (-not $opId) { $opId = $resp.Headers.'X-Ms-Operation-Id' | Select-Object -First 1 }
        OK "Lakehouse creation submitted (opId=$opId) — czekam..."
        do {
            Start-Sleep 5
            $op = Invoke-RestMethod -Uri "$FABRIC_API/operations/$opId" -Headers $HEADERS
            Write-Host "  ... $($op.status)"
        } while ($op.status -notin @("Succeeded","Failed"))
        if ($op.status -ne "Succeeded") { Fail "Lakehouse creation failed: $($op | ConvertTo-Json -Depth 10)" }
        # Result
        $existingLh = (Invoke-Fabric GET "/workspaces/$WorkspaceId/lakehouses").value `
            | Where-Object { $_.displayName -eq "lh_mdm" } | Select-Object -First 1
        if (-not $existingLh) { Fail "Lakehouse 'lh_mdm' not found after async create" }
        $LAKEHOUSE_ID = $existingLh.id
        OK "Lakehouse 'lh_mdm' utworzony (id=$LAKEHOUSE_ID, schemas=enabled)"
    } else {
        Fail "Unexpected status code: $($resp.StatusCode)"
    }
}

# ─── 2. UPLOAD NOTEBOOKÓW ─────────────────────────────────────────────────────
Step "Upload notebooków do Fabric workspace..."
$nbDir = Join-Path $REPO_ROOT "fabric\notebooks"

$NB_IDS = @{}
foreach ($file in Get-ChildItem $nbDir -Filter "*.py" | Where-Object { $_.Name -ne "requirements.txt" }) {
    $name = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
    $NB_IDS[$name] = Deploy-Notebook `
        -Name $name `
        -PythonFile $file.FullName `
        -LakehouseId $LAKEHOUSE_ID `
        -LakehouseWorkspaceId $WorkspaceId
}

# Upload seed notebook (jest w innym katalogu)
$seedFile = Join-Path $REPO_ROOT "fabric\lakehouse\seed\nb_seed_demo_data.py"
$NB_IDS["nb_seed_demo_data"] = Deploy-Notebook `
    -Name "nb_seed_demo_data" `
    -PythonFile $seedFile `
    -LakehouseId $LAKEHOUSE_ID `
    -LakehouseWorkspaceId $WorkspaceId
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
Write-Host ("`n" + ("=" * 65)) -ForegroundColor Magenta
Write-Host "  FABRIC DEPLOY - GOTOWE" -ForegroundColor Magenta
Write-Host ("=" * 65) -ForegroundColor Magenta
Write-Host "  Workspace ID:  $WorkspaceId" -ForegroundColor Yellow
Write-Host "  Lakehouse:     lh_mdm" -ForegroundColor Yellow
Write-Host "  Notebooki:     $($NB_IDS.Count + 2) wgranych" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Nastepny krok:" -ForegroundColor White
Write-Host "  Fabric Portal -> lh_mdm -> SQL Analytics Endpoint" -ForegroundColor White
Write-Host "  Skopiuj 'Server' i ustaw w Function App:" -ForegroundColor White
Write-Host '  az functionapp config appsettings set --name func-mdm-stewardship --resource-group rg-fabric-poc-sdc --settings "FABRIC_SQL_SERVER=<endpoint>"' -ForegroundColor Gray
Write-Host ("=" * 65) -ForegroundColor Magenta
