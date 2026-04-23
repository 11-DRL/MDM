<#
.SYNOPSIS
  Safe deploy of MDM Fabric artifacts via Fabric REST API.

.DESCRIPTION
  1. Ensures lakehouse lh_mdm exists
  2. Uploads/updates notebooks from fabric/notebooks and seed notebook
  3. Runs bootstrap notebooks (DDL + config seed)
  4. Uploads/updates pipelines from fabric/pipelines
  5. Optionally runs demo seed notebook
#>

param(
  [Parameter(Mandatory = $true)]
  [string]$WorkspaceId,

  [switch]$SkipSeed
)

$ErrorActionPreference = "Stop"

$FABRIC_API = "https://api.fabric.microsoft.com/v1"
$REPO_ROOT = Split-Path $PSScriptRoot -Parent

function Step([string]$msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }
function Ok([string]$msg) { Write-Host "  [OK] $msg" -ForegroundColor Green }
function Warn([string]$msg) { Write-Host "  [WARN] $msg" -ForegroundColor Yellow }

function Get-FabricToken {
  $token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
  if ([string]::IsNullOrWhiteSpace($token)) {
    throw "Could not get Fabric API token. Run 'az login' first."
  }
  return $token.Trim()
}

$TOKEN = Get-FabricToken
$HEADERS = @{
  Authorization = "Bearer $TOKEN"
  "Content-Type" = "application/json"
}

function Invoke-Fabric {
  param(
    [Parameter(Mandatory = $true)][string]$Method,
    [Parameter(Mandatory = $true)][string]$Path,
    [object]$Body = $null,
    [int[]]$AllowedStatuses = @()
  )

  $uri = "$FABRIC_API$Path"
  $splat = @{
    Method = $Method
    Uri = $uri
    Headers = $HEADERS
  }
  if ($null -ne $Body) {
    $splat.Body = ($Body | ConvertTo-Json -Depth 30 -Compress)
  }

  try {
    return Invoke-RestMethod @splat
  } catch {
    $status = $null
    $raw = $_.Exception.Message
    if ($_.Exception -and $_.Exception.Response -and $_.Exception.Response.StatusCode) {
      $status = [int]$_.Exception.Response.StatusCode
    }
    if ($status -and ($AllowedStatuses -contains $status)) {
      return $null
    }
    throw "Fabric API error [$Method $Path] status=$status message=$raw"
  }
}

function Invoke-FabricRaw {
  param(
    [Parameter(Mandatory = $true)][string]$Method,
    [Parameter(Mandatory = $true)][string]$Path,
    [object]$Body = $null,
    [int[]]$AllowedStatuses = @()
  )

  $uri = "$FABRIC_API$Path"
  $splat = @{
    Method = $Method
    Uri = $uri
    Headers = $HEADERS
  }
  if ($null -ne $Body) {
    $splat.Body = ($Body | ConvertTo-Json -Depth 40 -Compress)
  }

  $lastErr = $null
  for ($attempt = 1; $attempt -le 4; $attempt++) {
    try {
      $resp = Invoke-WebRequest @splat
      return @{
        StatusCode = [int]$resp.StatusCode
        Headers = $resp.Headers
        Content = $resp.Content
      }
    } catch {
      $status = $null
      $content = $null
      $msg = $_.Exception.Message
      if ($_.Exception -and $_.Exception.Response) {
        $status = [int]$_.Exception.Response.StatusCode
        try {
          $reader = New-Object IO.StreamReader($_.Exception.Response.GetResponseStream())
          $content = $reader.ReadToEnd()
          $reader.Close()
        } catch {}
      }
      $lastErr = "status=$status message=$msg content=$content"
      $isRetryable = (-not $status) -or ($status -ge 500) -or ($status -eq 429)
      if ($isRetryable -and $attempt -lt 4) {
        Start-Sleep -Seconds (2 * $attempt)
        continue
      }
      if ($status -and ($AllowedStatuses -contains $status)) {
        return @{
          StatusCode = [int]$status
          Headers = @{}
          Content = $content
        }
      }
      throw "Fabric raw API error [$Method $Path] $lastErr"
    }
  }
  throw "Fabric raw API error [$Method $Path] $lastErr"
}

function Get-HeaderValue {
  param(
    [Parameter(Mandatory = $true)]$Headers,
    [Parameter(Mandatory = $true)][string]$Name
  )
  $value = $Headers[$Name]
  if ($null -eq $value) { return $null }
  if ($value -is [System.Array]) { return ($value -join "") }
  return [string]$value
}

function Wait-FabricOperation {
  param(
    [Parameter(Mandatory = $true)][string]$OperationId,
    [int]$TimeoutSeconds = 600
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  do {
    $op = Invoke-Fabric -Method "GET" -Path "/operations/$OperationId"
    $status = $op.status
    Write-Host "    operation $OperationId status=$status"
    if ($status -in @("Succeeded", "Failed", "Cancelled")) {
      if ($status -ne "Succeeded") {
        throw "Operation $OperationId failed with status=$status"
      }
      return
    }
    Start-Sleep -Seconds 5
  } while ((Get-Date) -lt $deadline)

  throw "Operation $OperationId timed out"
}

function Get-FabricCollection {
  param([string]$Path)
  $resp = Invoke-Fabric -Method "GET" -Path $Path
  if ($null -eq $resp) { return @() }
  if ($resp.PSObject.Properties.Name -contains "value") {
    return @($resp.value)
  }
  return @()
}

function Convert-PythonToNotebookBase64 {
  param(
    [Parameter(Mandatory = $true)][string]$PythonFile,
    [string]$LakehouseName = "lh_mdm",
    [Parameter(Mandatory = $true)][string]$LakehouseId,
    [Parameter(Mandatory = $true)][string]$LakehouseWorkspaceId
  )

  $raw = Get-Content $PythonFile -Raw -Encoding UTF8
  $lines = $raw -split "`r?`n"

  $cells = New-Object System.Collections.Generic.List[object]
  $buffer = New-Object System.Collections.Generic.List[string]

  function Flush-Cell {
    param([System.Collections.Generic.List[string]]$Buf)
    if ($Buf.Count -eq 0) { return $null }
    $src = @()
    foreach ($l in $Buf) {
      $src += ($l + "`n")
    }
    return @{
      cell_type = "code"
      source = $src
      metadata = @{ microsoft = @{ language = "python" } }
      outputs = @()
      execution_count = $null
    }
  }

  foreach ($line in $lines) {
    if ($line -match "^# -{10,}$") { continue }
    if ($line -match "^# CELL \d+") {
      $cellObj = Flush-Cell -Buf $buffer
      if ($cellObj) { $cells.Add($cellObj) }
      $buffer.Clear()
      continue
    }
    $buffer.Add($line)
  }

  $lastCell = Flush-Cell -Buf $buffer
  if ($lastCell) { $cells.Add($lastCell) }

  $ipynb = @{
    nbformat = 4
    nbformat_minor = 5
    metadata = @{
      kernelspec = @{
        display_name = "Synapse PySpark"
        language = "Python"
        name = "synapse_pyspark"
      }
      language_info = @{ name = "python" }
      microsoft = @{ language = "python"; ms_spell_check = @{ ms_spell_check_language = "en" } }
      dependencies = @{
        lakehouse = @{
          default_lakehouse              = $LakehouseId
          default_lakehouse_name         = $LakehouseName
          default_lakehouse_workspace_id = $LakehouseWorkspaceId
          known_lakehouses               = @(@{ id = $LakehouseId })
        }
      }
    }
    cells = $cells.ToArray()
  }

  $json = $ipynb | ConvertTo-Json -Depth 40 -Compress
  return [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($json))
}

function Ensure-Lakehouse {
  param([string]$Name)
  $lakehouses = Get-FabricCollection "/workspaces/$WorkspaceId/lakehouses"
  $existing = $lakehouses | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
  if ($existing) {
    Ok "Lakehouse already exists: $Name (id=$($existing.id))"
    return $existing.id
  }

  $created = Invoke-Fabric -Method "POST" -Path "/workspaces/$WorkspaceId/lakehouses" -Body @{
    displayName = $Name
    description = "MDM Stewardship - Bronze/Silver/Gold"
  } -AllowedStatuses @(409)

  if ($created -and $created.id) {
    Ok "Lakehouse created: $Name (id=$($created.id))"
    return $created.id
  }

  $lakehouses = Get-FabricCollection "/workspaces/$WorkspaceId/lakehouses"
  $existing = $lakehouses | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
  if ($existing) {
    Ok "Lakehouse found after create call: $Name (id=$($existing.id))"
    return $existing.id
  }

  throw "Could not ensure lakehouse '$Name'."
}

function Upsert-Notebook {
  param(
    [string]$Name,
    [string]$PythonFile,
    [Parameter(Mandatory = $true)][string]$LakehouseId,
    [Parameter(Mandatory = $true)][string]$LakehouseWorkspaceId
  )

  $payloadB64 = Convert-PythonToNotebookBase64 `
    -PythonFile $PythonFile `
    -LakehouseId $LakehouseId `
    -LakehouseWorkspaceId $LakehouseWorkspaceId
  $body = @{
    definition = @{
      format = "ipynb"
      parts = @(
        @{
          path = "notebook-content.ipynb"
          payload = $payloadB64
          payloadType = "InlineBase64"
        }
      )
    }
  }

  $notebooks = Get-FabricCollection "/workspaces/$WorkspaceId/notebooks"
  $existing = $notebooks | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
  if ($existing) {
    Invoke-Fabric -Method "POST" -Path "/workspaces/$WorkspaceId/notebooks/$($existing.id)/updateDefinition" -Body $body | Out-Null
    Ok "Notebook updated: $Name"
    return $existing.id
  }

  $createBody = @{
    displayName = $Name
    definition = $body.definition
  }

  $raw = Invoke-FabricRaw -Method "POST" -Path "/workspaces/$WorkspaceId/notebooks" -Body $createBody -AllowedStatuses @(409)
  if ($raw.StatusCode -eq 409) {
    $notebooks = Get-FabricCollection "/workspaces/$WorkspaceId/notebooks"
    $existing = $notebooks | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
    if ($existing) {
      Invoke-Fabric -Method "POST" -Path "/workspaces/$WorkspaceId/notebooks/$($existing.id)/updateDefinition" -Body $body | Out-Null
      Ok "Notebook updated after conflict: $Name"
      return $existing.id
    }
    throw "Notebook name conflict but notebook not found in list: $Name"
  }
  if ($raw.StatusCode -eq 202) {
    $opId = Get-HeaderValue -Headers $raw.Headers -Name "x-ms-operation-id"
    if ([string]::IsNullOrWhiteSpace($opId)) {
      throw "Create notebook accepted but no operation id returned: $Name"
    }
    Wait-FabricOperation -OperationId $opId
    $notebooks = Get-FabricCollection "/workspaces/$WorkspaceId/notebooks"
    $existing = $notebooks | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
    if ($existing) {
      Ok "Notebook created: $Name (id=$($existing.id))"
      return $existing.id
    }
    throw "Notebook operation succeeded but notebook not found: $Name"
  }

  $created = $null
  if ($raw.Content) {
    try { $created = $raw.Content | ConvertFrom-Json } catch {}
  }
  if (-not $created -or -not $created.id) {
    throw "Failed to create notebook: $Name (status=$($raw.StatusCode))"
  }
  Ok "Notebook created: $Name (id=$($created.id))"
  return $created.id
}

function Run-Notebook {
  param(
    [Parameter(Mandatory = $true)][string]$NotebookId,
    [Parameter(Mandatory = $true)][string]$Name,
    [hashtable]$Params = @{}
  )

  $body = @{}
  if ($Params.Count -gt 0) {
    $body.configuration = @{ parameters = $Params }
  }

  $raw = Invoke-FabricRaw -Method "POST" -Path "/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances?jobType=RunNotebook" -Body $body
  $jobId = $null
  if ($raw.Headers) {
    $jobId = Get-HeaderValue -Headers $raw.Headers -Name "x-ms-job-id"
    if ([string]::IsNullOrWhiteSpace($jobId)) {
      $location = Get-HeaderValue -Headers $raw.Headers -Name "Location"
      if (-not [string]::IsNullOrWhiteSpace($location)) {
        $jobId = ($location.TrimEnd('/') -split "/")[-1]
      }
    }
  }

  if ([string]::IsNullOrWhiteSpace($jobId)) {
    throw "Failed to start notebook job: $Name"
  }

  Write-Host "  Running $Name (jobId=$jobId) ..."
  $deadline = (Get-Date).AddMinutes(20)

  do {
    Start-Sleep -Seconds 10
    $statusResp = Invoke-Fabric -Method "GET" -Path "/workspaces/$WorkspaceId/items/$NotebookId/jobs/instances/$jobId"
    $status = $statusResp.status
    Write-Host "    status=$status"
  } while (($status -notin @("Succeeded", "Failed", "Cancelled", "DeadLettered")) -and (Get-Date) -lt $deadline)

  if ($status -ne "Succeeded") {
    throw "Notebook $Name finished with status=$status"
  }
  Ok "Notebook succeeded: $Name"
}

function Upsert-Pipeline {
  param(
    [string]$Name,
    [string]$JsonFile
  )

  $content = Get-Content $JsonFile -Raw -Encoding UTF8
  $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($content))
  $def = @{
    parts = @(
      @{
        path = "pipeline-content.json"
        payload = $b64
        payloadType = "InlineBase64"
      }
    )
  }

  $pipelines = Get-FabricCollection "/workspaces/$WorkspaceId/dataPipelines"
  $existing = $pipelines | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
  if ($existing) {
    Invoke-Fabric -Method "POST" -Path "/workspaces/$WorkspaceId/dataPipelines/$($existing.id)/updateDefinition" -Body @{ definition = $def } | Out-Null
    Ok "Pipeline updated: $Name"
    return $existing.id
  }

  $raw = Invoke-FabricRaw -Method "POST" -Path "/workspaces/$WorkspaceId/dataPipelines" -Body @{
    displayName = $Name
    definition = $def
  } -AllowedStatuses @(409)

  if ($raw.StatusCode -eq 409) {
    $pipelines = Get-FabricCollection "/workspaces/$WorkspaceId/dataPipelines"
    $existing = $pipelines | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
    if ($existing) {
      Invoke-Fabric -Method "POST" -Path "/workspaces/$WorkspaceId/dataPipelines/$($existing.id)/updateDefinition" -Body @{ definition = $def } | Out-Null
      Ok "Pipeline updated after conflict: $Name"
      return $existing.id
    }
    throw "Pipeline name conflict but pipeline not found in list: $Name"
  }

  if ($raw.StatusCode -eq 202) {
    $opId = Get-HeaderValue -Headers $raw.Headers -Name "x-ms-operation-id"
    if ([string]::IsNullOrWhiteSpace($opId)) {
      throw "Create pipeline accepted but no operation id returned: $Name"
    }
    Wait-FabricOperation -OperationId $opId
    $pipelines = Get-FabricCollection "/workspaces/$WorkspaceId/dataPipelines"
    $existing = $pipelines | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
    if ($existing) {
      Ok "Pipeline created: $Name (id=$($existing.id))"
      return $existing.id
    }
    throw "Pipeline operation succeeded but pipeline not found: $Name"
  }

  $created = $null
  if ($raw.Content) {
    try { $created = $raw.Content | ConvertFrom-Json } catch {}
  }
  if (-not $created -or -not $created.id) {
    throw "Failed to create pipeline: $Name (status=$($raw.StatusCode))"
  }
  Ok "Pipeline created: $Name (id=$($created.id))"
  return $created.id
}

Step "Ensuring lakehouse"
$LAKEHOUSE_ID = Ensure-Lakehouse -Name "lh_mdm"
if (-not $LAKEHOUSE_ID) { throw "Could not resolve lakehouse id" }

Step "Deploying notebooks"
$notebookIds = @{}
$nbDir = Join-Path $REPO_ROOT "fabric\notebooks"
foreach ($file in Get-ChildItem $nbDir -Filter "*.py") {
  $name = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
  $notebookIds[$name] = Upsert-Notebook `
    -Name $name `
    -PythonFile $file.FullName `
    -LakehouseId $LAKEHOUSE_ID `
    -LakehouseWorkspaceId $WorkspaceId
}

$seedFile = Join-Path $REPO_ROOT "fabric\lakehouse\seed\nb_seed_demo_data.py"
$notebookIds["nb_seed_demo_data"] = Upsert-Notebook `
  -Name "nb_seed_demo_data" `
  -PythonFile $seedFile `
  -LakehouseId $LAKEHOUSE_ID `
  -LakehouseWorkspaceId $WorkspaceId

Step "Running bootstrap notebooks"
Run-Notebook -NotebookId $notebookIds["nb_bootstrap_ddl"] -Name "nb_bootstrap_ddl"
Run-Notebook -NotebookId $notebookIds["nb_seed_mdm_config"] -Name "nb_seed_mdm_config"

if (-not $SkipSeed) {
  Step "Running demo seed notebook"
  Run-Notebook -NotebookId $notebookIds["nb_seed_demo_data"] -Name "nb_seed_demo_data"
} else {
  Warn "Demo seed skipped"
}

Step "Deploying pipelines"
$pipelineDir = Join-Path $REPO_ROOT "fabric\pipelines"
foreach ($file in Get-ChildItem $pipelineDir -Filter "*.json") {
  $name = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
  $null = Upsert-Pipeline -Name $name -JsonFile $file.FullName
}

Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Magenta
Write-Host "Fabric deploy completed" -ForegroundColor Magenta
Write-Host "WorkspaceId: $WorkspaceId" -ForegroundColor Yellow
Write-Host "Lakehouse: lh_mdm" -ForegroundColor Yellow
Write-Host ("Notebooks deployed: " + $notebookIds.Count) -ForegroundColor Yellow
Write-Host ("=" * 70) -ForegroundColor Magenta
