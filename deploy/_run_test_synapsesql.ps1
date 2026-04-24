param([string]$WS="7ce7e85b-84c8-41a3-ad49-0a1bdbe74bdf")
$LH="660f2a50-5225-4efe-b49d-91a5206f3e67"
$WH="0ed57608-2b56-4d22-9080-2da9c3c03396"
$AUTH=az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$H=@{Authorization="Bearer $AUTH";"Content-Type"="application/json"}
$py = Get-Content "c:\Users\LukaszLelwic\MDM\MDM\fabric\notebooks\nb_test_synapsesql.py" -Raw
$py = $py -replace "`r`n","`n"

$meta = @"
{
  "kernel_info": { "name": "synapse_pyspark" },
  "dependencies": {
    "lakehouse": { "default_lakehouse": "$LH", "default_lakehouse_name": "lh_mdm", "default_lakehouse_workspace_id": "$WS", "known_lakehouses": [{"id":"$LH"}] },
    "warehouse": { "default_warehouse": "$WH", "known_warehouses": [{"id":"$WH","type":"Warehouse"}] }
  }
}
"@

$sb = [System.Text.StringBuilder]::new()
[void]$sb.AppendLine("# Fabric notebook source")
[void]$sb.AppendLine("")
[void]$sb.AppendLine("# METADATA ********************")
[void]$sb.AppendLine("")
foreach ($l in ($meta -split "`n")) { [void]$sb.AppendLine("# META $l") }

$cells = [regex]::Split($py, "# CELL \d+[^\n]*\n")
for ($i=1; $i -lt $cells.Count; $i++) {
  $body = $cells[$i].Trim()
  [void]$sb.AppendLine("")
  [void]$sb.AppendLine("# CELL ********************")
  [void]$sb.AppendLine("")
  [void]$sb.AppendLine($body)
  [void]$sb.AppendLine("")
  [void]$sb.AppendLine("# METADATA ********************")
  [void]$sb.AppendLine("")
  [void]$sb.AppendLine('# META {"language":"python","language_group":"synapse_pyspark"}')
}

$src = $sb.ToString() -replace "`r",""
$b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($src))

$body = @{
  displayName = "nb_test_synapsesql"
  definition = @{ format="fabricGitSource"; parts=@(@{path="notebook-content.py"; payload=$b64; payloadType="InlineBase64"}) }
} | ConvertTo-Json -Depth 10

# Check if exists
$existing = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WS/notebooks" -Headers $H).value | Where-Object { $_.displayName -eq "nb_test_synapsesql" }
if ($existing) {
  Write-Host "Updating existing notebook $($existing.id)"
  $upd = @{ definition = @{ format="fabricGitSource"; parts=@(@{path="notebook-content.py"; payload=$b64; payloadType="InlineBase64"}) } } | ConvertTo-Json -Depth 10
  $resp = Invoke-WebRequest -Method POST -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WS/notebooks/$($existing.id)/updateDefinition" -Headers $H -Body $upd -UseBasicParsing
  Write-Host "updateDefinition status: $($resp.StatusCode)"
  $NB_ID = $existing.id
} else {
  $resp = Invoke-WebRequest -Method POST -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WS/notebooks" -Headers $H -Body $body -UseBasicParsing
  Write-Host "create status: $($resp.StatusCode)"
  if ($resp.StatusCode -eq 202) {
    $opId = $resp.Headers.'x-ms-operation-id' | Select-Object -First 1
    do { Start-Sleep 4; $op = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId" -Headers $H; Write-Host "  op=$($op.status)" } while ($op.status -notin @("Succeeded","Failed"))
    $nbs = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WS/notebooks" -Headers $H).value
    $NB_ID = ($nbs | Where-Object { $_.displayName -eq "nb_test_synapsesql" }).id
  } else {
    $NB_ID = ($resp.Content | ConvertFrom-Json).id
  }
}
Write-Host "NB_ID=$NB_ID"

# Run it
$resp = Invoke-WebRequest -Method POST -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WS/items/$NB_ID/jobs/instances?jobType=RunNotebook" -Headers $H -UseBasicParsing
$loc = $resp.Headers.Location | Select-Object -First 1
Write-Host "Job: $loc"
do { Start-Sleep 10; $job = Invoke-RestMethod -Method GET -Uri $loc -Headers $H; Write-Host "  status=$($job.status)" } while ($job.status -in @("NotStarted","InProgress"))
$job | ConvertTo-Json -Depth 6
