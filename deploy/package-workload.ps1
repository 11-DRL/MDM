# Package Fabric Workload as proper NuGet .nupkg (with Content_Types, _rels, nuspec, psmdcp).
# Uses dotnet pack via temp csproj -> produces NuGet-compliant package.
# Output: deploy\dist\<PackageId>.<version>.nupkg

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$wl       = Join-Path $repoRoot "fabric\workload"
$beDir    = Join-Path $wl "BE"
$feDir    = Join-Path $wl "FE"
$outDir   = Join-Path $repoRoot "deploy\dist"

function Step($m) { Write-Host "`n* $m" -ForegroundColor Cyan }
function OK($m)   { Write-Host "  OK $m" -ForegroundColor Green }
function Fail($m) { Write-Host "  FAIL $m" -ForegroundColor Red; exit 1 }

Step "Walidacja artefaktow..."
$required = @(
    (Join-Path $beDir "WorkloadManifest.xml"),
    (Join-Path $beDir "MDMStewardship.xml"),
    (Join-Path $feDir "Product.json"),
    (Join-Path $feDir "MDMStewardship.json"),
    (Join-Path $feDir "assets\mdm-icon-32.png"),
    (Join-Path $feDir "assets\mdm-icon-44.png")
)
foreach ($f in $required) { if (-not (Test-Path $f)) { Fail "Brak: $f" } }
OK "Pliki obecne"

$product = Get-Content (Join-Path $feDir "Product.json") -Raw | ConvertFrom-Json
$version = $product.version
$wlName  = $product.workloadName
OK "Workload: $wlName  v=$version"

if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir | Out-Null }
$outFile = Join-Path $outDir "$wlName.$version.nupkg"
if (Test-Path $outFile) { Remove-Item $outFile -Force }

# Stage w repo (unikamy short-name LUKASZ~1 w TEMP)
$staging = Join-Path $repoRoot ("deploy\build-" + [guid]::NewGuid().ToString("N").Substring(0,8))
if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
New-Item -ItemType Directory -Path $staging -Force | Out-Null
Copy-Item -Recurse $beDir (Join-Path $staging "BE")
Copy-Item -Recurse $feDir (Join-Path $staging "FE")

# Minimal SDK csproj with Pack items
$csproj = Join-Path $staging "$wlName.csproj"
@"
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <IsPackable>true</IsPackable>
    <IncludeBuildOutput>false</IncludeBuildOutput>
    <NoBuild>true</NoBuild>
    <GenerateAssemblyInfo>false</GenerateAssemblyInfo>
    <GenerateDocumentationFile>false</GenerateDocumentationFile>
    <NoDefaultExcludes>true</NoDefaultExcludes>
    <PackageId>$wlName</PackageId>
    <Version>$version</Version>
    <Authors>LOsteria Digital</Authors>
    <Description>MDM Stewardship Fabric workload.</Description>
    <PackageOutputPath>$outDir</PackageOutputPath>
    <SuppressDependenciesWhenPacking>true</SuppressDependenciesWhenPacking>
    <IncludeSymbols>false</IncludeSymbols>
  </PropertyGroup>
  <ItemGroup>
    <None Include="BE\**\*" Pack="true" PackagePath="BE" />
    <None Include="FE\**\*" Pack="true" PackagePath="FE" />
  </ItemGroup>
</Project>
"@ | Set-Content $csproj -Encoding UTF8

Step "dotnet pack..."
Push-Location $staging
try {
    & dotnet restore $csproj --nologo -v quiet 2>&1 | ForEach-Object { Write-Host "  $_" }
    if ($LASTEXITCODE -ne 0) { Fail "dotnet restore failed" }
    & dotnet pack $csproj --no-build --nologo -v minimal 2>&1 | ForEach-Object { Write-Host "  $_" }
    if ($LASTEXITCODE -ne 0) { Fail "dotnet pack failed" }
} finally { Pop-Location }

if (-not (Test-Path $outFile)) { Fail "Nie ma: $outFile" }

Step "Weryfikacja paczki:"
Add-Type -AssemblyName System.IO.Compression.FileSystem
$z = [System.IO.Compression.ZipFile]::OpenRead($outFile)
$z.Entries | ForEach-Object { Write-Host "  [$($_.FullName)]" }
$z.Dispose()

Remove-Item -Recurse -Force $staging -ErrorAction SilentlyContinue

$size = (Get-Item $outFile).Length
Write-Host ""
Write-Host ("=" * 64) -ForegroundColor Magenta
Write-Host "  GOTOWE - $([Math]::Round($size/1KB,1)) KB" -ForegroundColor Magenta
Write-Host "  Plik: $outFile" -ForegroundColor Yellow
Write-Host ("=" * 64) -ForegroundColor Magenta
