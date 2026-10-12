$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
$manifestFile = Get-ChildItem -LiteralPath '../Proyecto_SUTRAN/data_lake/metadata' -Filter 'manifest_*.json' | Sort-Object LastWriteTime -Descending | Where-Object { $m = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json; $m.status -eq 'success' -and $m.target -eq 'hdfs' } | Select-Object -First 1
if (-not $manifestFile) { throw 'Primero ejecutar cargar_datos.ps1.' }
$manifestPath = '/project/data_lake/metadata/' + $manifestFile.Name
Invoke-Docker compose exec -T jupyter spark-submit --master yarn /project/scripts/pipeline_33.py --manifest $manifestPath

