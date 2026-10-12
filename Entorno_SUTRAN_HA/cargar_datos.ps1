$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
Invoke-Docker compose exec -T jupyter python /project/scripts/ingest.py --config /project/config/sources_hdfs.json --target hdfs

