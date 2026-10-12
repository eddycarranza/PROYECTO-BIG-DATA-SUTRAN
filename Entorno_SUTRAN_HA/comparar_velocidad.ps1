$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
Invoke-Docker compose exec -T jupyter spark-submit --master yarn /project/scripts/comparar_velocidad_2021_2022.py

