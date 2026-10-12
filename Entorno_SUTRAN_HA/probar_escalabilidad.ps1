$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
Invoke-Docker compose exec -T jupyter spark-submit --master yarn /project/scripts/preparar_escalabilidad_37.py
& python '..\Proyecto_SUTRAN\scripts\ejecutar_escalabilidad_37.py'

Invoke-Docker compose exec -T jupyter spark-submit --master yarn /project/scripts/validar_escalabilidad_37.py
