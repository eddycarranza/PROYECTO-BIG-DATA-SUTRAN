$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
function Invoke-CatalogCompose { Invoke-Docker compose --profile catalogo @args }
Invoke-CatalogCompose exec -T jupyter hdfs dfs -mkdir -p /user/hive/warehouse
Invoke-CatalogCompose exec -T jupyter hdfs dfs -chmod 1777 /user/hive/warehouse
Invoke-CatalogCompose up -d metastore
Invoke-CatalogCompose exec -T jupyter python /opt/wait_tcp.py metastore 9083
Write-Output 'Metastore responde. Ejecutar las pruebas de creación y persistencia indicadas en README.'
