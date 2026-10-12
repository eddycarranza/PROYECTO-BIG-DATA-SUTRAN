$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
$destination = Join-Path $PSScriptRoot '..\Proyecto_SUTRAN\documentation\seguridad_38'
New-Item -ItemType Directory -Force -Path $destination | Out-Null
$lines = @('Auditoria de solo lectura del laboratorio SUTRAN', ('Fecha local: ' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')), 'No se incluyen valores de credenciales.', '')
$compose = (Invoke-Docker compose config --format json) -join [Environment]::NewLine | ConvertFrom-Json
foreach ($service in $compose.services.psobject.Properties) {
    if ($service.Value.ports) { $lines += $service.Name + ': ' + ($service.Value.ports -join ', ') }
}
$lines += ''
$lines += Invoke-Docker compose exec -T jupyter sh -c 'for k in hadoop.security.authentication hadoop.security.authorization hadoop.rpc.protection dfs.permissions.enabled dfs.http.policy dfs.encrypt.data.transfer; do printf "%s=" "$k"; hdfs getconf -confKey "$k" 2>/dev/null; done'

$lines += Invoke-Docker compose exec -T jupyter hdfs dfs -ls /sutran

$lines | Set-Content -LiteralPath (Join-Path $destination 'auditoria_laboratorio.txt') -Encoding UTF8
Write-Output 'Auditoria guardada sin credenciales.'
