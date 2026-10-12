$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
function Invoke-Compose { Invoke-Docker compose @args }
Invoke-Compose exec jupyter hdfs haadmin -getAllServiceState
Invoke-Compose exec jupyter yarn rmadmin -getAllServiceState
Invoke-Compose exec jupyter hdfs dfsadmin -report
Invoke-Compose exec jupyter yarn node -list
$SafeMode = Invoke-Docker compose exec -T jupyter hdfs dfsadmin -safemode get
if (($SafeMode -join ' ') -notmatch 'Safe mode is OFF') {
    throw 'HDFS no está listo para escribir. Revisar DataNodes y volver a verificar.'
}
Invoke-Compose exec jupyter spark-submit --master yarn /workspace/probar_entorno.py
