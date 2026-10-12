$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
function Invoke-Compose {
    Invoke-Docker compose @args
}
function Wait-Tcp($HostName, $Port) {
    Invoke-Compose run --rm --no-deps jupyter python /opt/wait_tcp.py $HostName $Port
}
Invoke-Compose up -d zk1 zk2 zk3 jn1 jn2 jn3
Wait-Tcp zk1 2181
Wait-Tcp zk2 2181
Wait-Tcp zk3 2181
Wait-Tcp jn1 8485
Wait-Tcp jn2 8485
Wait-Tcp jn3 8485
Invoke-Compose run --rm --no-deps nn1 init-nn
Invoke-Compose run --rm --no-deps nn1 init-zk
Invoke-Compose up -d nn1 zkfc1
Wait-Tcp nn1 8020
Invoke-Compose run --rm --no-deps nn2 init-standby
Invoke-Compose up -d nn2 zkfc1 zkfc2 rm1 rm2 dn1 dn2 dn3 nm1 nm2 nm3 jupyter
Invoke-Compose ps
Write-Output 'Servicios iniciados. Falta verificar nodos, estados HA y la prueba Spark.'
