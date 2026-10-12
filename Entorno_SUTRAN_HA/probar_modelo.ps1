param([string]$Salida)
if ([string]::IsNullOrWhiteSpace($Salida)) {
    $version = (Get-Date -Format 'yyyyMMdd_HHmmss') + '_' + [guid]::NewGuid().ToString('N').Substring(0, 8)
    $Salida = "/project/documentation/predicciones_ejemplo_$version.csv"
}
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
Invoke-Docker compose run --rm --no-deps jupyter spark-submit --master 'local[2]' `
    --conf 'spark.hadoop.fs.defaultFS=file:///' `
    /project/scripts/predecir_modelo.py `
    --modelo 'file:///project/modelos/modelo_base' `
    --ficha /project/modelos/ficha_modelo.json `
    --entrada /project/ejemplos/entrada_modelo.csv --salida $Salida
