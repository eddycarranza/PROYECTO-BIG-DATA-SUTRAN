param(
    [string]$Entrada = '',
    [string]$Salida = '/project/documentation/predicciones.csv'
)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
$predictionArguments = @('compose', 'exec', '-T', 'jupyter', 'spark-submit', '--master', 'yarn',
    '/project/scripts/predecir_modelo.py', '--salida', $Salida)
if ($Entrada) { $predictionArguments += @('--entrada', $Entrada) }
Invoke-Docker @predictionArguments
