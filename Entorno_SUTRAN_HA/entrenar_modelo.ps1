$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot 'comun.ps1')
if (-not (Test-Path -LiteralPath '..\Proyecto_SUTRAN\config\ultimo_dataset_modelo.json')) {
    Write-Host 'Preparando datos históricos. Todavía no se entrena el modelo.'
    Invoke-Docker compose exec -T jupyter spark-submit --master yarn /project/scripts/preparar_modelo_42.py
}
Write-Host 'Iniciando el entrenamiento del punto 4.2 y la comparación con el mes anterior.'
$taskLog = Join-Path $PSScriptRoot ('entrenamiento_42_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '.log')
Invoke-Docker compose exec -T jupyter spark-submit --master yarn /project/scripts/entrenar_modelo_42.py --entrenar | Tee-Object -FilePath $taskLog
Write-Host 'Entrenamiento finalizado. Revisar resultado_modelo.json y predicciones_prueba.csv en Proyecto_SUTRAN/documentation/modelo_42/entrenamiento.'
