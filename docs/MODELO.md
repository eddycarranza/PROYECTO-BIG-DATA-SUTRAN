# Uso del modelo base

El estimador es `pyspark.ml.regression.LinearRegression`, con `regParam=0.1`, `elasticNetParam=0` y hasta 100 iteraciones. El pipeline contiene imputación por mediana, codificación del departamento y ensamblado del vector de características. No es una red neuronal y no se elige un número de capas.

El resultado es el número esperado de registros de accidentes de un departamento en el mes objetivo. Se limita a cero cuando la regresión devuelve un valor negativo, sin redondearlo. Una predicción de 8,4 es un conteo estimado; no representa 8,4 % de riesgo.

## Entrada de un CSV propio

El CSV debe estar en UTF-8, separado por comas, con punto decimal y una fila por departamento y mes objetivo. El orden de las columnas no importa. El ejemplo incluido conserva cinco unidades reales del conjunto de prueba. Los nombres de departamentos se normalizan a mayúsculas sin tildes; se rechazan departamentos ausentes del entrenamiento.

| Columna | Cómo se obtiene |
|---|---|
| departamento | Nombre del departamento |
| accidentes_previos | Conteo de registros del mes calendario inmediatamente anterior |
| fallecidos_previos | Suma de fallecidos informados en ese mes |
| heridos_previos | Suma de heridos informados en ese mes |
| exceso_previo_kmh | Promedio del exceso de velocidad observado en ese mes, en km/h |
| log_detecciones_previas | `ln(1 + número de detecciones)` del mes anterior |
| log_pasos_peajes_previos | `ln(1 + total de pasos en peajes)` del mes anterior |
| cobertura_velocidad_previa | 1 si hay detecciones observadas; 0 si faltan |
| cobertura_peajes_previa | 1 si hay flujo observado; 0 si falta |
| mes_seno | `sin(2 × pi × mes_objetivo / 12)` |
| mes_coseno | `cos(2 × pi × mes_objetivo / 12)` |

El número del mes objetivo está entre 1 y 12. Por ejemplo, para predecir julio se utilizan mediciones de junio, `mes_seno = sin(2*pi*7/12)` y `mes_coseno = cos(2*pi*7/12)`.

Si no hay fuente de velocidad o peajes para esa unidad, dejar vacía la medida y su logaritmo, y marcar cobertura 0. No sustituir ausencias por ceros: el modelo carga las medianas aprendidas durante el entrenamiento. Los fallecidos y heridos pueden quedar vacíos cuando no se informaron. El conteo previo de accidentes, las coberturas y el calendario deben estar completos.

La interfaz acepta hasta 10 000 filas por CSV y conserva todas sus filas. Antes de predecir rechaza columnas faltantes, valores negativos, números no finitos y coberturas incompatibles. La agregación, el período y la disponibilidad histórica de la información deben revisarse antes de armar un CSV nuevo.

## Ejecutar con los parámetros incluidos

Después de configurar `.env` y construir la imagen, desde `Entorno_SUTRAN_HA/`:

```powershell
docker compose run --rm --no-deps jupyter spark-submit --master 'local[2]' `
    --conf 'spark.hadoop.fs.defaultFS=file:///' `
    /project/scripts/predecir_modelo.py `
    --modelo file:///project/modelos/modelo_base `
    --ficha /project/modelos/ficha_modelo.json `
    --entrada /project/ejemplos/entrada_modelo.csv `
    --salida /project/documentation/predicciones_propias.csv
```

Los argumentos son rutas **dentro del contenedor**: `/project` corresponde a `Proyecto_SUTRAN/` en Windows. Para reemplazar la entrada, guardar otro CSV en `Proyecto_SUTRAN/ejemplos/` y cambiar `--entrada`. La salida debe tener un nombre que todavía no exista.

La carpeta `modelos/modelo_base/` contiene el `PipelineModel` exportado del entrenamiento documentado en `docs/evidencias/modelo/resultado_modelo.json`. Mantener sus subcarpetas `metadata` y `stages` completas; un CSV con coeficientes no reemplaza ese artefacto. `ficha_modelo.json` identifica los departamentos, períodos y métricas de la misma ejecución.

## Evaluación

El comparador repite el conteo del mes anterior. Sirve para comprobar si agregar velocidad, flujo, calendario y departamento aporta frente a una regla simple.

Entrenamiento: 371 unidades con objetivo febrero de 2020–junio de 2021. Prueba: 75 unidades con objetivo julio–septiembre de 2021. Solo se incluyen pares de meses consecutivos con accidentes observados en ambos. La evaluación está condicionada por esa cobertura; una unidad sin filas no se considera automáticamente una unidad sin accidentes.

La métrica principal es MAE y la secundaria RMSE, ambas en número de registros. El modelo tiene MAE 5,2379 y RMSE 7,1051; el comparador tiene MAE 5,2533 y RMSE 6,9186. El modelo mejora ligeramente el primer indicador y empeora el segundo. No se ajustaron hiperparámetros con estos 75 ejemplos.

La prueba corresponde a un pronóstico de un mes en cada origen. Predecir agosto utiliza julio observado; no es una predicción simultánea de los tres meses hecha en junio. Esta evaluación retrospectiva tampoco verifica la latencia de publicación de las fuentes.
