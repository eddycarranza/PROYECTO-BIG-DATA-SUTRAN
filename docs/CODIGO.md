# Organización del código

El proyecto se trabaja desde la carpeta `SUTRAN_GitHub`. El [README principal](../README.md) explica la instalación y los comandos de uso.

## Carpetas

| Carpeta | Contenido |
|---|---|
| `Entorno_SUTRAN_HA/` | Configuración de Docker y comandos de ejecución |
| `Proyecto_SUTRAN/scripts/` | Ingesta, procesamiento, controles, análisis y modelo |
| `Proyecto_SUTRAN/config/` | Inventario de fuentes y referencias a las últimas ejecuciones |
| `Proyecto_SUTRAN/modelos/` | Modelo entrenado incluido para la demostración |
| `Proyecto_SUTRAN/ejemplos/` | Entrada pequeña y predicciones esperadas |
| `Proyecto_SUTRAN/documentation/` | Resultados generados al ejecutar el proyecto |
| `docs/evidencias/` | Resultados históricos que respaldan el informe |
| `tests/` | Validación del CSV de entrada y de las métricas publicadas |

Los nombres de las etapas mantienen la numeración del informe para localizar el código durante la sustentación.

## Flujo de trabajo

| Etapa | Script principal | Comando desde `Entorno_SUTRAN_HA/` |
|---|---|---|
| Ingesta | `ingest.py` | `cargar_datos.ps1` |
| 3.3 Limpieza e integración | `pipeline_33.py` | `ejecutar_pipeline.ps1` |
| 3.4 Calidad | `control_calidad_34.py` | `verificar_calidad.ps1` |
| 3.5 Almacenamiento | `almacenamiento_35.py` | `preparar_almacenamiento.ps1` |
| 3.6 Optimización | `optimizacion_36.py` | `medir_optimizacion.ps1` |
| 3.7 Escalabilidad | `ejecutar_escalabilidad_37.py` | `probar_escalabilidad.ps1` |
| 3.8 Seguridad | Auditoría del entorno | `auditar_seguridad_38.ps1` |
| 4.1 Exploración | `analisis_exploratorio_41.py` | `ejecutar_eda.ps1` |
| Comparación de velocidad | `comparar_velocidad_2021_2022.py` | `comparar_velocidad.ps1` |
| 4.2 Preparación y entrenamiento | `preparar_modelo_42.py`, `entrenar_modelo_42.py` | `entrenar_modelo.ps1` |
| Demostración del modelo incluido | `predecir_modelo.py` | `probar_modelo.ps1` |

## Funciones compartidas

- `transformaciones.py` normaliza encabezados, texto y números. Lo utilizan el pipeline real y el de escalabilidad para aplicar las mismas reglas.
- `contrato_modelo.py` define las características numéricas y valida el CSV de predicción. La preparación del entrenamiento usa esa misma lista de características.
- `comun.ps1` localiza Docker y comprueba el resultado de sus comandos. Los lanzadores importan esta función antes de ejecutar cada etapa.

En `entrenar_modelo_42.py`, `build_pipeline` construye el estimador, `regression_metrics` calcula los errores y `department_rankings` compara los rankings. `main` coordina la ejecución y conserva los informes.

En `predecir_modelo.py`, `resolve_model` localiza el modelo y su ficha, `export_predictions` guarda el CSV y `main` ejecuta la predicción. Predecir no vuelve a entrenar el modelo.

## Cambiar el código

Para cambiar una regla de normalización, revisar `transformaciones.py`. Para modificar las entradas del modelo, revisar también su preparación, la ficha y el [contrato de entrada](MODELO.md); un modelo ya guardado sigue esperando las características con las que fue entrenado.

Las salidas de escalabilidad son datos artificialmente replicados. Se usan para medir el pipeline y se mantienen fuera del análisis exploratorio y del entrenamiento.

Desde la raíz del repositorio, las comprobaciones de código son:

```powershell
python -m pip install -r requirements-dev.txt
python -m ruff check .
python -m ruff format --check .
python -m unittest discover -s tests -v
```

Estas comprobaciones no sustituyen una ejecución del laboratorio Hadoop. La prueba del modelo incluido se ejecuta con `probar_modelo.ps1`, como se explica en el README.
