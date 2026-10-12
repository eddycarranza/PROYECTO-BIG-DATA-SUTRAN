# Etapas del proyecto

Los números en los nombres de scripts corresponden a la estructura del informe del curso. Se conservaron para facilitar la revisión de cada evidencia.

| Etapa | Script principal | Salida |
|---|---|---|
| 3.2 Ingesta | ingest.py | Originales en Raw y manifiesto con SHA-256 |
| 3.3 Limpieza e integración | pipeline_33.py | Tablas normalizadas, agregadas y cuarentena |
| 3.4 Calidad | control_calidad_34.py | Reglas, completitud, rechazos y advertencias |
| 3.5 Almacenamiento | almacenamiento_35.py | Parquet/Snappy y particiones anuales |
| 3.6 Optimización | optimizacion_36.py | Comparación de consultas antes/después |
| 3.7 Escalabilidad | preparar_escalabilidad_37.py, ejecutar_escalabilidad_37.py, pipeline_escalabilidad_37.py, validar_escalabilidad_37.py | Réplicas de prueba, tiempos, recursos y comprobaciones |
| 4.1 Análisis | analisis_exploratorio_41.py, comparar_velocidad_2021_2022.py | Gráficos y agregados para interpretación |
| 4.2 Conjunto del modelo | preparar_modelo_42.py | Pares de meses consecutivos con separación temporal |
| 4.2 Entrenamiento | entrenar_modelo_42.py | PipelineModel, métricas y predicciones de prueba |
| Predicción | predecir_modelo.py | Conteos estimados con parámetros guardados |

## Organización del almacenamiento

La raíz del laboratorio es `hdfs://sutran/sutran`. `sutran` es el nameservice configurado en Hadoop; no es un servidor público.

- `raw`: originales sin alterar, por fuente, fecha y hash de versión.
- `standardized`: fechas, nombres y medidas tipados.
- `curated`: tablas integradas con claves únicas por unidad y mes.
- `serving`: indicadores preparados para análisis.
- `quarantine`: filas rechazadas y su motivo.
- `metadata`: manifiestos e informes de ejecución.
- `modeling_42`: conjuntos, modelos y predicciones.
- `benchmark_37`: entradas y salidas artificialmente replicadas para medir el pipeline.

Los scripts conservan las versiones anteriores. Los archivos `ultimo_*.json` señalan la última ejecución válida de cada etapa; no certifican por sí solos que las rutas existan en otro clúster.

## Decisiones sobre las fuentes

La ingesta compara tamaño y SHA-256 antes y después de copiar. La limpieza acepta 287 330 de las 287 415 filas originales, y aparta 85. Las repeticiones exactas de accidentes quedan marcadas porque no hay identificador que permita establecer si son eventos distintos. En flujo, las claves peaje-mes repetidas se separan para evitar multiplicar registros al unir.

La integración de 2021 se realiza después de agregar cada fuente por departamento y mes. Las medidas sin correspondencia quedan nulas, acompañadas por indicadores de cobertura. No se unen eventos individuales entre sí ni se inventan observaciones para completar meses.

La referencia vial de 2018 es histórica. La verificación de enero–septiembre de 2021 encontró 3 993 accidentes con coincidencia de ruta y departamento y 145 sin coincidencia. Esos 145 requieren revisión y permanecen en la tabla aceptada: no encontrar una ruta en 2018 no demuestra que un registro de 2021 sea inválido.

## Evidencias incluidas

`docs/evidencias/` contiene informes y agregados de ejecuciones del equipo. Son resultados históricos para revisar el trabajo, separados de las salidas de una instalación nueva. Sus URI HDFS describen aquel laboratorio y no son enlaces de descarga.

La optimización midió reducciones pequeñas en medianas, con rangos superpuestos. La prueba 100x procesó 28 741 500 filas replicadas correctamente; no demuestra por sí sola que el volumen real necesite una plataforma distribuida ni establece el punto donde una PC deje de alcanzar.

El catálogo Hive y las pruebas de conmutación no son requisitos del recorrido del modelo. La configuración de seguridad es de laboratorio: acceso local, fuentes montadas en solo lectura y comprobaciones de integridad. Kerberos, TLS y distribución entre máquinas distintas requieren una implementación adicional para producción.
