# SUTRAN: procesamiento de datos y modelo base

Proyecto del Grupo 5 del curso Big Data y Analítica de Datos de la UPAO. Trabajamos con datos públicos de accidentes, velocidad y flujo en peajes para preparar tablas limpias, explorar sus resultados y entrenar un primer modelo.

El modelo estima cuántos **registros de accidentes** podría tener un departamento el mes siguiente, usando la información del mes anterior. Es una regresión lineal regularizada de Spark MLlib. Este avance llega hasta el análisis exploratorio y el modelo base del punto 4.2.

## Carpetas del proyecto

| Carpeta | Contenido |
|---|---|
| `Entorno_SUTRAN_HA` | Configuración de Docker, Hadoop y Spark, y comandos para ejecutar el proyecto. |
| `Proyecto_SUTRAN/scripts` | Código de ingesta, limpieza, calidad, análisis y modelo. |
| `Proyecto_SUTRAN/config` | Inventario de fuentes y referencias a las últimas ejecuciones. |
| `Proyecto_SUTRAN/modelos` | Modelo entrenado incluido para la prueba. |
| `Proyecto_SUTRAN/ejemplos` | CSV de entrada y predicciones esperadas. |
| `Proyecto_SUTRAN/documentation` | Resultados que se generan al ejecutar los scripts. |
| `Datos_originales` | Lugar donde se colocan las siete fuentes originales. |
| `docs` | Explicación del código y evidencias del informe. |
| `tests` | Pruebas de las entradas del modelo y de las métricas. |

`Entorno_SUTRAN_HA` y `Proyecto_SUTRAN` son partes del mismo proyecto. Deben mantenerse juntas: los comandos de la primera carpeta ejecutan el código de la segunda.

## Qué necesitas

Las instrucciones usan Windows 10/11, **Visual Studio Code** y **Docker Desktop con contenedores Linux**. La virtualización debe estar habilitada y Docker debe estar abierto durante las pruebas.

Para probar el modelo guardado basta un contenedor. Para el laboratorio completo usamos una PC con 16 GB de RAM y unos 8 GB disponibles para Docker/WSL. Como referencia, reservar al menos 15 GB de disco; la prueba de escalabilidad 100x requiere espacio adicional.

La primera construcción necesita Internet. Docker instala Java 11, Python 3.11, Hadoop 3.4.3, Spark 3.5.9 y las bibliotecas del proyecto. No hace falta instalarlos por separado en Windows. Python 3.11 en Windows se necesita para las pruebas de código y el coordinador de escalabilidad.

## Abrirlo en Visual Studio Code

1. Si el proyecto está en un ZIP, extraerlo primero.
2. Abrir Visual Studio Code y seleccionar **Archivo → Abrir carpeta…**.
3. Elegir la carpeta que contiene este README, `Entorno_SUTRAN_HA` y `Proyecto_SUTRAN`.
4. Seleccionar **Terminal → Nueva terminal** y usar el perfil **PowerShell**. Se puede elegir desde la flecha junto al botón **+** del panel de terminal.

En la terminal integrada, comprobar que estamos en la carpeta correcta:

```powershell
Get-ChildItem README.md, Entorno_SUTRAN_HA, Proyecto_SUTRAN
```

Los siguientes bloques se copian en esa terminal. Ejecutar uno a la vez y esperar a que termine antes de continuar. La etiqueta `powershell` solo indica el tipo de comandos.

## Preparar el entorno

Con Docker Desktop abierto, entrar en la carpeta del entorno:

```powershell
cd .\Entorno_SUTRAN_HA
docker info
```

La primera vez, crear la configuración local y construir la imagen. Este bloque conserva un `.env` existente:

```powershell
if (-not (Test-Path -LiteralPath .env)) {
    $tokenLocal = [guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N')
    $configLocal = "JUPYTER_TOKEN=$tokenLocal`r`nCOMPOSE_PROJECT_NAME=sutran-ha`r`n"
    [IO.File]::WriteAllText((Join-Path (Get-Location) '.env'), $configLocal,
        [Text.UTF8Encoding]::new($false))
}
docker compose config --quiet
docker compose build jupyter
```

Si la configuración es correcta, `config --quiet` termina sin mostrar contenido. La construcción puede tardar por las descargas. `.env` contiene la configuración privada del equipo y no se comparte.

**Mantener la terminal en `Entorno_SUTRAN_HA` para los pasos siguientes.** Si se abre otra terminal en la raíz del proyecto, volver a ejecutar `cd .\Entorno_SUTRAN_HA`.

Si Windows bloquea un script por la política de ejecución, ejecutar una vez en esa terminal y volver a intentar el comando:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Este ajuste dura hasta que se cierre la terminal.

## Probar el modelo guardado

Esta es la forma más rápida de comprobar que el modelo funciona. Genera cinco predicciones sin descargar las fuentes ni repetir el entrenamiento:

```powershell
.\probar_modelo.ps1
```

Al terminar debe aparecer un mensaje parecido a este:

```text
PREDICCIONES_OK: 5 filas en /project/documentation/predicciones_ejemplo_<version>.csv
```

Cada prueba guarda un archivo nuevo en `Proyecto_SUTRAN/documentation`. Para ver el último resultado en la misma terminal:

```powershell
$archivo = Get-ChildItem '..\Proyecto_SUTRAN\documentation' -Filter 'predicciones_ejemplo_*.csv' |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
Import-Csv -LiteralPath $archivo.FullName | Format-Table -AutoSize
```

También se puede abrir desde el Explorador de Visual Studio Code, dentro de `Proyecto_SUTRAN → documentation`.

El ejemplo usa información de junio de 2021 para predecir julio. Los resultados aproximados son Amazonas 8,56; Áncash 29,98; Apurímac 10,30; Arequipa 40,50 y Ayacucho 8,21. Son conteos estimados y pueden tener decimales: **8,56 no significa 8,56 % de riesgo**.

Las predicciones esperadas están en `Proyecto_SUTRAN/ejemplos/predicciones_esperadas.csv`. Coincidir con ellas comprueba que se cargó el modelo correcto. Su precisión se evalúa por separado, comparando con los valores reales.

Para preparar otro CSV, consultar la [guía de entradas del modelo](docs/MODELO.md).

## Ejecutar el procesamiento completo

Esta parte utiliza HDFS y YARN. Requiere las siete fuentes originales, que no están incluidas en el repositorio. Sus nombres y procedencia están en [Datos_originales/README.md](Datos_originales/README.md).

Colocar los archivos directamente en `Datos_originales`, respetando el inventario. La ingesta comprueba su tamaño y SHA-256. Para reproducir las métricas del informe se necesitan las mismas versiones; si los portales entregan archivos distintos, hay que revisar esa actualización. Si las versiones originales ya no están disponibles, solicitarlas al equipo.

En la terminal de Visual Studio Code, desde `Entorno_SUTRAN_HA`, iniciar y verificar el laboratorio:

```powershell
.\iniciar.ps1
.\verificar.ps1
```

La verificación consulta Hadoop y ejecuta una prueba de Spark. Debe aparecer `PRUEBA_OK`. Después, ejecutar estas etapas en orden:

```powershell
.\cargar_datos.ps1
.\ejecutar_pipeline.ps1
.\verificar_calidad.ps1
.\preparar_almacenamiento.ps1
```

Estos pasos cargan los originales, limpian e integran las fuentes, verifican la calidad y guardan las tablas en Parquet. El control de calidad debe terminar con estado `passed` antes de continuar.

Los informes quedan en `Proyecto_SUTRAN/documentation`. Los archivos `ultimo_*.json` de `config` indican qué resultados usará la siguiente etapa. Se generan en cada instalación; no deben sustituirse por referencias de otra computadora.

## Entrenar el modelo base

Después de completar el procesamiento:

```powershell
.\entrenar_modelo.ps1
```

La primera ejecución prepara los datos y entrena el modelo. En `Proyecto_SUTRAN/documentation/modelo_42/entrenamiento` se crea una carpeta con `resultado_modelo.json` y `predicciones_prueba.csv`. `config/ultimo_modelo.json` identifica el modelo guardado en HDFS.

Si se cambió el procesamiento de los datos, regenerar el conjunto antes de entrenar:

```powershell
docker compose exec -T jupyter spark-submit --master yarn /project/scripts/preparar_modelo_42.py
.\entrenar_modelo.ps1
```

Para usar el último modelo entrenado:

```powershell
.\predecir_modelo.ps1
```

Este comando predice las filas del conjunto de prueba y guarda `Proyecto_SUTRAN/documentation/predicciones.csv`. Si ya existe, elegir otro nombre con `-Salida`. Para un CSV propio:

```powershell
.\predecir_modelo.ps1 `
    -Entrada /project/ejemplos/entrada_modelo.csv `
    -Salida /project/documentation/predicciones_nuevas.csv
```

`/project` corresponde a `Proyecto_SUTRAN` dentro del contenedor.

### Resultados de la primera evaluación

Entrenamos con 371 unidades departamento-mes, con meses objetivo de febrero de 2020 a junio de 2021. La prueba contiene 75 unidades de julio a septiembre de 2021. La imputación y la codificación del departamento se ajustaron solo con entrenamiento.

Como referencia usamos el conteo del mes anterior:

| Método | MAE | RMSE |
|---|---:|---:|
| Modelo base | 5,238 | 7,105 |
| Conteo del mes anterior | 5,253 | 6,919 |

Ambas métricas se expresan en número de registros y un valor menor indica menos error. El modelo mejora ligeramente el MAE, pero empeora el RMSE. Por ahora no muestra una ventaja consistente frente a la referencia. Los resultados completos están en [las evidencias del modelo](docs/evidencias/modelo/).

La evaluación pronostica un mes cada vez usando el mes anterior observado. No predice los tres meses simultáneamente desde junio.

## Análisis y pruebas del informe

Después del procesamiento, la calidad y el almacenamiento:

```powershell
.\ejecutar_eda.ps1
.\comparar_velocidad.ps1
.\medir_optimizacion.ps1
.\auditar_seguridad_38.ps1
```

Para la prueba de escalabilidad:

```powershell
.\probar_escalabilidad.ps1
```

Esta prueba requiere Python 3.11 en Windows, disponible con el comando `python`. Usa réplicas 1x, 10x y 100x para medir tiempos y recursos. Las réplicas quedan fuera del análisis y del entrenamiento. La velocidad de 2022 se analiza por separado: aquí no contamos con etiquetas de accidentes de ese año para entrenar.

## Consultar y detener el laboratorio

| Servicio | Dirección |
|---|---|
| HDFS | `http://localhost:9870` y `http://localhost:9871` |
| YARN | `http://localhost:8088` y `http://localhost:8089` |
| JupyterLab | `http://localhost:8888` |

Desde `Entorno_SUTRAN_HA`:

```powershell
docker compose ps
docker compose logs --tail 80 jupyter nn1 rm1
docker compose stop
```

`stop` detiene los servicios y conserva los datos. Para reanudarlos, usar `docker compose start`. El comando `docker compose down -v` elimina los volúmenes del laboratorio.

Todos los servicios funcionan en una misma PC. La replicación de HDFS no protege frente a la pérdida de ese equipo. Si se cambia de carpeta, detener la copia anterior antes de iniciar otra con el mismo `COMPOSE_PROJECT_NAME`.

## Si aparece un error

| Situación | Qué revisar |
|---|---|
| `docker` no se reconoce | Instalar Docker Desktop y volver a abrir Visual Studio Code. |
| No hay conexión con Docker | Abrir Docker Desktop y esperar a que `docker info` responda. |
| Falta `.env` o `JUPYTER_TOKEN` | Ejecutar la configuración inicial. Si `.env` existe, comprobar que el token tenga un valor. |
| El script está bloqueado | Aplicar el ajuste temporal indicado en «Preparar el entorno» y volver a ejecutar el script. |
| La salida ya existe | Elegir otro nombre con `-Salida`. `probar_modelo.ps1` lo hace automáticamente. |
| El CSV aparece en una sola columna en Excel | Importar desde **Datos → Desde texto/CSV** y elegir coma como separador. |
| El SHA-256 no coincide | Revisar la versión del original; no cambiar el hash para saltarse la validación. |
| Un puerto está ocupado | Comprobar si hay otra copia del laboratorio en ejecución. |
| Falta una ruta de HDFS indicada en `ultimo_*.json` | Ejecutar las etapas previas en esta instalación. |

## Qué falta en este avance

La integración y el modelo trabajan por departamento y mes. La incorporación de clima, feriados, emergencias y correspondencias espaciales por tramo queda pendiente. Los conteos representan registros publicados; las repeticiones sospechosas se conservan porque la fuente no permite identificar cada accidente de forma única.

El flujo de peajes y las detecciones de velocidad tienen cobertura parcial. La referencia vial de 2018 tampoco certifica que todas las vías mantengan la misma clasificación en 2021. Antes de usar el modelo operativamente habría que ampliar la validación y comprobar cuándo estuvieron disponibles las fuentes, además de la fecha de los eventos.

## Revisar el código

La [guía del código](docs/CODIGO.md) indica qué script corresponde a cada etapa. También están disponibles la [descripción del pipeline](docs/PIPELINE.md) y la [guía del modelo](docs/MODELO.md).

Para las comprobaciones de código, abrir una terminal en la raíz del proyecto, con Python 3.11 instalado en Windows. Si la terminal está en `Entorno_SUTRAN_HA`, volver con `cd ..`:

```powershell
python -m pip install -r requirements-dev.txt
python -m ruff check .
python -m ruff format --check .
python -m unittest discover -s tests -v
```

Estas pruebas revisan el código, las entradas del modelo y las métricas guardadas. La ejecución completa de Hadoop y Spark se verifica con los pasos del laboratorio.
