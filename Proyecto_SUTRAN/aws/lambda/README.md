# Disparador de Glue para lotes de accidentes

Amazon S3 notifica a la función `sutran-disparador-accidentes` cuando se crea
un archivo con prefijo `entradas/accidentes/` y sufijo `.csv`. La función inicia
el trabajo `sutran-pipeline-completo` en Ohio (`us-east-2`) y registra el nombre
del objeto junto con el identificador de ejecución de Glue.

| Archivo | Función |
| --- | --- |
| `lambda_function.py` | Recibe el evento de S3 y solicita la ejecución de Glue. |
| `configurar_sutran_lambda.py` | Configura el rol de ejecución, la función y la notificación del bucket. |

## Instalar desde CloudShell

El instalador usa la sesión autenticada de CloudShell. Requiere acceso al
bucket y al trabajo de Glue, y permisos para configurar IAM, Lambda y las
notificaciones de S3. Para una instalación nueva, pon los dos archivos Python
en la misma carpeta de CloudShell y ejecuta desde esa carpeta.

```bash
python3 -m zipfile -c sutran_disparador_lambda.zip lambda_function.py
python3 configurar_sutran_lambda.py
```

La señal de que la regla quedó configurada es `DISPARADOR_SUTRAN_OK`. Para
ejecutar el instalador contra la función ya instalada, usa el ZIP exacto con
el que se desplegó. El instalador verifica su huella antes de reutilizarla.

## Prueba de arranque automático

En el proyecto SUTRAN se cargó un CSV con solo el encabezado en
`entradas/accidentes/`. CloudWatch registró `submitted`, la clave del objeto
y el identificador de la corrida lanzada por Lambda. El mismo identificador
apareció en Glue con estado `SUCCEEDED` y error `null`.

Para comprobar la ejecución en tu cuenta, busca el evento en el registro de
Lambda y copia su `job_run_id`.

```bash
aws logs tail /aws/lambda/sutran-disparador-accidentes --since 1h --region us-east-2
aws glue get-job-run --job-name sutran-pipeline-completo --run-id ID_DE_LA_CORRIDA \
  --region us-east-2 --query 'JobRun.{Id:Id,Estado:JobRunState,Error:ErrorMessage}'
```

La prueba verifica que la carga de S3 produce una ejecución exitosa de Glue.
El CSV de encabezado tiene cero registros. La comprobación del cambio de un
indicador en Power BI se hace con un lote que tenga registros válidos.

Referencias oficiales: [Eventos de S3 en Lambda](https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html) y
[Consulta de una corrida de Glue](https://docs.aws.amazon.com/cli/latest/reference/glue/get-job-run.html).