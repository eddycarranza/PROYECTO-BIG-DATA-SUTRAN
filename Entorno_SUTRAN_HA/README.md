# Entorno Docker del laboratorio

Seguir el [README principal](../README.md) para construir, procesar y entrenar. Las tareas PowerShell se ejecutan desde esta carpeta y utilizan `comun.ps1` para localizar Docker y verificar los códigos de salida.

`compose.yaml` define dos NameNodes, tres JournalNodes, tres ZooKeepers, dos ResourceManagers y tres trabajadores DataNode/NodeManager. Todos viven en una PC. `jupyter` aloja Python, Spark y el cliente Hadoop. Los originales se montan en `/inputs` en solo lectura; `Proyecto_SUTRAN/` se monta en `/project`.

El token se configura en `.env`, a partir de `.env.example`. No se versiona. Los scripts del contenedor necesitan finales de línea LF, definidos en `.gitattributes`. La imagen contiene las versiones fijadas en `requirements.txt`.

## Catálogo opcional

Hive Metastore 3.1.3 está en el perfil `catalogo`. No hace falta para el pipeline ni el modelo. Después de verificar Hadoop, puede iniciarse con `iniciar_catalogo.ps1`. Las pruebas son:

```powershell
docker compose exec -T jupyter spark-submit /workspace/probar_catalogo.py --crear
docker compose --profile catalogo restart metastore
docker compose exec -T jupyter python /opt/wait_tcp.py metastore 9083
docker compose exec -T jupyter spark-submit /workspace/probar_catalogo.py
```

`--crear` se usa una vez. La tabla de prueba contiene diez filas sintéticas y no participa en el análisis ni el entrenamiento. La primera consulta requiere Internet para resolver dependencias del cliente Hive. Este componente no forma parte de la validación del modelo incluida en la entrega.

Para pausar sin borrar datos, ejecutar `docker compose stop`. No ejecutar `down -v` para resolver errores: elimina volúmenes.
