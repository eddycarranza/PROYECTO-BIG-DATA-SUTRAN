# Código y configuración

Las instrucciones completas están en el [README principal](../README.md).

- `scripts/`: etapas del pipeline y del modelo; los números corresponden al informe.
- `config/sources.json`: inventario para ingesta local; las rutas relativas se resuelven respecto a esta carpeta.
- `config/sources_hdfs.json`: inventario para Docker/HDFS, con entrada `/inputs`.
- `modelos/`: parámetros ya entrenados y su ficha.
- `ejemplos/`: entrada pequeña de prueba y resultados esperados.
- `documentation/` y `data_lake/`: salidas generadas, excluidas de Git.

Los resultados históricos se conservan en [docs/evidencias](../docs/evidencias/), separados del estado de una instalación nueva.
