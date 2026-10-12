# Fuentes originales

Colocar directamente en esta carpeta los siete archivos que siguen. El pipeline requiere todos, incluso los usados como contexto o análisis separado. Los enlaces remiten a las entidades publicadoras; la entrega utiliza las versiones identificadas por SHA-256 en `Proyecto_SUTRAN/config/sources_hdfs.json`.

| ID | Nombre exacto del archivo | Filas originales | MB | Procedencia |
|---|---|---:|---:|---|
| F01 | Accidentes de tránsito en carreteras-2020-2021-Sutran.csv | 8,155 | 0.445 | [SUTRAN](https://www.datosabiertos.gob.pe/dataset/accidentes-de-tr%C3%A1nsito-en-carreteras) |
| F02 | Base CNM Datos Abiertos 2019-2021.csv | 160,018 | 13.400 | [SUTRAN](https://www.datosabiertos.gob.pe/dataset/excesos-de-velocidad-detectados-por-cinem%C3%B3metros) |
| F03 | Flujo vehicular registrado en peajes_2014-2026_I.csv | 11,532 | 1.619 | [Ministerio de Transportes y Comunicaciones](https://www.datosabiertos.gob.pe/dataset/flujo-vehicular-registrado-en-las-unidades-de-peaje-de-la-red-vial-nacional-2014-%E2%80%93-2026-i) |
| F04 | RELACION DE ESTACIONES DE PESAJE_GAT (2).csv | 36 | 0.007 | [SUTRAN](https://www.datosabiertos.gob.pe/dataset/relacion-de-unidades-desconcentradas%20-2021-sutran) |
| F05 | red_vial_nacional_dic18.zip | 4,546 | 40.255 | [Ministerio de Transportes y Comunicaciones](https://portal.mtc.gob.pe/estadisticas/files/shapefiles/red_vial_nacional_dic18.zip) |
| F06 | CINEMOMETROS_2022_ENERO A DICIEMBRE 2022_0.xlsx | 103,092 | 3.449 | [SUTRAN](https://www.datosabiertos.gob.pe/dataset/excesos-de-velocidad-detectados-por-cinem%C3%B3metros/resource/193c875e-4d5d-47a1-957e) |
| F07 | RELACION DE ESTACIONES DE PESAJE AL 2022_0.csv | 36 | 0.007 | [SUTRAN](https://www.datosabiertos.gob.pe/dataset/relaci%C3%B3n-de-estaciones-de-pesaje/resource/5e8c1956-10ed-4449-8dda-fcca02f9f505) |

Total de originales: 59,180 MB (1 MB = 1 000 000 bytes). Los tamaños y hashes internos se comparan en bytes para evitar redondeos durante la verificación.

Algunas páginas pueden ofrecer archivos actualizados o haber cambiado de dirección. Buscar el período y el nombre exactos en el catálogo de SUTRAN/MTC. Si no se consigue la misma versión, solicitar al equipo los originales de la entrega o preparar una actualización del inventario después de revisar el nuevo contenido.

No renombrar un archivo de otro período para hacerlo pasar por la versión del inventario. No modificar ni recodificar los originales antes de la ingesta. Las licencias y condiciones pertenecen a las entidades publicadoras y deben consultarse por fuente.

Los registros de accidentes cubren 2020 a septiembre de 2021; el histórico de velocidad, 2019–2021; la fuente adicional de velocidad corresponde a 2022. El ZIP vial es una referencia de diciembre de 2018. Los datos de 2022 no añaden etiquetas de accidentes para el entrenamiento actual.
