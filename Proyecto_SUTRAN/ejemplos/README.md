# Ejemplo del modelo

`entrada_modelo.csv` contiene cinco unidades agregadas reales del conjunto de prueba, con mes objetivo julio de 2021 y características observadas en junio. No contiene datos de vehículos individuales.

`predicciones_esperadas.csv` conserva los resultados del modelo incluido para esas cinco filas. La prueba en un contenedor temporal coincidió con las predicciones originales del entrenamiento dentro de una tolerancia absoluta de 0,000000001.

Se puede comparar la salida de `probar_modelo.ps1` con este archivo. Los decimales son conteos estimados y no probabilidades. Las instrucciones de columnas y agregación están en [docs/MODELO.md](../../docs/MODELO.md).
