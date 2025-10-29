from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# ==========================================================
# Script: batch_processing.py
# Descripción: Procesamiento batch del dataset "Fuentes Hídricas - Fusagasugá"
# ==========================================================

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("FuentesHidricasBatch") \
    .getOrCreate()

# Ruta del archivo CSV
ruta_csv = "/home/vboxuser/proyecto_fusagasuga/datasets/fuentes_hidricas_fusagasuga.csv"

# Cargar los datos
df = spark.read.option("header", "true").option("inferSchema", "true").csv(ruta_csv)

# Mostrar esquema y primeras filas
print("=== ESQUEMA DEL DATASET ===")
df.printSchema()
print("=== PRIMERAS FILAS ===")
df.show(5)

# Limpiar columnas con espacios y caracteres especiales
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().replace(" ", "_").replace("(", "").replace(")", ">

# Seleccionar columnas relevantes
columnas_relevantes = [
    "Nombre_de_la_Fuente_Hídrica",
    "Temperatura_C",
    "pH",
    "Oxígeno_Disuelto_mgL",
    "Conductividad_µScm",
    "Turbidez_NTU"
]

df = df.select(*[col(c) for c in columnas_relevantes if c in df.columns])

# Filtrar nulos
df = df.na.drop()

