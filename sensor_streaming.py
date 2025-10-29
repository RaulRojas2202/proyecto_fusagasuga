from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StringType, DoubleType

# ==========================================================
# Script: streaming_processing.py
# Descripción: Consume datos en tiempo real desde Kafka y analiza pH y temperatura
# ==========================================================

spark = SparkSession.builder \
    .appName("FuentesHidricasStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================
# Schema del JSON enviado por el producer
# =====================
schema = StructType() \
    .add("ID", StringType()) \
    .add("Municipio", StringType()) \
    .add("Nombre del Punto", StringType()) \
    .add("Fuente Hídrica", StringType()) \
    .add("Temperatura", StringType()) \
    .add("pH", StringType()) \
    .add("Fecha", StringType())

# =====================
# Leer datos desde Kafka
# =====================
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_streaming") \
    .option("startingOffsets", "latest") \
    .load()

# =====================
# Parsear JSON y seleccionar columnas
# =====================
values_df = stream_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# =====================
# Convertir columnas numéricas
# =====================
values_df = values_df.withColumn("Temperatura", col("Temperatura").cast(DoubleType()))
values_df = values_df.withColumn("pH", col("pH").cast(DoubleType()))

# =====================
# Calcular promedio de pH y Temperatura cada 10 segundos
# =====================
promedios = values_df.groupBy().agg(
    avg("Temperatura").alias("Promedio_Temperatura"),
    avg("pH").alias("Promedio_pH")
)

# =====================
# Mostrar resultados en consola
# =====================
query = promedios.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()
