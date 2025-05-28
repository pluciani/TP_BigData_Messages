from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# Définir le schéma des messages JSON
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", StringType())
])

# Créer la session Spark
spark = SparkSession.builder \
    .appName("KafkaToCSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lire les messages du topic Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topic1") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir la valeur binaire en chaîne puis en JSON
json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Écrire les données dans un paquet Parquet dans HDFS
# query = json_df.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "hdfs://namenode:9000/data/temperature") \
#     .option("checkpointLocation", "hdfs://namenode:9000/data/temperature/checkpoint") \
#     .start()

query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


query.awaitTermination()
