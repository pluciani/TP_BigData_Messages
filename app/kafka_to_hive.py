# -*- coding: utf-8 -*-
import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_date
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Réduction des logs
logging.getLogger("py4j").setLevel(logging.WARN)
SparkContext.setSystemProperty("spark.ui.showConsoleProgress", "false")

# Création de la session Spark avec Hive
spark = SparkSession.builder \
    .appName("KafkaTopicsToHive") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

# Schémas Kafka
topic1_schema = StructType() \
    .add("id", StringType()) \
    .add("name", StringType()) \
    .add("price", DoubleType())

topic2_schema = StructType() \
    .add("user_id", StringType()) \
    .add("product_id", StringType()) \
    .add("quantity", IntegerType()) \
    .add("total", DoubleType())

# Création des tables Hive si elles n'existent pas
spark.sql("""
CREATE TABLE IF NOT EXISTS products (
    id STRING,
    name STRING,
    price DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS purchases (
    user_id STRING,
    product_id STRING,
    quantity INT,
    total DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
""")

# Fonction de traitement de batch
def log_and_write_batch(df, epoch_id, table_name):
    print("\n📦 Nouveau batch reçu pour la table Hive: {}".format(table_name))
    df.show(truncate=False)
    df.withColumn("dt", current_date().cast("string")) \
      .write \
      .mode("append") \
      .format("hive") \
      .partitionBy("dt") \
      .saveAsTable(table_name)

# Fonction streaming Kafka → Hive
def stream_to_hive(topic, schema, table_name):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    return parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch: log_and_write_batch(df, epoch, table_name)) \
        .option("checkpointLocation", "/tmp/checkpoint_{}".format(table_name)) \
        .start()

# Lancer les streams
print("🔄 Streaming topic1 → Hive table products")
q1 = stream_to_hive("topic1", topic1_schema, "products")

print("🔄 Streaming topic2 → Hive table purchases")
q2 = stream_to_hive("topic2", topic2_schema, "purchases")

print("✅ Streaming en cours... (Ctrl+C pour arrêter)")
spark.streams.awaitAnyTermination()
