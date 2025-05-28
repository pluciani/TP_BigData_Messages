# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InitHiveTables") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .getOrCreate()

# Création des tables partitionnées par date
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

print("Tables Hive créées avec succès.")
spark.stop()
