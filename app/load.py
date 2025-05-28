from pyspark.sql import SparkSession

# Creer une session Spark
spark = SparkSession.builder \
    .appName("Load JSON from HDFS") \
    .getOrCreate()

# Chemin HDFS
hdfs_path = "hdfs://namenode:9000/data/music_streaming/music_streaming.json"

# Charger les fichiers JSON
df = spark.read.option("multiline", "true").json(hdfs_path)

# Affiche le schema pour voir ce qui a ete lu
df.printSchema()

# Affiche les premieres lignes du DataFrame
df.show(truncate=False)

# Supprimer les enregistrements ou user_id ou song_id est manquant
df_clean = df.dropna(subset=["user_id", "song_id"])

df_clean = df_clean.filter(df_clean.duration_ms >= 30000)

print("df count", df.count())
print("df_clean count", df_clean.count())

# Arreter la session Spark
spark.stop()
