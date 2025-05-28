from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, date_format, when, lower, when, col, least, lit

# hdfs dfs -mkdir -p /data/music_streaming
# hdfs dfs -put -f /myhadoop/music_streaming/music_streaming.json /data/music_streaming/

# Créer une session Spark
spark = SparkSession.builder.appName("MusicStreamingData").getOrCreate()

# Charger les fichiers JSON depuis HDFS
df = spark.read.json("hdfs://namenode:9000/data/music_streaming/music_streaming.json", multiLine=True)

# Afficher le schéma
df.printSchema()

# Afficher quelques lignes de données
df.show(5, truncate=False)

# Arrêter la session Spark
# spark.stop()

# # Nettoyage et transformation des données
df_clean = (
    df
    .dropna(subset=["user_id", "song_id"])
    .filter(col("duration_ms") >= 30000)
    .withColumn("stream_time", to_timestamp(col("stream_time")))
)

# Afficher le schéma et quelques lignes après nettoyage
df_clean.printSchema()
df_clean.show(5, truncate=False)

df_transformed = (
    df_clean
    .withColumn("listening_hour", hour(col("stream_time")))
    .withColumn("day_of_week", date_format(col("stream_time"), "EEEE"))
    .withColumn(
        "device",
        when(lower(col("device")).like("%mobile%"), "mobile")
        .when(lower(col("device")).like("%phone%"), "mobile")
        .when(lower(col("device")).like("%tablet%"), "tablet")
        .when(lower(col("device")).like("%desktop%"), "desktop")
        .when(lower(col("device")).like("%pc%"), "desktop")
        .otherwise("other")
    )
    .withColumn("long_session", col("duration_ms") > 180000)
)

df_transformed.printSchema()
df_transformed.show(5, truncate=False)

# Écriture des données transformées au format Parquet, partitionnées par day_of_week
df_transformed.write.mode("overwrite").partitionBy("day_of_week").parquet("hdfs://namenode:9000/processed/music_streaming/")

# Écriture d'une copie des données au format CSV, sans partitionnement
df_transformed.write.mode("overwrite").option("header", True).csv("hdfs://namenode:9000/processed/music_streaming_csv/")

# Enregistrer le DataFrame transformé comme table temporaire
df_transformed.createOrReplaceTempView("music_streaming")

# # 1. Les 3 artistes les plus écoutés
top_artists = spark.sql("""
    SELECT artist, COUNT(*) as listen_count
    FROM music_streaming
    GROUP BY artist
    ORDER BY listen_count DESC
    LIMIT 3
""")
top_artists.show()

# 2. Nombre de sessions > 3 min par type de device
long_sessions_by_device = spark.sql("""
    SELECT device, COUNT(*) as long_session_count
    FROM music_streaming
    WHERE long_session = true
    GROUP BY device
""")
long_sessions_by_device.show()

# 3. Durée moyenne d’écoute par jour de la semaine
avg_duration_by_day = spark.sql("""
    SELECT day_of_week, AVG(duration_ms)/60000 as avg_duration_min
    FROM music_streaming
    GROUP BY day_of_week
    ORDER BY avg_duration_min DESC
""")
avg_duration_by_day.show()

df_updated = df_transformed.withColumn(
    "rating",
    when(
        col("device") == "desktop",
        least(col("rating") + 0.5, lit(5))
    ).otherwise(col("rating"))
)

df_updated.write.mode("overwrite").json("hdfs://namenode:9000/updated/music_streaming/")
