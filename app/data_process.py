from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("DataProcess") \
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/data/labeled_data.csv", header=True, inferSchema=True)

# Delete unnecessary columns
df = df.drop('hate_speech', 'count', 'offensive_language', 'neither')

# Rename the 'class' column to 'sentiment' 
df = df.withColumnRenamed('class', 'sentiment')

df.show(10)

# 0 = positive, 1 and 2 = negative
negative_count = df.filter(df['sentiment'] == 2).count()
negative_count1 = df.filter(df['sentiment'] == 1).count()
positive_count = df.filter(df['sentiment'] == 0).count()
print("Nombre de messages negatifs : {}".format(negative_count + negative_count1))
print("Nombre de messages positifs : {}".format(positive_count))

# Add new dataset to add more positive messages to balance the dataset
df_positive = spark.read.csv("hdfs://namenode:9000/data/train.csv", header=True, inferSchema=True)

df_positive = df_positive.drop('textID','selected_text', 'Time of Tweet', 'Age of User', 'Country', 'Population -2020', 'Land Area (Km)', 'Density (P/Km)')

df_positive = df_positive.filter(~df_positive['sentiment'].isin(['neutral', 'negative']))

df_positive = df_positive.withColumn('sentiment', when(df_positive['sentiment'] == 'positive', 0).otherwise(df_positive['sentiment']))

df_positive = df_positive.withColumnRenamed('text', 'tweet')

positive_count2 = df_positive.filter(df_positive['sentiment'] == 0).count()
print("Nombre de messages positifs dans le nouveau dataset : {}".format(positive_count2))

df_positive.show(10)

for column in df.columns:
    if column not in df_positive.columns:
        df_positive = df_positive.withColumn(column, lit(None))
df_positive = df_positive.select(df.columns)
df_merged = df.unionByName(df_positive)

df_merged = df_merged.withColumn(
    'sentiment',
    when(col('sentiment') == 2, 1).otherwise(col('sentiment'))
)

# Counting messages
negative_count_merged = df_merged.filter(df_merged['sentiment'] == 1).count()
positive_count_merged = df_merged.filter(df_merged['sentiment'] == 0).count()


print("Nombre total de messages negatifs : {}".format(negative_count_merged))
print("Nombre total de messages positifs: {}".format(positive_count_merged))

df_merged.write.csv("hdfs://namenode:9000/data/processed_data.csv", header=True, mode="overwrite")

spark.stop()
