from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("TextClassification").getOrCreate()

dfTrain = spark.read.option("delimiter", ",").csv("hdfs://namenode:9000/data/processed_data_train.csv", header=True, inferSchema=True)

# print(df.show(5))

dfTrain = dfTrain.withColumn("label",dfTrain["sentiment"].cast("integer"))


print(dfTrain.show(5))


dfTest = spark.read.option("delimiter", ",").csv("hdfs://namenode:9000/data/processed_data_test.csv", header=True, inferSchema=True)

# print(df.show(5))

dfTest = dfTest.withColumn("label",dfTest["sentiment"].cast("integer"))


print(dfTest.show(5))


tokenizer = Tokenizer(inputCol="tweet", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
idf = IDF(inputCol="rawFeatures", outputCol="features")

lr = LogisticRegression(featuresCol="features", labelCol="label")

pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])

model = pipeline.fit(dfTrain)

predictions = model.transform(dfTest)
predictions.select("tweet", "label", "prediction", "probability").show(10, truncate=False)

evaluator = BinaryClassificationEvaluator(labelCol="label")
accuracy = evaluator.evaluate(predictions)
print(f"Accuracy {accuracy}")

model.write().overwrite().save("hdfs://namenode:9000/data/toxicity_model")