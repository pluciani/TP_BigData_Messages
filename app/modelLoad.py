from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("modelLoadToxicity").getOrCreate()

dfTest = spark.read.option("delimiter", ",").csv("hdfs://namenode:9000/data/processed_data_test.csv", header=True, inferSchema=True)

dfTest = dfTest.withColumn("label",dfTest["sentiment"].cast("integer"))

loaded_model = PipelineModel.load("hdfs://namenode:9000/data/toxicity_model")

predictions = loaded_model.transform(dfTest)
predictions.select("tweet", "label", "prediction", "probability").show(10, truncate=False)

evaluator = BinaryClassificationEvaluator(labelCol="label")


predictionAndLabels = predictions.select("prediction", "label").rdd.map(lambda row: (float(row['prediction']), float(row['label'])))

from pyspark.mllib.evaluation import MulticlassMetrics

metrics = MulticlassMetrics(predictionAndLabels)

# Overall accuracy
accuracy = metrics.accuracy

# For binary classification, use label 1.0 (positive class)
precision = metrics.precision(1.0)
recall = metrics.recall(1.0)
f1Score = metrics.fMeasure(1.0)

print(f"Accuracy: {accuracy}")
print(f"Precision: {precision}")
print(f"Recall: {recall}")
print(f"F1 Score: {f1Score}")