from task3_1 import load_iris_to_hdfs
import os

load_iris_to_hdfs()

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer
import sys

# Set environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Applied_Machine_Learning").getOrCreate()

schema = StructType([
    StructField("sepal_length", DoubleType()),
    StructField("sepal_width", DoubleType()),
    StructField("petal_length", DoubleType()),
    StructField("petal_width", DoubleType()),
    StructField("class", StringType())])

dataset = spark.read.csv("/tmp/iris.csv", schema = schema)

#features column created as a vector of input fields
assembler = VectorAssembler(inputCols = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'], outputCol='features')
output = assembler.transform(dataset)

train_raw = output.select('features','class')

#class column indexed with integer values
indexer = StringIndexer(inputCol="class", outputCol="class_indexed")
indexer_model = indexer.fit(train_raw)
indexer_labels = indexer_model.labels
train = indexer_model.transform(train_raw)

#Model definition and training
lr = LogisticRegression(labelCol="class_indexed")
lrn = lr.fit(train)

#Prediction data
pred_data = spark.createDataFrame(
[(5.1, 3.5, 1.4, 0.2),
(6.2, 3.4, 5.4, 2.3)],
["sepal_length", "sepal_width", "petal_length", "petal_width"])

trans_pred_data = assembler.transform(pred_data)

#model predictions
predictions = lrn.transform(trans_pred_data.select('features')).select('prediction')
predictions = [indexer_labels[int(i[0])] for i in predictions.collect()]

with open('../out/out_3_2.txt', 'w') as fp:
    fp.write(f'class\n{predictions[0]}\n{predictions[1]}')
    
