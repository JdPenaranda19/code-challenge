from pyspark.sql import SparkSession
import csv
import requests
import os
import sys

# Set environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def get_groceries_rdd():
    spark = SparkSession.builder.appName('code_challenge').getOrCreate()
    sc=spark.sparkContext

    url = "https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv"

    r = requests.get(url)
    text = r.iter_lines()
    csv_split_lines = []

    for row in text:
        csv_split_lines.append(row.decode('utf-8').split(','))

    groceries_rdd = sc.parallelize(csv_split_lines)

    return spark, groceries_rdd


if __name__ == '__main__': 
    
    spark, groceries_rdd = get_groceries_rdd()
    print(groceries_rdd.take(5))