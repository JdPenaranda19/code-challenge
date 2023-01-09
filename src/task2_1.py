from pyspark.sql import SparkSession
import os, shutil

path = 'LearningSparkV2/mlflow-project-example/data/sf-airbnb-clean.parquet'

#Clone LearningSparkV2 with sparse mode to get only the parquet file required
def clone_file_if_not_exists():
    if not os.path.exists(path):
        os.system('''git clone \
                  --depth 1  \
                  --filter=blob:none  \
                  --sparse \
                  https://github.com/databricks/LearningSparkV2.git \
                ;

                cd LearningSparkV2
                git sparse-checkout set mlflow-project-example/data/sf-airbnb-clean.parquet/''')
        
#Get spark session and airbnb dataframe
def get_airbnb_df():
    
    clone_file_if_not_exists()
    
    spark = SparkSession.builder.appName('code_challenge').getOrCreate()
    df = spark.read.parquet("file:///" + path)
    
    shutil.rmtree('LearningSparkV2')
    
    return spark, df


if __name__ == '__main__': 
    
    spark, airbnb_df = get_airbnb_df()
    print(airbnb_df.take(1))
    