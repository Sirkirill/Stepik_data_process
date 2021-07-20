import os
from pathlib import Path
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from decouple import config

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def start_spark(app_name):
    builder = SparkSession \
        .builder\
        .master(master='local')\
        .appName(app_name)\
        .getOrCreate()

    return builder


def extract_data(spark, path):
    data_frame = spark.read.load(path, format='json')

    return data_frame


def collect_n_max_value(n, data_frame):
    df_transform = data_frame.orderBy('learners_count', ascending=False).limit(n)

    return df_transform


if __name__ == "__main__":
    spark_sess = start_spark('Stepik_processing')
    df = extract_data(spark_sess, BASE_DIR + '/data_processing/')
    top5 = collect_n_max_value(5, df)

    save_folder = BASE_DIR + '/data_top'
    top5.write.format('json').mode("overwrite").save(save_folder)
    top5.show()
