from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from decouple import config

BASE_DIR = config('BASE_DIR', default='')


def start_spark(app_name):
    builder = SparkSession.builder.master(master='local').appName(app_name)

    return builder


def extract_data(spark, path_way):
    data_frame = spark.read.load(path_way, format='json')

    return data_frame


def collect_n_max_value(n, data_frame):
    df_transform = data_frame.orderBy('learners_count', ascending=False).limit(5)

    return df_transform


spark_builder = start_spark('Stepik_processing')
spark_sess = spark_builder.getOrCreate()


path = BASE_DIR + '/data_collection/'
df = extract_data(spark_sess, path)
top5 = collect_n_max_value(5, df)

save_folder = BASE_DIR + '/data_top'
top5.write.format('json').mode("overwrite").save(save_folder)
top5.show()
