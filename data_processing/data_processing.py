
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def start_spark(app_name):
    builder = SparkSession.builder.master(master='local').appName(app_name)

    return builder


def extract_data(spark):
    path = 'file:///Users/kbarkalov/PycharmProjects/Stepik_data_process/data_processing/data.json'
    data_frame = spark.read.load(path, format='json')

    return data_frame


def collect_n_max_value(n, data_frame):
    df_transform = data_frame.orderBy('learners_count', ascending=False).limit(5)

    return df_transform


spark_builder = start_spark('Stepik_processing')
spark_sess = spark_builder.getOrCreate()

df = extract_data(spark_sess)
top5 = collect_n_max_value(5, df)

save_folder = 'file:///Users/kbarkalov/PycharmProjects/Stepik_data_process/data_processing/data_top'
top5.write.format('json').save(save_folder)
top5.show()
