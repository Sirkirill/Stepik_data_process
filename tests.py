import json

import pytest
import requests
import pandas as pd
import matplotlib.pyplot as plt
from decouple import config
from pyspark.sql import SparkSession
from matplotlib.testing.decorators import image_comparison
from chispa.dataframe_comparer import *
from pandas.testing import assert_frame_equal

from data_processing.data_processing import start_spark, collect_n_max_value, extract_data
from data_visualization.data_visualization import pandas_df, visualize

BASE_DIR = config('BASE_DIR', default='')


def test_stepik_connection():
    url = 'https://stepik.org:443/api/courses'
    response = requests.get(url)

    assert response.status_code == 200


def test_spider():
    url = 'https://stepik.org:443/api/courses?page=1'
    response = requests.get(url)
    results = response.json()
    next_page = results['meta']['page'] + 1

    assert next_page == 2


@pytest.fixture(scope='session')
def spark_session(request):
    spark = start_spark('test')

    request.addfinalizer(lambda: spark.stop())

    return spark


def test_collect_n_max_value(spark_session):
    source_data = [
        (1, 12, "False"),
        (2, 3, "False"),
        (3, 45, "True"),
        (4, 0, "False"),
        (5, 33, "True")
    ]
    excepted_data = [
        (3, 45, "True"),
        (5, 33, "True"),
        (1, 12, "False")
    ]

    excepted_df = spark_session.createDataFrame(excepted_data, ["id", "learners_count", "is_popular"])
    source_df = spark_session.createDataFrame(source_data, ["id", "learners_count", "is_popular"])
    actual_df = collect_n_max_value(3, source_df)

    assert actual_df.count() == 3
    assert_df_equality(actual_df, excepted_df)


def test_collect_data(spark_session):
    path = BASE_DIR + '/test_data/test.json'
    actual_df = extract_data(spark_session, path)

    excepted_data = [
        (99297, False, 4),
        (99379, False, 3),
        (99330, False, 2),
        (99311, False, 2),
        (99365, False, 1),
    ]
    excepted_df = spark_session.createDataFrame(excepted_data, ["id", "is_popular", "learners_count"])
    assert actual_df.count() == 5
    assert_df_equality(excepted_df, actual_df)


def test_pandas_df():
    path = BASE_DIR + "/test_data/test_top_data/"
    df = pandas_df(path)
    id = [99297, 99379, 99330, 99311, 99365]
    is_popular = [False, False, False, False, False]
    learners_count = [4, 3, 2, 2, 1]
    excepted_df = pd.DataFrame({"id": id, "is_popular": is_popular, "learners_count": learners_count})
    assert_frame_equal(df, excepted_df)


@image_comparison(baseline_images=['line_dashes'], remove_text=True,
                  extensions=['png'])
def test_line_dashes():
    path = BASE_DIR + '/test_data/test_top_data/'
    visualize(path)
