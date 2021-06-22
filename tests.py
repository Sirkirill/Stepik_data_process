import json

import pytest
import requests
import pandas as pd
import matplotlib.pyplot as plt
from decouple import config
from pyspark.sql import SparkSession
from matplotlib.testing.decorators import image_comparison

from data_processing.data_processing import start_spark, collect_n_max_value, extract_data


BASE_DIR = config('BASE_DIR', default='')


def test_stepik_connection():
    url = 'https://stepik.org:443/api/courses'
    response = requests.get(url)

    assert response.status_code == 200


def test_spider():
    url = 'https://stepik.org:443/api/courses?page=1'
    response = requests.get(url)
    results = response.json()
    next_page = results['meta']['page']+1

    assert next_page == 2


@pytest.fixture(scope='spark_session')
def spark_session():
    spark = start_spark('test')
    return spark


@pytest.mark.usefixtures('spark_session')
def test_collect_n_max_value(spark_session):
    test_df = spark_session.createDataFrame(
        [
            (1, 12, 'False'),
            (2, 3, 'False'),
            (3, 45, 'True'),
            (4, 0, 'False'),
            (5, 33, 'True')
        ],
        [
            ['id', 'learners_count', 'is_popular']
        ]
    )
    new_df = collect_n_max_value(3, test_df)
    assert new_df.count() == 3


@pytest.mark.usefixtures('spark_session')
def test_collect_data(spark_session):
    path = BASE_DIR + '/test.json'
    df = extract_data(spark_session, path)

    assert df.count() == 5


@image_comparison(baseline_images=['line_dashes'], remove_text=True,
                  extensions=['png'])
def test_visualization():
    path = BASE_DIR + '/test.json'

    data = pd.read_json(path, lines=True)
    df = pd.DataFrame(data)

    assert df.count() == 5

    df["learners_count"].plot(kind="bar")
