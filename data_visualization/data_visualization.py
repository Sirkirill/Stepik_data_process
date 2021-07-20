import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from decouple import config

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def pandas_df(path_to_jsons):
    json_files = [pos_json for pos_json in os.listdir(path_to_jsons) if pos_json.endswith('.json')]
    json_file = path_to_jsons + json_files[0]

    data = pd.read_json(json_file, lines=True)
    df = pd.DataFrame(data)

    return df


def visualize(path_to_jsons):
    visualize_df = pandas_df(path_to_jsons)
    visualize_df["learners_count"].plot(kind="bar")
    plt.xlabel("Top courses")
    plt.ylabel("Learners count")
    plt.savefig(path_to_jsons + 'plot.png')


if __name__ == "__main__":
    path = BASE_DIR + '/data_top/'
    visualize(path)
    plt.show()
