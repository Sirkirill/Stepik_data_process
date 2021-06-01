import os,json

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


path_to_jsons = '/Users/kbarkalov/PycharmProjects/Stepik_data_process/data_top/'
json_files = [pos_json for pos_json in os.listdir(path_to_jsons) if pos_json.endswith('.json')]
json_file = path_to_jsons + json_files[0]

data = pd.read_json(json_file, lines=True)
df = pd.DataFrame(data)

df["learners_count"].plot(kind="bar")
plt.show()
