import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from decouple import config

BASE_DIR = config('BASE_DIR')

path_to_jsons = BASE_DIR + '/data_top/'
json_files = [pos_json for pos_json in os.listdir(path_to_jsons) if pos_json.endswith('.json')]
json_file = path_to_jsons + json_files[0]

data = pd.read_json(json_file, lines=True)
df = pd.DataFrame(data)

df["learners_count"].plot(kind="bar")

plt.xlabel("Top courses")
plt.ylabel("Learners count")
plt.savefig(path_to_jsons + 'plot.png')
plt.show()
