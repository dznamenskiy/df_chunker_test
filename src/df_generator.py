import pandas as pd
import numpy as np
import random

def get_initial_dataframe(shuffle=True):
    dfs = pd.date_range(
        "2023-01-01 00:00:00",
        "2023-01-01 00:00:36",
        freq="s"
    )
    shuffled_dates = np.random.permutation(dfs)
    repeat = random.randint(1,5)
    df = pd.DataFrame({"dt": shuffled_dates.repeat(repeat) if shuffle else dfs.repeat(repeat)})
    return df
