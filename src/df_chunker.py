import pandas as pd
import numpy as np

CHUNK_SIZE = 5

def get_chunks(df, key, chunk_size=CHUNK_SIZE):
    start = 0
    df.set_index(key, drop=False, inplace=True)
    df = df.sort_index()
    length = len(df)
    index_values = df.index.values
    change_points = np.where(index_values[1:] != index_values[:-1])[0] + 1
    while start < length:
        target_end = start + chunk_size
        actual_end_index = np.searchsorted(change_points, target_end)
        actual_end = change_points[actual_end_index] if actual_end_index < len(change_points) else length
        yield df.iloc[start:actual_end]
        start = actual_end
