from collections import Counter
import pytest
import pandas as pd
import numpy as np
import src

# берем рандомный датафрейм от 40 до 200 строк
# тест на то, что выходное количество строк исходного датафрейма и чанков совпадает
def test_rows_count():
    df = src.df_generator.get_initial_dataframe()
    count = len(df)
    checked_count = 0
    chunks = src.df_chunker.get_chunks(df, 'dt')
    for chunk in chunks:
        checked_count += len(chunk)
    assert checked_count == count, "Количество строк исходного датафрейма и чанков не совпадает"
# тест на сравнение чанков и исходного датафрейма
def test_chunks_union():
    df = src.df_generator.get_initial_dataframe(shuffle=False)
    chunks = src.df_chunker.get_chunks(df, 'dt')
    concat_chunks = pd.concat(chunks)
    assert concat_chunks.equals(df), "Исходный датафрейм и чанки после конкатенации неидентичны"
# тест на деление по чанкам на 2 строки
def test_2_size_df_chunker():
    df = src.df_generator.get_initial_dataframe()
    count = len(df)
    i = 0
    chunks = src.df_chunker.get_chunks(df, 'dt', chunk_size=2)
    for chunk in chunks:
        i += len(chunk)
        if i < count:
            assert len(chunk) >= 2, "Срез должен быть больше или равен 2"
# тест на деление по чанкам на 5 строк
def test_5_size_df_chunker():
    df = src.df_generator.get_initial_dataframe()
    count = len(df)
    i = 0
    chunks = src.df_chunker.get_chunks(df, 'dt', chunk_size=5)
    for chunk in chunks:
        i += len(chunk)
        if i < count:
            assert len(chunk) >= 5, "Срез должен быть больше или равен 5"
# тест на деление по чанкам на 10 строк
def test_10_size_df_chunker():
    df = src.df_generator.get_initial_dataframe()
    count = len(df)
    i = 0
    chunks = src.df_chunker.get_chunks(df, 'dt', chunk_size=10)
    for chunk in chunks:
        i += len(chunk)
        if i < count:
            assert len(chunk) >= 10, "Срез должен быть больше или равен 10"
# тест на то, что нет пересечений дат между чанками
def test_dates_inside_df_chunker():
    unique_dates = []
    df = src.df_generator.get_initial_dataframe()
    chunks = src.df_chunker.get_chunks(df, 'dt')
    for chunk in chunks:
        unique_dates.extend(chunk['dt'].unique().tolist())
    counts = Counter(unique_dates)
    assert all(val == 1 for val in counts.values()), "Даты в чанках неуникальны"