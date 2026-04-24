import src
import pytest

def main():
    print('Проверка всех тестов...')
    pytest.main(['-v', 'tests'])
    input('Нажмите Enter, чтобы вывести случайный набор из чанков')
    df = src.df_generator.get_initial_dataframe()
    for i in src.df_chunker.get_chunks(df, 'dt', chunk_size=0):
        print(i.to_string(index=False))

if __name__ == '__main__':
    main()