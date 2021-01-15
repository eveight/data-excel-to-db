import pandas
import glob, os
from sqlalchemy import create_engine

# Конект к БД
engine = create_engine('postgresql://postgres:password@localhost:5432/nal')


def valid_columns(df):
    """ Валидация названий колонок, для дальнейшей работы с БД.
        Принимает ДатаФрейм."""
    df.rename(columns=lambda x: x.replace('(', '').replace(')', '').replace('%', '')[0:30], inplace=True)


def drop_duplicates_in_table(*args):
    """ Удаление дубликатов в таблице.
        Может принимать неограниченное кол. имён таблиц(str)"""
    for name_table in args:
        # Считываем все данные из БД
        df = pandas.read_sql_table(name_table, engine)
        print(df)
        # Удаляем дубликаты
        df = df.drop_duplicates()
        print(df)
        # Инсерт обратно в БД
        df.to_sql(name_table, engine, if_exists='replace', index=False)


# Поиск всех эксель документов в папке "data"
os.chdir("data")

for file in glob.glob("*.xlsx"):
    try:
        print(file + ' выгружаем в ДатаФрейм... Инсертим в БД...')

        df_p1 = pandas.read_excel(file,
                                  sheet_name='ПЗ',
                                  engine='openpyxl')
        valid_columns(df_p1)
        # Удаление дубликатов
        df_p1 = df_p1.drop_duplicates()
        # Инсерт в БД
        df_p1.to_sql('pz', engine, if_exists='append')

        df_p2 = pandas.read_excel(file,
                                  sheet_name='ПК',
                                  engine='openpyxl')
        valid_columns(df_p2)
        # Удаление дубликтов
        df_p2 = df_p2.drop_duplicates()
        # Инсерт в БД
        df_p2.to_sql('pc', engine, if_exists='append')

        df_p3 = pandas.read_excel(file,
                                  sheet_name='Е',
                                  engine='openpyxl')
        valid_columns(df_p3)
        # Удаление дубликтов
        df_p3 = df_p3.drop_duplicates()
        # Инсерт в БД
        df_p3.to_sql('e', engine, if_exists='append')

        df_p4 = pandas.read_excel(file,
                                  sheet_name='І',
                                  engine='openpyxl')
        valid_columns(df_p4)
        # Удаление дубликтов
        df_p4 = df_p4.drop_duplicates()
        # Инсерт в БД
        df_p4.to_sql('i', engine, if_exists='append')
    except (KeyError, NameError):
        continue

drop_duplicates_in_table('pc', 'pz')
