import json
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable
import os

from airflow.decorators import task




# которая считывает файл titanic.csv и расчитывает среднюю арифметическую цену билета (Fare)
# для каждого класса (Pclass) и сохраняет результирующий датафрейм в файл titanic_mean_fares.csv
@task
def mean_fare_per_class(datajs):
    # df = pd.read_csv(get_path('titanic.csv'))
    #ti = kwargs['ti']
    #datajs = ti.xcom_pull(task_ids='download_titanic_dataset', key='titanic_dataset_inside_XCOM')
    dx = json.loads(datajs)
    df = pd.read_json(dx["datj"], orient='split')

    df2 = df.groupby('Pclass')['Fare'] \
        .agg(['mean']) \
        .rename(columns={'mean': 'Fare'})

    # df2.to_csv(get_path('titanic_mean_fares.csv'), encoding='utf-8')
    # запись в таблицу xtableone
    engine = create_engine('postgresql://airflow:airflow@localhost:5432/data_warehouse')

    xtab = Variable.get("xtableone")
    df2.to_sql(xtab, engine, if_exists='replace')
    return 0



def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

@task
def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    # df.to_csv(get_path('titanic.csv'), encoding='utf-8')
    # df = pd.read_csv(get_path('titanic.csv'))
    dx = df.to_json(orient="split")
    datajs = json.dumps({"datj": dx})

    return datajs
    #kwargs['ti'].xcom_push(key='titanic_dataset_inside_XCOM', value=datajs)

@task
def pivot_dataset(datajs):
    # titanic_df = pd.read_csv(get_path('titanic.csv'))
    # ti = kwargs['ti']
    # datajs = ti.xcom_pull(task_ids='download_titanic_dataset', key='titanic_dataset_inside_XCOM')
    dx = json.loads(datajs)
    titanic_df = pd.read_json(dx["datj"], orient='split')

    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    # df.to_csv(get_path('titanic_pivot.csv'))
    engine = create_engine('postgresql://airflow:airflow@localhost:5432/data_warehouse')

    xtab = Variable.get("xtabletwo")
    df.to_sql(xtab, engine, if_exists='replace')
    return 0
