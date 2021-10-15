import datetime
import time
import json
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import os
from pandas.io import sql

# базовые аргументы DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

args = {
    'owner': 'aklyavlin',  # Информация о владельце DAG
    'start_date': datetime.datetime(2021, 8, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': datetime.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию,
    'provide_context': True  # Передаем ли мы контекст
}

# глобальные переменные
folder = os.path.expanduser('~') + '/data_dags/'
json_name_file = 'data_json.json'
csv_name_file = 'data_csv.csv'
key_api = Variable.get("KEY_API")
сity = 'Ulyanovsk'
moscow_timezone = 3


def extract_data():
    # Запрос погоды
    response = requests.get(
        'http://api.worldweatheronline.com/premium/v1/weather.ashx',
        params={
            'q': '{}'.format(сity),
            'format': 'json',
            'FX': 'no',
            'num_of_days': 1,
            'key': key_api,
            'includelocation': 'no'
        },
        headers={
            'Authorization': key_api
        }
    )

    if response.status_code == 200:
        # Разбор ответа сервера
        json_data = response.json()
        print(json.dumps(json_data, indent=2))
        with open(folder + json_name_file, 'w') as f:
            json.dump(json_data, f, indent=2)
            f.close()


def transform_data(**kwargs):
    with open(folder + json_name_file, 'r') as jd:
        json_data = json.load(jd)
        print(json_data)
        jd.close()

    value_list = []

    start_utc = datetime.datetime.utcnow()
    start_moscow = start_utc + datetime.timedelta(hours=moscow_timezone)

    city = json_data['data']['request'][0]['query']
    observation_time = json_data['data']['current_condition'][0]['observation_time']
    temp = json_data['data']['current_condition'][0]['temp_C']
    humidity = json_data['data']['current_condition'][0]['humidity']

    # Время наблюденя из json'а
    value_list.append(pd.to_datetime(observation_time).strftime('%Y-%m-%d %H:%M:%S'))
    res_df = pd.DataFrame(value_list, columns=['observation_time'])
    # Время запроса (по Москве)
    res_df["date_from_msk"] = start_moscow
    res_df["date_from_msk"] = pd.to_datetime(res_df["date_from_msk"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    # Время запроса (по UTC)
    res_df["date_from_utc"] = start_utc
    res_df["date_from_utc"] = pd.to_datetime(res_df["date_from_utc"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    # Город наблюдения
    res_df["city_observation"] = city
    # Температура
    res_df["city_temp_c"] = temp
    # Влажность
    res_df["city_humidity"] = humidity

    dx = res_df.to_json(orient='split')
    datajs = json.dumps(dx)
    kwargs['ti'].xcom_push(key='wdata_inside_XCOM', value=datajs)



def load_data(**kwargs):

    datajs = kwargs['ti'].xcom_pull(task_ids='transform_data', key='wdata_inside_XCOM')
    dx = json.loads(datajs)
    df = pd.read_json(dx, orient='split')
    print(df.head())

    hook = PostgresHook(postgre_conn_id='postgres_default')

    cols = ['observation_time', 'date_from_msk', 'date_from_utc', 'city_observation','city_temp_c', 'city_humidity']
    rows_db = df.values
    hook.insert_rows("weather_worldweatheronline", rows_db, target_fields=cols, commit_every=0)



with DAG(
        dag_id='weather_worldweatheronline_api',  # Имя DAG
        schedule_interval='@hourly',  # Периодичность запуска (0 * * * * )
        default_args=args,  # Базовые аргументы
) as dag:

    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)

    transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)

    load_data = PythonOperator(task_id='load_data', python_callable=load_data)

    extract_data >> transform_data >> load_data
