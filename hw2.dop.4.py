import json
import os
import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine
from airflow.models import Variable

from dvs_module2dop4 import *
from airflow.decorators import dag, task

# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2021, 10, 10),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=10),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}


@dag(schedule_interval=None, start_date=dt.datetime(2021, 10, 10), catchup=False, tags=['dmitry sevost test2'], dag_id='xdvs_hw2.dop.4',
     default_args=args)
def test_taskflow_api():
    bo1 = BashOperator(
        task_id='first_task',
        bash_command='echo "DVS_DAG start: run_id={{ run_id }} | dag_run={{ dag_run }}"'
    )
    bo2 = BashOperator(
        task_id='last_task',
        bash_command='echo "DVS_DAG end: ds={{ ds }} "'
    )

    bo1
    zdf = download_titanic_dataset()
    [mean_fare_per_class(zdf), pivot_dataset(zdf)] >> bo2


res = test_taskflow_api()
