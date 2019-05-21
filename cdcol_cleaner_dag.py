# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta


every_minute = '* * * * *'
every_day = '@daily'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'retry_delay': timedelta(minutes=5),
    # Run once a day at midnight
    'schedule_interval': every_minute,
}


dag = DAG(
    dag_id='cdcol_updater_and_cleaner_dag',
    default_args=default_args
)

task_1 = BashOperator(
    dag=dag,
    task_id='cdcol_updater_task',
    bash_command='python /web_storage/algorithms/cdcol_updater.py'
)

task_2 = BashOperator(
    dag=dag,
    task_id='cdcol_cleaner_task',
    bash_command='python /web_storage/algorithms/cdcol_cleaner.py'
)

task_1 >> task_2