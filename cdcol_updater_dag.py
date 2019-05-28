# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta


every_minute = '*/1 * * * *'
every_day = '@daily'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2019-05-20T23:59'
}

dag = DAG(
    dag_id='cdcol_updater_dag',
    schedule_interval=every_minute,
    max_active_runs=1,
    catchup=False,
    default_args=default_args
)

task = BashOperator(
    dag=dag,
    task_id='cdcol_updater_task',
    bash_command='python /web_storage/algorithms/cdcol_updater/cdcol_updater.py'
)

