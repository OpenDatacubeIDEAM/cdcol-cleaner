from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
from datetime import datetime, timedelta

run_path = '~/CDColCleaner/run.py'

seven_days_ago = datetime.combine(datetime.today() - timedelta(7), datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('simple', default_args=default_args)
t1 = BashOperator(
task_id='CDCol cleaner',
bash_command= 'python ' + '',
dag=dag)
