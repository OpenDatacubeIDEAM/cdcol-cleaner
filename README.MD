# CDCOL CLEANER

CDCol Cleaner is a single task dag which remove the results of the workflow that were executed 
some days ago. Additionally, it moves the dag script to */web_storage/dags_history*.

## Setting UP

Place the file **cdcol_cleaner.py** in /web_storage/algorithms. And edit the following parameters to get it working properly.

```python
"""
The default dags results path.
"""
DAGS_RESULTS_PATH = '/web_storage/results'

"""
The base Dags logs folder
/web_storage/logs/{{dag_id}}
"""
DAGS_BASE_LOG_FOLDER = '/home/airflow/logs'

"""
Path on which old dags scripts will be stored. 
"""
DAGS_HISTORY_PATH = '/web_storage/dags_history'

"""
This dags will be ignored by the cleaner
"""
DAGS_IGNORE = [
    # This is the cleaner dag (mandatory)
    'cdcol_cleaner_dag.py'
]

"""
Database connection data.
"""
DB_CONNECTION_DATA = {
    'host':'postgres',
    'dbname':'airflow',
    'user':'airflow',
    'passwd':'airflow'
}

"""
If the execution_date - today() of a dag exceeds
this number it will be deleted.
"""
DAYS_OLD = 3
```

Place the file **cdcol_cleaner_dag.py** in /web_storage/dags. And edit the following parameters to get it working properly.