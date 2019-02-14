# -*- coding: utf-8 -*-

import os
import datetime
import psycopg2
import psycopg2.extras

"""
The default dags results path 
"""
DAGS_RESULTS_PATH = '/web_storage/results'


def get_dags_from_db(**kwargs):
    """
        Return the dag ids whose execution date exceeds 
        a certain number of days.

        Args:
            kwargs (dict): Contains connection data 
                such host, dbname, user, passwd and days.
                If the execution date of the dag exceeds 
                a certain number of days it will be considered 
                in the result list.

        Returns:
            list: of dags executed certain number of day ago.
    """
    query_format = (
        'SELECT '
        'id, '
        'dag_id, '
        'execution_date, '
        'state, '
        'run_id, '
        'external_trigger, '
        'conf, '
        'end_date, '
        'start_date '
        'FROM dag_run '
        'WHERE (now() - execution_date) > interval \'%(days)s\' day ' 
        'AND state = \'success\' '
        'OR state = \'failed\' ;'
    )

    connection_format = (
        "dbname='%(dbname)s' " 
        "user='%(user)s' "
        "host='%(host)s' "
        "password='%(passwd)s'"
    )

    connection_str = connection_format % kwargs
    query = query_format % kwargs

    # Opening connecting
    connection = psycopg2.connect(connection_str)
    cursor = connection.cursor()

    # Performing query
    cursor.execute(query)
    rows = cursor.fetchall()

    # Close connection
    connection.close()

    return rows


def update_dag_exection_db(**kwargs):

    kwargs['results_deleted_at'] = datetime.datetime.now()

    query_format = (
        'UPDATE execution_execution SET '
        'results_available=FALSE, '
        'results_deleted_at= \'%(results_deleted_at)s \' '
        'WHERE id=%(dag_id)s;'
    )

    connection_format = (
        "dbname='%(dbname)s' " 
        "user='%(user)s' "
        "host='%(host)s' "
        "password='%(passwd)s'"
    )

    connection_str = connection_format % kwargs
    query = query_format % kwargs

    # Opening connecting
    connection = psycopg2.connect(connection_str)
    cursor = connection.cursor()

    # Performing query
    cursor.execute(query)
    connection.commit()

    # Close connection
    connection.close()


def get_dags_older_result_paths(**kwargs):

    rows = get_dags_older_result_paths(**kwargs)

    # Collect the complete path of the 
    # dags that will be removomed
    dags_paths = []
    for row in rows:
        dag_id = row[1]
        dag_path = os.path.join(DAGS_RESULTS_PATH,dag_id)
        dags_paths.append((dag_id,dag_path))

    return dags_paths


def delete_dag_results_folders(dags_paths,**kwargs):
    for dag_id, dag_path in dags_paths:
        if os.path.exists(dag_path):
            os.remove(dag_path)
            update_dag_exection_db(**kwargs)


if __name__ == '__main__':
    
    data = {
        'host':'postgres',
        'dbname':'airflow',
        'user':'airflow',
        'passwd':'airflow',
        'days': 0,
    }

    dags_paths = get_dags_older_result_paths(**data)