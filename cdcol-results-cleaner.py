# -*- coding: utf-8 -*-
"""
SIMPLE LOCKFILE TO DETECT PREVIOUS INSTANCES OF APP
"""

import os
import datetime
import psycopg2
import logging


logging.basicConfig(
    format='%(levelname)s : %(asctime)s : %(message)s',
    level=logging.DEBUG
)

# To print loggin information in the console
logging.getLogger().addHandler(logging.StreamHandler())


"""
The default dags results path 
"""
DAGS_RESULTS_PATH = '/web_storage/results'


def get_old_dags_from_db(**kwargs):
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


def mark_dag_results_as_deleted_db(**kwargs):
    """ Mark result forder as deleted in the db.

    Mark a result as deleted in 'execution_execution' table 
    created by the web componnent. This table keeps track of the results 
    generate by airflow diring an execution.

    Args:
        kwargs (dict): Must contain the dag_id to be 
            updated.
    """

    # The date on which the dag was deleted
    kwargs['results_deleted_at'] = datetime.datetime.now()

    # We mark in the db a result as deleted
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


def move_dag_to_history_folder(dag_path):
    """Move a dag to a history directory

    Args:
        dag_path (str): path to the dag file
    """
    dag_history_path = os.path.join(DAGS_HISTORY_PATH,dag_id)
    os.rename(dag_path, dag_history_path)


def delete_dag_results_folders(**kwargs):
    """Delete dag result folder in web_storage.

    Args:
        kwargs (dict): Contains db contecion data such us
            dbname, user, passwd, host.
    """

    rows = get_old_dags_from_db(**kwargs)

    # Collect the complete path of the 
    # dags that will be removomed
    for row in rows:
        dag_id = row[1]
        dag_path = row[10]
        if os.path.exists(dag_path):
            os.remove(dag_path)
            update_dag_exection_db(**kwargs)
            move_dag_to_history_folder(dags_path)        


if __name__ == '__main__':
    
    data = {
        'host':'postgres',
        'dbname':'airflow',
        'user':'airflow',
        'passwd':'airflow',
        'days': 0,
    }

    dags_paths = get_old_dags_result_paths(**data)