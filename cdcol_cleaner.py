# -*- coding: utf-8 -*-

import os
import shutil
import datetime
import psycopg2
import logging
import time


logging.basicConfig(
    format='%(levelname)s : %(asctime)s : %(message)s',
    level=logging.DEBUG
)

# To print loggin information in the console
# logging.getLogger().addHandler(logging.StreamHandler())


"""
The default dags results path.
"""
DAGS_RESULTS_PATH = '/web_storage/results'

"""
Path on which old dags scripts will be stored. 
"""
DAGS_HISTORY_PATH = '/web_storage/dags_history'

"""
Database connection data.
"""
DB_CONNECTION_DATA = {
    'host':'postgres',
    'dbname':'airflow',
    'user':'airflow',
    'passwd':'airflow'
}


def select_query(query_str):
    """Perform select queries.
    
    Args:
        query_str (str): SQL query to be performed.
    """
    connection_format = (
        "dbname='%(dbname)s' " 
        "user='%(user)s' "
        "host='%(host)s' "
        "password='%(passwd)s'"
    )

    connection_str = connection_format % DB_CONNECTION_DATA

    # Opening connecting
    connection = psycopg2.connect(connection_str)
    cursor = connection.cursor()

    # Performing query
    cursor.execute(query_str)
    rows = cursor.fetchall()

    # Close connection
    connection.close()

    return rows


def update_query():
    """Perform update queries.
    
    Args:
        query_str (str): SQL query to be performed.
    """
    pass


def get_dag_from_db(dag_id):
    """Return data of the dag with the given dag_id.
    
    Args:
        dag_id (str): Dag id
    """
    query_format = (
        'SELECT '
        'dag_id, '
        'fileloc '
        'FROM dag '
        'WHERE dag_id = \'%s\'; '
    )

    query_str = query_format % dag_id
    rows = select_query(query_str)
    return rows


def get_old_dags_runs_from_db(days):
    """
    Return the dag ids whose execution date exceeds 
    a certain number of days.

    Args:
        days (int): If the execution_date of the dag exceeds 
            a certain number of days it will be considered 
            in the result list.

    Returns:
        list: of dags executed certain number of day ago.
    """
    query_format = (
        'SELECT '
        'dag_id, '
        'execution_date, '
        'state '
        'FROM dag_run '
        'WHERE (now() - execution_date) > interval \'%s\' day ' 
        'AND state = \'success\' '
        'OR state = \'failed\' ;'
    )

    query_str = query_format % days
    rows = select_query(query_str)
    return rows


def mark_dag_results_as_deleted_db(dag_id):
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

    query_str = query_format % days
    rows = perform_update_query(query_str)
    return rows


def move_dag_script_to_history_folder(dag_id,dag_path):
    """Move a dag to a history directory

    Args:
        dag_id (str): dag id.
        dag_path (str): path to the dag file.
    """

    # Ensure dags scrts history folder exists
    if not os.path.exists(DAGS_HISTORY_PATH):
        os.makedirs(DAGS_HISTORY_PATH)

    file_name, file_ext = os.path.basename(dag_path).split('.')
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dag_name = file_name + '_' + timestamp + '.' + file_ext
    
    dag_history_path = os.path.join(DAGS_HISTORY_PATH,dag_name)
    shutil.move(dag_path, dag_history_path)
    # os.rename(dag_path, dag_history_path)

    logging.info(
        "Dag '%s' was moved to '%s'", 
        dag_path, dag_history_path
    )


def delete_dag_results_folder(dag_id,dag_results_path):
    logging.info(
        "Dag '%s' located at '%s' will be deleted.", 
        dag_id, dag_results_path
    )

    try:
        shutil.rmtree(dag_results_path)
    except OSError as e:
        logging.error(
            "Results of dag_id '%s'  can not be located at '%s' does not exists.", 
            dag_id, dag_results_path
        )
    else:
        logging.info(
            "Results of dag_id '%s' at '%s' were deleted.", dag_id, dag_results_path
        )
            

def lock_file(func):
    """
    Create a lock file during the decorated function 
    execution. When the decorated function finish it
    removes the lockfile.
    """
    def wrapper(*args,**kwargs):
        pid_file_name = 'cdcol_cleaner.lock'
        file_path = os.path.dirname(os.path.abspath(__file__))
        pid_file_path = os.path.join(file_path,pid_file_name)

        if not os.path.exists(pid_file_path):

            logging.info(
                "Running cdcol_result_cleaner ..."
            )
            # Create lockfile
            with open(pid_file_path,'w') as pid_file:
                pid_file.write(str(os.getpid()))

            # Running the decorated function
            func(*args,**kwargs)

            # Delete lockfile
            os.remove(pid_file_path)

            logging.info("End cdcol_result_cleaner ...")

        else:
            logging.warning("cdcol_result_cleaner is still running !")

    return wrapper


@lock_file
def delete_old_dags_result_folders(days):
    """Delete dag result folder in web_storage.
    
    Args:
        days (int): If the execution_date of the dag exceeds 
            a certain number of days it will be considered 
            in the result list.
    """

    rows = get_old_dags_runs_from_db(days)

    # Collect the complete path of the 
    # dags that will be removomed

    for row in rows:
        dag_id = row[0]
        dag_path = get_dag_from_db(dag_id)[0][1]
        dag_results_path = os.path.join(DAGS_RESULTS_PATH,dag_id)

        if os.path.exists(dag_path) and '_cleaner_dag' not in dag_id:
            delete_dag_results_folder(dag_id,dag_results_path)
            move_dag_script_to_history_folder(dag_id,dag_path)
            # mark_dag_results_as_deleted_db(dag_id)

if __name__ == '__main__':
    
    delete_old_dags_result_folders(days=0)