# -*- coding: utf-8 -*-

"""
    cdcol_cleanner

    The cdcol_cleaner remove dags that are in success or fail state
    and have been executed 'DAYS_OLD' days ago.

    This script perform the following activities.

    1. Delete the result folder of a dag.
    2. Move the <dag_file_name>.py file to 'DAGS_HISTORY_PATH'.
    3. Delete dag logs from 'DAGS_BASE_LOG_FOLDER',
    4. Update execution_execution (WEB) table to mark the results as deleted.
    5. Delete dags from the airflow database.
"""

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



"""
The default dags results path.
"""
DAGS_BASE_PATH = '/web_storage/dags'

"""
The default dags results path.
"""
DAGS_RESULTS_PATH = '/web_storage/results'

"""
The base Dags logs folder
/web_storage/logs/{{dag_id}}
"""
DAGS_BASE_LOG_FOLDER = '/web_storage/logs'

"""
Path on which old dags scripts will be stored. 
"""
DAGS_HISTORY_PATH = '/web_storage/dags_history'

"""
This dags will be ignored by the cleaner
"""
DAGS_IGNORE = [
    # This is the cleaner dag (mandatory)
    'cdcol_cleaner_dag.py',
    'cdcol_updater_dag.py'
]

"""
Database connection data.
"""
AIRFLOW_DB_CONN_DATA = {
    'host':'192.168.106.21',
    'dbname':'airflow',
    'user':'airflow',
    'passwd':'cubocubo'
}

WEB_DB_CONN_DATA = {
    'host':'192.168.106.21',
    'dbname':'ideam_1',
    'user':'portal_web',
    'passwd':'CDCol_web_2016'
}

"""
If the execution_date - today() of a dag exceeds
this number. And they are in success or failed state,
they will be deleted.
"""
DAYS_OLD = 3


def select_query(query_str,conn_data):
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

    connection_str = connection_format % conn_data

    # Opening connecting
    connection = psycopg2.connect(connection_str)
    cursor = connection.cursor()

    # Performing query
    cursor.execute(query_str)
    rows = cursor.fetchall()

    # Close connection
    connection.close()

    return rows


def update_query(query_str,conn_data):
    """Perform update queries.
    
    Args:
        query_str (str): SQL query to be performed.
        conn_data (dict): a dictionary with database conection 
            data. 
    """
    connection_format = (
        "dbname='%(dbname)s' " 
        "user='%(user)s' "
        "host='%(host)s' "
        "password='%(passwd)s'"
    )

    connection_str = connection_format % conn_data

    # Opening connecting
    connection = psycopg2.connect(connection_str)
    cursor = connection.cursor()

    # Performing query
    cursor.execute(query_str)
    connection.commit()

    # updated row count
    row_count = cursor.rowcount
   
    # Close connection
    connection.close()

    return row_count


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
    rows = select_query(query_str,AIRFLOW_DB_CONN_DATA)
    return rows


def get_old_dags_runs_from_db(days):
    """
    Return the dag ids whose execution date exceeds 
    a certain number of days. And they are in success 
    or failed state.

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
        'AND dag_id != \'cdcol_cleaner_dag\' '
        'AND dag_id != \'cdcol_updater_dag\' '
        'AND state != \'running\' ;'
    )

    query_str = query_format % days
    rows = select_query(query_str,AIRFLOW_DB_CONN_DATA)
    return rows


def mark_dag_results_as_deleted_db(dag_id):
    """ Mark result forder as deleted in the db.

    Mark a result as deleted in 'execution_execution' for 
    the execution coresponding with the given dag_id.

    Args:
        dag_id (str): Name of th dags which results were
            deleted. 

    Return: the number of rows updated in the execution_execution
        table. It must be one,because for each execution only 
        one dag_id is allowed.
    """

    # The date on which the dag was deleted
    kwargs = {
        'dag_id': dag_id,
        'results_deleted_at': datetime.datetime.now(),
    }

    # We mark in the db a result as deleted
    query_format = (
        'UPDATE execution_execution SET '
        'results_available=FALSE, '
        'results_deleted_at= \'%(results_deleted_at)s \' '
        'WHERE dag_id=\'%(dag_id)s\';'
    )

    query_str = query_format % kwargs
    row_count = update_query(query_str,WEB_DB_CONN_DATA)
    return row_count


def move_dag_script_to_history_folder(dag_id,dag_path):
    """Move a dag to a 'DAGS_HISTORY_PATH'

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

    try:
        shutil.move(dag_path, dag_history_path)
    except OSError as e:
        logging.error(
            "Dag '%s' can not be moved from '%s' to '%s'", 
            dag_id, dag_path, dag_history_path
        )
    else:
        logging.info(
            "Dag '%s' was moved from '%s' to '%s'", 
            dag_id, dag_path, dag_history_path
        )


def delete_dag_results_folder(dag_id,dag_results_path):
    """Remove the given dag results folder

    Args:
        dag_id (str): dag id.
        dag_results_path (str): path to the dag result folder.
    """
    logging.info(
        "Dag '%s' results located at '%s' will be deleted.", 
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

def delete_dag_logs_folder(dag_id):
    """Remove all logs for a given dag 

    Remove the log folder of the dag with the given dag_id
    if 'DAGS_BASE_LOG_FOLDER' is defined

    Args:
        dag_id (str): dag id.
    """

    if DAGS_BASE_LOG_FOLDER:

        dag_logs_path = os.path.join(DAGS_BASE_LOG_FOLDER,dag_id)
        
        logging.info(
            "Dag '%s' logs located at '%s' will be deleted.", 
            dag_id, dag_logs_path
        )

        try:
            shutil.rmtree(dag_logs_path)
        except OSError as e:
            logging.error(
                "Logs of dag_id '%s'  can not be located at '%s' does not exists.", 
                dag_id, dag_logs_path
            )
        else:
            logging.info(
                "Logs of dag_id '%s' at '%s' were deleted.", dag_id, dag_logs_path
            )


def delete_dag_from_ariflow_db(dag_id):
    """ Delete a dag from all airflow database tables

    Args:
        dag_id (str): The dag that will be deleted from
            airflow tables.        
    """

    query_format = (
        'create temporary table t_dags_to_remove as select dag_id from dag where dag_id like \'%s\'; '
        'delete from xcom where dag_id in (select * from t_dags_to_remove); '
        'delete from task_reschedule where dag_id in (select * from t_dags_to_remove); '
        'delete from task_instance where dag_id in (select * from t_dags_to_remove); '
        'delete from task_fail where dag_id in (select * from t_dags_to_remove); '
        'delete from sla_miss where dag_id in (select * from t_dags_to_remove); '
        'delete from log where dag_id in (select * from t_dags_to_remove); '
        'delete from job where dag_id in (select * from t_dags_to_remove); '
        'delete from dag_run where dag_id in (select * from t_dags_to_remove); '
        'delete from dag_stats where dag_id in (select * from t_dags_to_remove); '
        'delete from dag where dag_id in (select * from t_dags_to_remove); '
    )

    query_str = query_format % dag_id
    row_count = update_query(query_str,AIRFLOW_DB_CONN_DATA)

    logging.info(
        'From dag_id %s, %s records were deleted.',
        dag_id,row_count
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


            try:
                # Running the decorated function
                func(*args,**kwargs)
            except Exception as e:
                logging.error("Something wrong happed !: %s",e)
                raise e
            finally:
                # Finally ensure the lock file is deleted
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


        logging.info('Row %s',row)
        # The dag file path is just used to get the dag_file_name 
        # NOTE: Arflow is not returning the file path correctly.
        dag_rows = get_dag_from_db(dag_id)

        if dag_rows:
            dag_file_path = get_dag_from_db(dag_id)[0][1]
            dag_file_name = os.path.basename(dag_file_path)
        else:
            dag_file_name = '{}.py'.format(dag_id)

        #dag_file_name = os.path.basename(dag_file_path)
        dag_results_folder = os.path.join(DAGS_RESULTS_PATH,dag_id)
        dag_file_path = os.path.join(DAGS_BASE_PATH,dag_file_name)

        # If the dag file exists and it is not an ignored dag. 
        # It will be deleted

        dag_exists = os.path.exists(dag_file_path)
        is_ignored = dag_file_name in DAGS_IGNORE


        logging.info(
            "Inspect %s: dag file path (%s)"
            ,dag_id,dag_file_path
        )

        logging.info(
            "Inspect %s: dag results path (%s)"
            ,dag_id,dag_results_folder
        )

        logging.info(
            "Inspect %s: dag exists (%s) and is in ignore dags (%s)"
            ,dag_id,dag_exists,is_ignored
        )

        if not is_ignored:
            delete_dag_results_folder(dag_id,dag_results_folder)
            move_dag_script_to_history_folder(dag_id,dag_file_path)
            delete_dag_logs_folder(dag_id)
            mark_dag_results_as_deleted_db(dag_id)
            delete_dag_from_ariflow_db(dag_id)

if __name__ == '__main__':
    
    delete_old_dags_result_folders(days=DAYS_OLD)
