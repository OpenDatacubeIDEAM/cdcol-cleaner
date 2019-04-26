# -*- coding: utf-8 -*-

"""
cdcol_updater 

This script update the state of executions which dag_id 
is in state 'success' or 'fail'.

If the dag_id is in state 'running' the state of the 
execution is not updated.
"""

import psycopg2
import logging


logging.basicConfig(
    format='%(levelname)s : %(asctime)s : %(message)s',
    level=logging.DEBUG
)

# To print loggin information in the console
# logging.getLogger().addHandler(logging.StreamHandler())


"""
Database connection data.
"""
AIRFLOW_DB_CONN_DATA = {
    'host':'172.18.0.2',
    'dbname':'airflow',
    'user':'airflow',
    'passwd':'airflow'
}

WEB_DB_CONN_DATA = {
    'host':'db',
    'dbname':'ideam',
    'user':'portal_web',
    'passwd':'CDCol_web_2016'
}


def select_query(query_str,conn_data):
    """Perform select queries.
    
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


def get_enqueued_state_executions():
    """Return the executions in state "EN ESPERA"
    
    Retrieve the executions form execution_execution
    which has state == 1
    """

    query_format = (
        'SELECT '
        'dag_id '
        'FROM execution_execution '
        'WHERE state = \'%s\' ;'
    )

    query_str = query_format % ('1',)
    rows = select_query(query_str,WEB_DB_CONN_DATA)
    return rows


def get_dag_run(dag_id):
    """Return the executions in state "EN ESPERA"
    
    Retrieve the executions from execution_execution
    which has state == 1
    """

    query_format = (
        'SELECT '
        'dag_id, '
        'state, '
        'start_date, '
        'end_date '
        'FROM dag_run '
        'WHERE dag_id = \'%s\' ;'
    )

    query_str = query_format % (dag_id,)
    rows = select_query(query_str,AIRFLOW_DB_CONN_DATA)

    return rows[-1] if rows else None


def update_execution(**fields):
    """Update the execution_execution table form the web database.

    Updates the state, started_at, finished_at and 
    results_available fields of a given dag_id
    """
    query_format = (
        'UPDATE execution_execution SET '
        'state=\'%(state)s\', '
        'started_at=\'%(start_date)s\', '
        'finished_at=\'%(end_date)s\', '
        'results_available=TRUE '
        'WHERE dag_id=\'%(dag_id)s\';'
    )

    query_str = query_format % fields
    row_count = update_query(query_str,WEB_DB_CONN_DATA)
    return row_count


def update_executions():
    """Update Success or Failed executions.

    1. Retrieve the executions which state is 'EN ESPERA' (1).
        from the Web database.
    2. Retrieve the dag runs for a given execution.dag_id 
        from the airflow database.
    3. Updates the executions that have dag runs with state 
        success or failed
    """
    executions = get_enqueued_state_executions()
    for dag_id, in executions:
        dag_run = get_dag_run(dag_id)
        logging.info(
            'Checking Dag %s, has runs (%s)',
            dag_id,
            bool(dag_run)
        )
        if dag_run:
            dag_id, state, start_date, end_date = dag_run
            logging.info(
                'Dag %s has finished (%s), state (%s)',dag_id,bool(end_date),state
            )

            if start_date and end_date:
                success = '4'
                error = '3'                
                state = success if state in 'success' else error
                updated = update_execution(
                    dag_id=dag_id,
                    state=state,
                    start_date=start_date,
                    end_date=end_date
                )
                logging.info('Dag %s updated (%s)',dag_id,bool(updated))
                


if __name__ == '__main__':
    update_executions()
