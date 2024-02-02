from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import pandas as pd
import pyodbc

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG :
dag = DAG(
    'batch',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
)
def pass_parameter():
    # SQL Server connection parameters
    server = '192.168.56.1'
    database = 'AutoCarsAnalytivsDW'
    username = 'aminscientist'
    password = 'PkpdndMSgUluU0d'

    # Return connection parameters as a tuple
    return server, database, username, password


def test_connection(**kwargs):

    ti = kwargs['ti']
    server, database, username, password = ti.xcom_pull(task_ids='pass_parameter')
    # Recreate the connection using the parameters
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(connection_string)
    conn.close()


############################ tasks #########################

data_extraction_task = BashOperator(
    task_id='data_extraction',
    bash_command='python3 ../ETL/data_extraction/data_extraction.py',
    dag=dag,
)

data_transformation_task = BashOperator(
    task_id='data_transformation',
    bash_command='python3 ../ETL/data_transformation/data_transformation.py',
    dag=dag,
)

data_loading_task = BashOperator(
    task_id='data_loading',
    bash_command='python3 ../ETL/data_loading/data_loading.py',
    dag=dag,
)

data_extraction_task >> data_transformation_task >> data_loading_task


if __name__ == "__main__":
    dag.cli()