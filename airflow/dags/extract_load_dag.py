from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.append('/opt/airflow/scripts')

from extract_load import extract_and_load


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'delivery_extract_load_dag',
    default_args=default_args,
    description='DAG для извлечения данных из Parquet и загрузки в PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'delivery'],
)


extract_load_task = PythonOperator(
    task_id='extract_and_load_data',
    python_callable=extract_and_load,
    dag=dag,
)


extract_load_task