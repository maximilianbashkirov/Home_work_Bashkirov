from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.append('/opt/airflow/scripts')

from transform_mart import transform_and_build_marts


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'delivery_transform_mart_dag',
    default_args=default_args,
    description='Построение витрин с помощью PySpark',
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'delivery', 'mart'],
)


transform_mart_task = PythonOperator(
    task_id='transform_and_build_marts',
    python_callable=transform_and_build_marts,
    dag=dag,
)


transform_mart_task