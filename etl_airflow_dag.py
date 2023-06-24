from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ETL import data_etl

with DAG(
  'etl_pipeline',
  description="A simple ETL pipeline using Python and Apache Airflow",
  start_date=datetime(year=2023, month=6, day=23),
  schedule_interval=timedelta(minutes=55)
) as dag:
  
  etl_airflow_dag = PythonOperator(
    task_id = 'ETL_pipeline',
    python_callable = data_etl
  )
  
