import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'pipelines'))

from airflow import DAG
from datetime import datetime, timedelta
from pipelines.extract import extract_data
from pipelines.transform import transform_data
from pipelines.load import load_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True,
    'tags': ['etl_pg']
}


with DAG(
    dag_id='estate_etl',
    default_args=default_args,
    schedule_interval='@weekly'
) as dag:
    src_data=extract_data()
    transform_df=transform_data(src_data)
    load_data(transform_df)


