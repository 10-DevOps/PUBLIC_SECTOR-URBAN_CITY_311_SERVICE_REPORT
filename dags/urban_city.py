from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task

from include.transform import transform
from include.upload_raw_data import upload_data

default_args = {
    'owner': 'amdari',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@hourly',
}

@task()
def extract_data_from_api():
    api_response = upload_data()
    
    return api_response

@task()
def transform_data():
    cleaned_data = transform()

    return cleaned_data


with DAG(dag_id='urban_city_requests',
         catchup=False, default_args=default_args):
      
  extract_data_from_api() >> transform_data()
  