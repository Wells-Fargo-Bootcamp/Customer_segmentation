from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Add the path to the Customer Segments Project
sys.path.insert(0, '/Users/moufas/Desktop/Customer Segments Project')

from pipeline import run_pipeline

# Define the default arguments for the DAG
default_args = {
    'owner': 'team3',
    'start_date': datetime(2024, 7, 17),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG('apache_beam_dag', default_args=default_args, schedule_interval='@weekly', catchup=False) as dag:
    
    # Define the task to run the Apache Beam pipeline
    run_beam_pipeline = PythonOperator(
        task_id='run_beam_pipeline',
        python_callable=run_pipeline
    )

    # Setting the task
    run_beam_pipeline
