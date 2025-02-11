from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Make sure this path exists and is accessible
SCRIPT_PATH = "/home/studen/mickael/python_data_engineer/wiki_scrapper/phase2/extract_metadata/save_metadata.py"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='wiki_metadata_pipeline',  # Added explicit dag_id
    default_args=default_args,
    description='Extract and store metadata from HTML files',
    schedule_interval='@daily',
    catchup=False,
    tags=['wikipedia', 'metadata']
) as dag:  # Using context manager
    
    def run_script():
        """Executes the metadata extraction script."""
        subprocess.run(["python3", SCRIPT_PATH], check=True)

    extract_metadata_task = PythonOperator(
        task_id='extract_metadata',
        python_callable=run_script,
        dag=dag
    )


