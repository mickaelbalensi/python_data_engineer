from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define AWS S3 and Redshift details
S3_BUCKET = "spotify-streaming-data"  # Change this to your bucket
S3_KEY_PREFIX = "kpi_genre_duration/"  # Folder where Glue writes output
S3_FULL_PATH = f"s3://{S3_BUCKET}/{S3_KEY_PREFIX}"  # S3 path for Redshift COPY

REDSHIFT_CONN_ID = "aws_redshift_default"  # Airflow connection to Redshift
REDSHIFT_TABLE = "spotify_genre_duration"  # Target table in Redshift
IAM_ROLE = "arn:aws:iam::your-account-id:role/your-redshift-role"  # Redshift IAM Role

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'spotify_etl_s3_redshift',
    default_args=default_args,
    description='Spotify ETL Pipeline using Glue, S3, and Redshift',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Run Glue job using Docker
run_local_glue = BashOperator(
    task_id='run_local_glue_job',
    bash_command='docker run --rm glue-spotify-etl',
    dag=dag
)

# Wait for a new file in S3
wait_for_output = S3KeySensor(
    task_id='wait_for_s3_output',
    bucket_name=S3_BUCKET,
    bucket_key=f"{S3_KEY_PREFIX}*.csv",  # Wait for any new CSV file
    wildcard_match=True,
    aws_conn_id="aws_default",
    poke_interval=60,  # Check every 60 seconds
    timeout=60 * 30,  # Timeout after 30 minutes
    mode='poke',
    dag=dag
)

# Load data from S3 into Redshift
load_to_redshift = RedshiftSQLOperator(
    task_id="copy_data_to_redshift",
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql=f"""
        COPY {REDSHIFT_TABLE}
        FROM '{S3_FULL_PATH}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1;
    """,
    dag=dag
)

def print_success():
    print("ETL process completed successfully! All tasks finished without errors.")

# Define the new success message task
final_success_task = PythonOperator(
    task_id='final_success_message',
    python_callable=print_success,
    dag=dag
)


# Set task dependencies
# run_local_glue >> wait_for_output >> load_to_redshift
run_local_glue >> wait_for_output >> final_success_task