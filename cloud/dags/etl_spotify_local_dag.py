from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
# from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import boto3

# Define AWS S3 and Redshift details
S3_BUCKET = "spotify-streaming-data"  # Change this to your bucket
S3_KEY_PREFIX = "kpi_genre_duration/"  # Folder where Glue writes output
S3_FULL_PATH = f"s3://{S3_BUCKET}/{S3_KEY_PREFIX}"  # S3 path for Redshift COPY

REDSHIFT_CONN_ID = "aws_redshift_default"  # You'll need to set this up in Airflow connections
REDSHIFT_SCHEMA = "public"  # Default schema, change if needed
REDSHIFT_TABLE = "spotify_genre_duration"  # The table where data will be loaded

# Path to your Docker Compose project
DOCKER_COMPOSE_PATH = "/home/mickael/data_engineer/dynamodb"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='spotify_etl_s3_redshift_dag',
    default_args=default_args,
    description='Spotify ETL Pipeline using Glue, S3, and Redshift',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Run Glue job using Docker
run_local_glue = BashOperator(
    task_id='run_local_glue_job',
    bash_command='cd ~/data_engineer/glue && docker compose up --build --remove-orphans',
    dag=dag
)

# Wait for a new file in S3
wait_for_output = S3KeySensor(
    task_id='wait_for_s3_output',
    bucket_name=S3_BUCKET,
    bucket_key=f"{S3_KEY_PREFIX}*",#.csv",  # Wait for any new CSV file
    wildcard_match=True,
    aws_conn_id="aws_default",
    poke_interval=60,  # Check every 60 seconds
    timeout=60 * 30,  # Timeout after 30 minutes
    mode='poke',
    dag=dag
)

def get_latest_s3_file(**kwargs):
    s3_client = boto3.client('s3', 
                            aws_access_key_id=kwargs['aws_access_key_id'], 
                            aws_secret_access_key=kwargs['aws_secret_access_key'],
                            region_name=kwargs['region'])
    
    response = s3_client.list_objects_v2(
        Bucket=kwargs['bucket'],
        Prefix=kwargs['prefix']
    )
    
    if 'Contents' not in response:
        raise Exception(f"No files found in s3://{kwargs['bucket']}/{kwargs['prefix']}")
    
    # Sort the objects by LastModified and get the most recent one
    latest_file = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)[0]
    latest_key = latest_file['Key']
    
    # Push the latest file key to XCom for the next task
    kwargs['ti'].xcom_push(key='latest_s3_file', value=latest_key)
    return latest_key



# Task to get the latest file from S3
get_latest_file = PythonOperator(
    task_id='get_latest_s3_file',
    python_callable=get_latest_s3_file,
    op_kwargs={
        'bucket': S3_BUCKET,
        'prefix': S3_KEY_PREFIX,
        'aws_access_key_id': '{{ conn.aws_default.login }}',
        'aws_secret_access_key': '{{ conn.aws_default.password }}',
        'region': 'eu-north-1'
    },
    provide_context=True,
    dag=dag
)

# BashOperator to run Docker Compose with the latest file info
load_to_dynamodb = BashOperator(
    task_id="load_s3_to_dynamodb",
    bash_command="""
        cd /home/mickael/data_engineer/dynamodb && \
        export LATEST_S3_KEY="{{ task_instance.xcom_pull(task_ids='get_latest_s3_file', key='latest_s3_file') }}" && \
        echo "Processing latest file: $LATEST_S3_KEY" && \
        docker compose build && \
        docker compose run --rm s3-to-dynamodb
    """,
    dag=dag
)

# load_to_redshift = S3ToRedshiftOperator(
#     task_id="copy_data_to_redshift",
#     redshift_conn_id=REDSHIFT_CONN_ID,
#     schema=REDSHIFT_SCHEMA,
#     table=REDSHIFT_TABLE,
#     s3_bucket=S3_BUCKET,
#     s3_key=S3_KEY_PREFIX,
#     copy_options=["CSV", "IGNOREHEADER 1"],
#     aws_conn_id="aws_default",
#     method="REPLACE",  # Changes to APPEND if you want to add to existing data
#     dag=dag
# )


def print_success():
    print("ETL process completed successfully! All tasks finished without errors.")

# Define the new success message task
final_success_task = PythonOperator(
    task_id='final_success_message',
    python_callable=print_success,
    dag=dag
)

# Set task dependencies
run_local_glue >> wait_for_output >>get_latest_file >> load_to_dynamodb >> final_success_task
