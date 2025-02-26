from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago

# Define default arguments and the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    's3_to_redshift_serverless',
    default_args=default_args,
    description='A simple S3 to Redshift Serverless DAG',
    schedule_interval=None,
)

# Define the S3 to Redshift Serverless task
copy_to_redshift = S3ToRedshiftOperator(
    task_id='copy_to_redshift',
    s3_bucket='your-s3-bucket-name',
    s3_key='path/to/streams1.csv',
    schema='PUBLIC',
    table='your_table_name',
    redshift_conn_id='redshift_serverless_connection',
    aws_conn_id='aws_default',
    copy_options=['csv'],
    dag=dag,
)

# Set the task in the DAG
copy_to_redshift
