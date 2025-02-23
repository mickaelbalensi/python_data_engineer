from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import io
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'redshift_etl',
    default_args=default_args,
    description='A simple ETL pipeline to load data from S3 to Redshift Serverless',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Function to create table in Redshift Serverless
def create_table_in_redshift(**kwargs):
    aws_hook = AwsBaseHook(aws_conn_id='redshift_serverless_default', client_type='redshift-data')
    client = aws_hook.get_conn()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS spotify_data_namespace.spotify_etl_workgroup (
        user_id INT,
        track_id VARCHAR(255),
        listen_time TIMESTAMP,
        PRIMARY KEY (user_id, track_id)
    );
    """

    client.execute_statement(
        database='dev',  
        workgroupName='spotify-etl-workgroup',  
        sql=create_table_sql
    )

# Function to extract data from S3
def extract_data_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'spotify-streaming-data'
    key = 'streams1.csv'
    s3_object = s3_hook.get_key(key, bucket_name)
    data = s3_object.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(data))
    kwargs['ti'].xcom_push(key='dataframe', value=df)

# Function to load data into Redshift Serverless
def load_data_to_redshift_serverless(**kwargs):
    df = kwargs['ti'].xcom_pull(key='dataframe', task_ids='extract_data_from_s3')
    aws_hook = AwsBaseHook(aws_conn_id='redshift_serverless_default', client_type='redshift-data')
    client = aws_hook.get_conn()

    # Convert DataFrame to CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Execute SQL to load data into Redshift Serverless
    sql = f"""
    COPY spotify_data_namespace.spotify_etl_workgroup
    FROM 's3://spotify-streaming-data/streams1.csv'
    IAM_ROLE 'arn:aws:iam::715841349617:role/service-role/AmazonRedshift-CommandsAccessRole-20250220T122056'
    CSV;
    """
    client.execute_statement(
        database='dev',
        workgroupName='spotify-etl-workgroup',
        sql=sql
    )


create_table_task = PythonOperator(
    task_id='create_table_in_redshift',
    python_callable=create_table_in_redshift,
    provide_context=True,
    dag=dag,
)


# Task to extract data from S3
extract_data_task = PythonOperator(
    task_id='extract_data_from_s3',
    python_callable=extract_data_from_s3,
    provide_context=True,
    dag=dag,
)

# Task to load data into Redshift Serverless
load_data_task = PythonOperator(
    task_id='load_data_to_redshift_serverless',
    python_callable=load_data_to_redshift_serverless,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
create_table_task >> extract_data_task >> load_data_task



# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import boto3
# import psycopg2
# import redshift_connector

# # AWS & Redshift Configuration
# S3_BUCKET = "spotify-streaming-data"
# CSV_FILE = "streams1.csv"
# REDSHIFT_ENDPOINT = "spotify-etl-workgroup.715841349617.eu-north-1.redshift-serverless.amazonaws.com:5439/dev"
# DATABASE = "dev"
# # USER = "your-username"
# # PASSWORD = "your-password"
# IAM_ROLE = "arn:aws:iam::715841349617:role/service-role/AmazonRedshift-CommandsAccessRole-20250220T122056"

# def upload_to_s3():
#     """Uploads a CSV file to S3"""
#     s3 = boto3.client('s3')
#     s3.upload_file(CSV_FILE, S3_BUCKET, CSV_FILE)
#     print("File uploaded to S3!")

# def create_redshift_table():
#     """Creates a table in Redshift"""
#     conn = redshift_connector.connect(
#         host=REDSHIFT_ENDPOINT,
#         database=DATABASE,
#         iam=True
#     )
#     cursor = conn.cursor()
#     cursor.execute("""
#     CREATE TABLE IF NOT EXISTS my_new_table (
#         id INT,
#         name VARCHAR(100),
#         age INT
#     );
#     """)
#     conn.commit()
#     cursor.close()
#     conn.close()
#     print("Table created in Redshift!")

# def load_data_to_redshift():
#     """Loads CSV data from S3 into Redshift"""
#     conn = redshift_connector.connect(
#         host=REDSHIFT_ENDPOINT,
#         database=DATABASE,
#         iam=True
#     )
#     cursor = conn.cursor()
#     cursor.execute(f"""
#     COPY my_new_table
#     FROM 's3://{S3_BUCKET}/{CSV_FILE}'
#     IAM_ROLE '{IAM_ROLE}'
#     FORMAT AS CSV
#     IGNOREHEADER 1;
#     """)
#     conn.commit()
#     cursor.close()
#     conn.close()
#     print("Data loaded into Redshift!")

# def verify_redshift_data():
#     """Verifies data in Redshift"""
#     conn = redshift_connector.connect(
#         host=REDSHIFT_ENDPOINT,
#         database=DATABASE,
#         iam=True
#     )
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM my_new_table LIMIT 5;")
#     rows = cursor.fetchall()
#     for row in rows:
#         print(row)
#     cursor.close()
#     conn.close()
#     print("Verification complete!")

# # Define Airflow DAG
# with DAG("redshift_etl",
#          start_date=datetime(2024, 2, 20),
#          schedule_interval="@daily",
#          catchup=False) as dag:

#     # task1 = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3)
#     # task2 = PythonOperator(task_id="create_redshift_table", python_callable=create_redshift_table)
#     task3 = PythonOperator(task_id="load_data_to_redshift", python_callable=load_data_to_redshift)
#     task4 = PythonOperator(task_id="verify_redshift_data", python_callable=verify_redshift_data)

#     # task1 >> task2 >> 
#     task3 >> task4  # Define task dependencies
