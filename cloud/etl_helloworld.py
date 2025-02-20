from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import psycopg2
import redshift_connector

# AWS & Redshift Configuration
S3_BUCKET = "spotify-streaming-data"
CSV_FILE = "streams1.csv"
REDSHIFT_ENDPOINT = "spotify-etl-workgroup.715841349617.eu-north-1.redshift-serverless.amazonaws.com:5439/dev"
DATABASE = "dev"
# USER = "your-username"
# PASSWORD = "your-password"
IAM_ROLE = "arn:aws:iam::715841349617:role/service-role/AmazonRedshift-CommandsAccessRole-20250220T122056"

def upload_to_s3():
    """Uploads a CSV file to S3"""
    s3 = boto3.client('s3')
    s3.upload_file(CSV_FILE, S3_BUCKET, CSV_FILE)
    print("File uploaded to S3!")

def create_redshift_table():
    """Creates a table in Redshift"""
    conn = redshift_connector.connect(
        host=REDSHIFT_ENDPOINT,
        database=DATABASE,
        iam=True
    )
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS my_new_table (
        id INT,
        name VARCHAR(100),
        age INT
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Table created in Redshift!")

def load_data_to_redshift():
    """Loads CSV data from S3 into Redshift"""
    conn = redshift_connector.connect(
        host=REDSHIFT_ENDPOINT,
        database=DATABASE,
        iam=True
    )
    cursor = conn.cursor()
    cursor.execute(f"""
    COPY my_new_table
    FROM 's3://{S3_BUCKET}/{CSV_FILE}'
    IAM_ROLE '{IAM_ROLE}'
    FORMAT AS CSV
    IGNOREHEADER 1;
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Data loaded into Redshift!")

def verify_redshift_data():
    """Verifies data in Redshift"""
    conn = redshift_connector.connect(
        host=REDSHIFT_ENDPOINT,
        database=DATABASE,
        iam=True
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM my_new_table LIMIT 5;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    cursor.close()
    conn.close()
    print("Verification complete!")

# Define Airflow DAG
with DAG("redshift_etl",
         start_date=datetime(2024, 2, 20),
         schedule_interval="@daily",
         catchup=False) as dag:

    # task1 = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3)
    # task2 = PythonOperator(task_id="create_redshift_table", python_callable=create_redshift_table)
    task3 = PythonOperator(task_id="load_data_to_redshift", python_callable=load_data_to_redshift)
    task4 = PythonOperator(task_id="verify_redshift_data", python_callable=verify_redshift_data)

    # task1 >> task2 >> 
    task3 >> task4  # Define task dependencies
