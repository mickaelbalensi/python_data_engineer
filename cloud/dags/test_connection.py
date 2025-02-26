from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.utils.dates import days_ago

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
    'minimal_redshift_serverless_dag',
    default_args=default_args,
    description='A minimal DAG to connect to Redshift Serverless',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Define the SQL query to execute
sql_query = """
SELECT 1 AS test_column;
"""

# Define the Redshift Serverless task
redshift_task = RedshiftDataOperator(
    task_id='execute_redshift_serverless_query',
    sql=sql_query,
    aws_conn_id='redshift_serverless_default',  
    cluster_identifier='spotify-etl-workgroup',  
    database='dev',
    #db_user='your-db-user',  
    dag=dag,
)

# Set the task in the DAG
redshift_task
