from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import pandas as pd
import io
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def create_table_if_not_exists(**context):
    try:
        pg_hook = PostgresHook(postgres_conn_id='redshift_default')
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS users (
            user_id INT PRIMARY KEY,
            user_name VARCHAR(100),
            user_age INT,
            user_country VARCHAR(50),
            created_at TIMESTAMP
        );
        """
        pg_hook.run(create_table_sql)
        logging.info("Table creation completed successfully")
        return "Table creation checked/completed"
    except Exception as e:
        logging.error(f"Error creating table: {str(e)}")
        raise AirflowException(f"Failed to create table: {str(e)}")

def extract_from_s3(**context):
    try:
        # Initialize S3 Hook
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        # Get the data from S3
        data = s3_hook.read_key(
            key='users.csv',
            bucket_name='spotify-streaming-data'
        )
        
        # Convert to pandas DataFrame
        df = pd.read_csv(io.StringIO(data))
        
        logging.info(f"Extracted {len(df)} rows from S3")
        logging.info(f"Columns: {df.columns.tolist()}")
        
        # Convert DataFrame to list of dictionaries and push to XCom
        data_to_load = df.to_dict('records')
        context['task_instance'].xcom_push(key='extracted_data', value=data_to_load)
        
        return "Data extracted successfully"
    except Exception as e:
        logging.error(f"Error extracting data from S3: {str(e)}")
        raise AirflowException(f"Failed to extract data from S3: {str(e)}")

def load_to_redshift(**context):
    try:
        # Get data from XCom
        data = context['task_instance'].xcom_pull(task_ids='extract_from_s3', key='extracted_data')
        
        if not data:
            raise AirflowException("No data received from extract task")
        
        # Convert back to DataFrame
        df = pd.DataFrame(data)
        
        # Initialize Postgres hook
        pg_hook = PostgresHook(postgres_conn_id='redshift_default')
        
        # Create a temporary table
        temp_table_sql = """
        CREATE TEMP TABLE temp_users (
            user_id INT,
            user_name VARCHAR(100),
            user_age INT,
            user_country VARCHAR(50),
            created_at TIMESTAMP
        );
        """
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                # Create temp table
                cur.execute(temp_table_sql)
                
                # Prepare data for insertion
                values = []
                for _, row in df.iterrows():
                    values.append(f"({row['user_id']}, '{row['user_name']}', {row['user_age']}, '{row['user_country']}', '{row['created_at']}')")
                
                # Insert data into temp table
                insert_sql = f"""
                INSERT INTO temp_users (user_id, user_name, user_age, user_country, created_at)
                VALUES {','.join(values)};
                """
                cur.execute(insert_sql)
                
                # Insert from temp to main table
                cur.execute("""
                    INSERT INTO users (user_id, user_name, user_age, user_country, created_at)
                    SELECT t.user_id, t.user_name, t.user_age, t.user_country, t.created_at
                    FROM temp_users t
                    LEFT JOIN users u ON t.user_id = u.user_id
                    WHERE u.user_id IS NULL;
                """)
                
                conn.commit()
        
        logging.info(f"Loaded {len(df)} rows to Redshift")
        return "Data loaded successfully"
    except Exception as e:
        logging.error(f"Error loading data to Redshift: {str(e)}")
        raise AirflowException(f"Failed to load data to Redshift: {str(e)}")

# Create DAG
dag = DAG(
    'mwaa_s3_to_redshift',
    default_args=default_args,
    description='Extract from S3 and load to Redshift',
    schedule_interval='@daily',
    catchup=False
)

# Task 0: Create table if not exists
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table_if_not_exists,
    provide_context=True,
    dag=dag
)

# Task 1: Extract from S3
extract_task = PythonOperator(
    task_id='extract_from_s3',
    python_callable=extract_from_s3,
    provide_context=True,
    dag=dag
)

# Task 2: Load to Redshift
load_task = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift,
    provide_context=True,
    dag=dag
)

# Set task dependencies
create_table_task >> extract_task >> load_task
