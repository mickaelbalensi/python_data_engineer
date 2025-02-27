import boto3
import pandas as pd
import io
import os
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get environment variables
s3_bucket = os.environ.get('S3_BUCKET', 'spotify-streaming-data')
s3_key = os.environ.get('S3_KEY', 'kpi_genre_duration/')
process_single_file = os.environ.get('PROCESS_SINGLE_FILE', 'false').lower() == 'true'
dynamodb_table = os.environ.get('DYNAMODB_TABLE', 'spotify_genre_duration')
dynamodb_endpoint = os.environ.get('DYNAMODB_ENDPOINT', 'http://localhost:8000')
use_local_dynamodb = os.environ.get('USE_LOCAL_DYNAMODB', 'true').lower() == 'true'
use_real_s3 = os.environ.get('USE_REAL_S3', 'true').lower() == 'true'

# AWS credentials from environment variables
aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')

def fetch_data_from_dynamodb(table_name, dynamodb):
    """Fetches and prints data from the specified DynamoDB table."""
    try:
        table = dynamodb.Table(table_name)
        response = table.scan()
        items = response.get('Items', [])
        
        if not items:
            logger.info(f"No records found in {table_name}")
        else:
            logger.info(f"Retrieved {len(items)} records from {table_name}")
            for item in items:
                logger.info(item)  # Print each item

    except Exception as e:
        logger.error(f"Error retrieving data from {table_name}: {str(e)}")


# Call this function after loading data into DynamoDB

def main():
    try:
        logger.info("Starting S3 to DynamoDB transfer process")
        
        # Initialize S3 client - using real AWS credentials
        s3_client_args = {
            'region_name': aws_region
        }
        
        # Only add credentials if they're provided
        if aws_access_key and aws_secret_key and use_real_s3:
            s3_client_args.update({
                'aws_access_key_id': aws_access_key,
                'aws_secret_access_key': aws_secret_key
            })
        
        s3_client = boto3.client('s3', **s3_client_args)
        
        # Initialize DynamoDB client
        dynamodb_args = {
            'region_name': aws_region
        }
        
        # Use local endpoint for DynamoDB if specified
        if use_local_dynamodb:
            logger.info(f"Using local DynamoDB at {dynamodb_endpoint}")
            dynamodb_args.update({
                'endpoint_url': dynamodb_endpoint,
                'aws_access_key_id': 'dummy',
                'aws_secret_access_key': 'dummy'
            })
        elif aws_access_key and aws_secret_key:
            dynamodb_args.update({
                'aws_access_key_id': aws_access_key,
                'aws_secret_access_key': aws_secret_key
            })
        
        # Wait for DynamoDB to be ready if using local
        if use_local_dynamodb:
            logger.info("Waiting for local DynamoDB to start...")
            time.sleep(5)
        
        dynamodb = boto3.resource('dynamodb', **dynamodb_args)
        
        # Determine files to process
        files_to_process = []
        
        if process_single_file and not s3_key.endswith('/'):
            # If given a specific file, just process that one file
            logger.info(f"Processing single file: s3://{s3_bucket}/{s3_key}")
            files_to_process.append(s3_key)
        else:
            # Otherwise, list files in the bucket/prefix
            logger.info(f"Listing files in s3://{s3_bucket}/{s3_key}")
            response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key)
            
            if 'Contents' not in response:
                logger.error(f"No files found in s3://{s3_bucket}/{s3_key}")
                return
            
            # If processing single file is enabled, get the latest file by LastModified
            if process_single_file:
                latest_file = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)[0]
                files_to_process.append(latest_file['Key'])
                logger.info(f"Using most recent file: {files_to_process[0]}")
            else:
                # Otherwise process all files
                files_to_process = [item['Key'] for item in response['Contents']]
        
        # Create the DynamoDB table if it doesn't exist
        existing_tables = dynamodb.meta.client.list_tables()['TableNames']
        
        if dynamodb_table not in existing_tables:
            logger.info(f"Creating DynamoDB table: {dynamodb_table}")
            table = dynamodb.create_table(
                TableName=dynamodb_table,
                KeySchema=[
                    {'AttributeName': 'id', 'KeyType': 'HASH'}  # Partition key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'id', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            # Wait for table creation
            logger.info("Waiting for table creation...")
            table.meta.client.get_waiter('table_exists').wait(TableName=dynamodb_table)
        else:
            logger.info(f"Table {dynamodb_table} already exists")
            table = dynamodb.Table(dynamodb_table)
        
        # Process each file
        for file_key in files_to_process:
            if not file_key.endswith('.csv') and not file_key.endswith('-00000'):
                logger.info(f"Skipping non-data file: {file_key}")
                continue
                
            logger.info(f"Processing file: {file_key}")
            
            try:
                # Get the file from S3
                response = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
                file_content = response['Body'].read().decode('utf-8')
                
                # Check if file is empty
                if not file_content.strip():
                    logger.warning(f"File {file_key} is empty, skipping")
                    continue
                
                # Try to parse as CSV
                try:
                    df = pd.read_csv(io.StringIO(file_content))
                except Exception as e:
                    logger.warning(f"Error parsing {file_key} as CSV: {str(e)}")
                    # Try tab-delimited or other formats
                    try:
                        df = pd.read_csv(io.StringIO(file_content), sep='\t')
                    except Exception:
                        logger.error(f"Unable to parse {file_key} in any recognized format")
                        continue
                
                # If DataFrame doesn't have an 'id' column, add one
                if 'id' not in df.columns:
                    logger.info("Adding 'id' column to DataFrame")
                    # Use file name as prefix for IDs to avoid conflicts between files
                    file_prefix = os.path.basename(file_key).split('.')[0]
                    df['id'] = [f"{file_prefix}_{i}" for i in range(len(df))]
                
                # Log DataFrame info
                logger.info(f"DataFrame columns: {df.columns.tolist()}")
                logger.info(f"DataFrame shape: {df.shape}")
                
                # Load data into DynamoDB
                logger.info(f"Loading {len(df)} items into DynamoDB")
                with table.batch_writer() as batch:
                    for _, row in df.iterrows():
                        # Convert row to dictionary and handle data types
                        item = {}
                        for col, val in row.items():
                            # Convert NaN to None
                            if pd.isna(val):
                                item[col] = None
                            # Convert numbers to strings
                            elif isinstance(val, (float, int)):
                                item[col] = str(val)
                            else:
                                item[col] = val
                        batch.put_item(Item=item)
                
                logger.info(f"Successfully loaded data from {file_key} to DynamoDB")
                
            except Exception as e:
                logger.error(f"Error processing file {file_key}: {str(e)}")
                # Continue with other files
                continue
        
        logger.info("S3 to DynamoDB transfer process completed successfully")
    
        # Fetch and print data from DynamoDB
        fetch_data_from_dynamodb(dynamodb_table, dynamodb)

    except Exception as e:
        logger.error(f"Error in S3 to DynamoDB transfer: {str(e)}")
        raise

    
if __name__ == "__main__":
    main()