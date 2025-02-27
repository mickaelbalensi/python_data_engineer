# check_dynamodb.py
import boto3
import json

# Initialize DynamoDB client
dynamodb = boto3.resource(
    'dynamodb',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='dummy',
    aws_secret_access_key='dummy',
    region_name='us-east-1'
)

# Specify table name
table_name = 'spotify_genre_duration'
table = dynamodb.Table(table_name)

# Scan the table (for small tables only - not recommended for production with large data)
response = table.scan()
items = response['Items']

# Print the total number of items
print(f"Total items in {table_name}: {len(items)}")

# Print the first few items
print("\nSample items:")
for item in items[:5]:  # Show first 5 items
    print(json.dumps(item, indent=2))