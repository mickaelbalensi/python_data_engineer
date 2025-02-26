import boto3

client = boto3.client('redshift-data', region_name="eu-north-1")

response = client.execute_statement(
    Database="dev",
    WorkgroupName="spotify-etl-workgroup",
    Sql="INSERT INTO test_data (id, name, age) VALUES (5, 'Tomer', 41');"
)

print(response)