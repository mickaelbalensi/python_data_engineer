from elasticsearch import Elasticsearch

# Define the Elasticsearch connection with scheme, host, and port
es = Elasticsearch([{'scheme': 'http', 'host': 'localhost', 'port': 9200}])

# Check if the connection is successful
if es.ping():
    print("Connected to Elasticsearch")
else:
    print("Could not connect to Elasticsearch")
