from elasticsearch import Elasticsearch
import pandas as pd

# Set up connection to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Sample data
data = {
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'city': ['New York', 'Los Angeles', 'Chicago']
}

# Convert the data to a pandas DataFrame
df = pd.DataFrame(data)

# Index each row in Elasticsearch
for index, row in df.iterrows():
    document = row.to_dict()
    es.index(index='people', document=document)

print("Data indexed successfully!")
