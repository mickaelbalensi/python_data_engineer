import pandas as pd
from elasticsearch import Elasticsearch
import json

from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'scheme': 'http', 'host': 'localhost', 'port': 9200}])

# Check connection
if es.ping():
    print("Connected to Elasticsearch")
else:
    print("Failed to connect to Elasticsearch")


# Load CSV file
data = pd.read_csv('data/spotify_songs.csv')

# Define the index and the mappings
index_name = 'songs'

# Check if the index exists, create if it doesn't
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, ignore=400, body={
        "mappings": {
            "properties": {
                "song_id": { "type": "integer" },
                "artist_name": { "type": "text" },
                "song_title": { "type": "text" },
                "mood": { "type": "keyword" },
                "energy": { "type": "float" },
                "popularity": { "type": "integer" }
            }
        }
    })

def calculate_mood(energy):
    if energy > 0.8:
        return 'happy'
    elif energy < 0.4:
        return 'sad'
    else:
        return 'neutral'

# Add mood enrichment to the data
data['mood'] = data['energy'].apply(calculate_mood)

# Index the data into Elasticsearch
for index, row in data.iterrows():
    song = row.to_dict()
    es.index(index=index_name, body=song)

print(f"Loaded {len(data)} songs into Elasticsearch.")
