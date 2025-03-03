from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

# Check if connection is successful
if es.ping():
    print("Connected to Elasticsearch")
else:
    print("Failed to connect to Elasticsearch")

# 1. Aggregation Query: Mood Counts
mood_aggregation_query = {
    "aggs": {
        "mood_count": {
            "terms": {
                "field": "mood",
                "size": 10
            }
        }
    }
}

# Execute the query
response = es.search(index="songs", body=mood_aggregation_query)
print("Mood Counts:")
for bucket in response['aggregations']['mood_count']['buckets']:
    print(f"{bucket['key']}: {bucket['doc_count']}")

# 2. Sort Query: Highest Energy Songs
energy_sort_query = {
    "query": {
        "match_all": {}
    },
    "sort": [
        {"energy": {"order": "desc"}}
    ]
}

response = es.search(index="songs", body=energy_sort_query)
print("\nHighest Energy Songs:")
for hit in response['hits']['hits']:
    print(hit['_source'])

# 3. Full-text Search: Artist Queries
artist_query = {
    "query": {
        "match": {
            "artist_name": "Ed Sheeran"
        }
    }
}

response = es.search(index="songs", body=artist_query)
print("\nArtist Search Results:")
for hit in response['hits']['hits']:
    print(hit['_source'])

# 4. Statistics Query: Popularity Metrics
popularity_aggregation_query = {
    "aggs": {
        "average_popularity": {
            "avg": {
                "field": "popularity"
            }
        }
    }
}

response = es.search(index="songs", body=popularity_aggregation_query)
print("\nAverage Popularity:")
print(response['aggregations']['average_popularity']['value'])
