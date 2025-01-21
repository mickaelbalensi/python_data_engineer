# app/main.py
from pymongo import MongoClient
import os
import time

def main():
    # MongoDB connection with retry logic
    max_retries = 5
    for attempt in range(max_retries):
        try:
            client = MongoClient('mongodb://root:example@mongo:27017/')
            db = client.testdb
            
            # Test the connection
            result = db.test.insert_one({"message": "Hello from Python!"})
            print(f"Inserted document with id: {result.inserted_id}")
            
            # Read it back
            doc = db.test.find_one({"message": "Hello from Python!"})
            print(f"Found document: {doc}")
            break
            
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Failed to connect to MongoDB after {max_retries} attempts")
                raise
            print(f"Attempt {attempt + 1} failed, retrying...")
            time.sleep(2)

if __name__ == "__main__":
    main()