#!/bin/bash
echo "Waiting for MongoDB to start..."
sleep 3  # Give MongoDB time to initialize
python main.py
