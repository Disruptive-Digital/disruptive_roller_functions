#!/bin/bash

# Build the Docker image
echo "Building Docker image..."
docker build -t api-to-bigquery .

# Run the container
echo "Starting container..."
docker run -p 8080:8080 \
  --name api-to-bigquery-container \
  api-to-bigquery