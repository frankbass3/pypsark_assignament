#!/bin/bash

# Step 1: Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Step 2: Run linting
echo "Running linting..."
flake8 src/ tests/

# Step 3: Run static analysis
echo "Running static analysis..."
pylint src/ tests/

# Step 4: Run unit tests
echo "Running unit tests..."
pytest tests/test.py

# Step 5: Build Docker image
echo "Building Docker image..."
docker build -t pyspark-app .

# Step 6: Run Docker container with Spark job
echo "Running Spark job in Docker..."
docker run --rm -v $(pwd):/app pyspark-app

# Optional: Clean up generated files
echo "Cleaning up..."
rm -rf __pycache__
