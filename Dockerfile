# Use official Spark base image
FROM bitnami/spark:3.2.1

# Set environment variables for Spark
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt into the container
COPY requirements.txt /app/

# Install Python dependencies inside the container
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application code into the container
COPY . /app/

# Set the entry point to run the PySpark job with a spark-submit command
ENTRYPOINT ["spark-submit", "--master", "local[*]", "--deploy-mode", "client", "/app/src/main.py"]
