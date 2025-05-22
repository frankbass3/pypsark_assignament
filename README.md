# SparkEngineer SwissRe Test Project

This project is a PySpark-based application designed to process and transform insurance-related datasets such as contracts and claims. The project is containerized using Docker and includes testing, automation, and configuration management features.

---

## 📁 Project Structure
```plaintext
/F
├── Dockerfile                       # Docker configuration for containerizing the app
├── make.sh                          # Shell script to automate tasks like installation, linting, testing
├── README.md                        # Project documentation
├── requirements.txt                 # Python dependencies for the project
├── SparkEngineer_SwissRe_Test (1).pdf # Placeholder for project-specific documents

├── config
│   └── config.yaml                  # Config file for dynamic parameters (e.g., URLs, paths)

├── data
│   ├── claims.csv                   # Input file for claims data
│   └── contracts.csv                # Input file for contracts data

├── src
│   └── main.py                      # Entry point for Spark job and transformations

└── tests
    ├── unit
    │   └── test_main.py             # Unit tests for main.py logic
    └── integration
        └── test_integration.py      # Integration tests for external interactions



---

## 🧠 Directory Overview

- **`src/`**: Contains the core logic for transformations and Spark jobs.
- **`tests/`**: Contains unit and integration tests to ensure code reliability.
- **`config/`**: Includes external configuration like file paths and schemas.
- **`data/`**: Sample datasets for local testing and development.

---

## 📦 Dependency Management

Dependencies are managed with `pip` and listed in `requirements.txt`:

```text
pyspark==3.2.1
requests==2.26.0
pytest==6.2.5
flake8==3.9.2
pylint==2.9.6

Configuration Management
External configs enable flexibility without changing source code. Example config/config.yaml:

service:
  url: "https://api.hashify.net/hash/md4/hex?value="

paths:
  source_data: "/data/contracts.csv"
  target_data: "/data/transactions.parquet"

schema:
  contract_schema:
    - SOURCE_SYSTEM
    - CONTRACT_ID
    - CONTRACT_TYPE
    - INSURED_PERIOD_FROM
    - INSURED_PERIOD_TO
    - CREATION_DATE

  claim_schema:
    - SOURCE_SYSTEM
    - CLAIM_ID
    - CONTRACT_SOURCE_SYSTEM
    - CONTRACT_ID
    - CLAIM_TYPE
    - DATE_OF_LOSS
    - AMOUNT
    - CREATION_DATE


## 🚀 Automation and CI/CD Pipeline

Although you're not explicitly building a CI/CD pipeline, the goal is to ensure that the entire process—from dependency management to deployment—can be automated with a single command.

### Steps to Automate

1. **Install dependencies:**  
   Use pip to install the required packages:


2. **Run unit tests:**  
Use pytest to run unit tests and ensure code works as expected:


3. **Static analysis and linting:**
   **Static analysis:** Run flake8 or pylint to ensure code quality.
  
  flake8 src/
  pylint src/

All these tasks can be automated in the `make.sh` script for convenience and repeatability.

## 🧪 Testing Strategy

Testing ensures code quality and data correctness. The strategy includes:

- **Code Quality Tests:**
- **Unit tests:** Test individual components of the code (e.g., transformations, data processing functions).  
 (inside the `make.sh`)
- **Static analysis:** Use tools like pylint, flake8, or black to ensure that the code follows best practices and style guides.  
 (inside the `make.sh`)

---

## 🐳 Components of the Dockerfile

- **Base Image:**  
Starts from the official Bitnami Spark image (`bitnami/spark:3.2.1`), which includes a Spark installation. This image is optimized for running Spark in Docker containers.

- **Environment Variables:**
- `SPARK_HOME` and `PATH` are set to ensure Spark is available for PySpark.
- `PYSPARK_PYTHON=python3` ensures Python 3 is used.

- **Working Directory:**  
The working directory is set to `/app`, which is where your application code will reside in the container.

- **Install Dependencies:**  
The `requirements.txt` file is copied into the container, and dependencies are installed using `pip install`.

- **Copy Application Code:**  
All the application files are copied into the container.

- **ENTRYPOINT:**  
The entry point is set to run the PySpark job using `spark-submit`. The `--master local[*]` runs the job locally using all available CPU cores, and `--deploy-mode client` ensures the driver runs in the client mode (on the same machine as the executor), which is typical for local development.

`/app/src/main.py` is the path to your PySpark job script that should be executed.


