"""
This script performs the transformation of contract and claim data into a transaction format.
It retrieves configuration from a YAML file, processes the data, and outputs the result.
"""

import logging
import time
import requests
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, current_timestamp, to_date, to_timestamp, udf
from pyspark.sql.types import StringType, IntegerType, DecimalType

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration from YAML file
def load_config(env="dev"):
    """
    Load configuration from the YAML file based on the environment.
    """
    try:
        config_path = f"config/config_{env}.yaml"  # Load environment-specific config
        with open(config_path, "r", encoding="utf-8") as file:  # Specify encoding
            return yaml.safe_load(file)
    except FileNotFoundError as e:
        logger.error("Configuration file not found: %s", e)
        raise

# Load the configuration
config = load_config()

# Extract configuration values
service_url = config['service']['url']
source_data_contract = config['paths']['source_data_contract']
source_data_claim = config['paths']['source_data_claim']
target_data_path = config['paths']['target_data']
contract_schema = config['schema']['contract_schema']
claim_schema = config['schema']['claim_schema']

# Initialize Spark session
spark = SparkSession.builder.appName("ContractClaimTransformation").getOrCreate()

# Define the function to call the REST API to get the unique NSE_ID
def get_nse_id(claim_id, retries=3):
    """
    Get the unique NSE_ID for a given claim ID by making an API call.
    Retries the request if it fails.
    """
    url = f"{service_url}{claim_id}"
    try:
        logger.info("Making request to %s", url)
        for _ in range(retries):
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                logger.info("Successfully retrieved NSE_ID for %s", claim_id)
                return response.json().get("Digest")
            logger.error("Request failed with status code %s for claim_id %s", response.status_code, claim_id)
            time.sleep(2)  # Wait before retrying
        return None
    except requests.exceptions.Timeout:
        logger.error("Request to %s timed out", url)
        return None
    except requests.exceptions.RequestException as e:
        logger.error("Network error: %s", e)
        return None

# Register the UDF to use the API function within Spark
nse_id_udf = udf(get_nse_id, StringType())

# Load data dynamically from CSV files (using paths from config)
contract_df = spark.read.option("header", "true").csv(source_data_contract)
claim_df = spark.read.option("header", "true").csv(source_data_claim)

# Validate contract schema against the expected contract schema
for col_name in contract_schema:
    if col_name not in contract_df.columns:
        raise ValueError(f"Missing expected column '{col_name}' in contract data")

# Validate claim schema against the expected claim schema
for col_name in claim_schema:
    if col_name not in claim_df.columns:
        raise ValueError(f"Missing expected column '{col_name}' in claim data")

# Transformation logic to create transactions
def transform_data(contract_df, claim_df):
    """
    Transform the contract and claim DataFrames into a transaction DataFrame.
    """
    return claim_df.join(
        contract_df, 
        (claim_df.CONTRACT_ID == contract_df.CONTRACT_ID) & 
        (claim_df.CONTRACT_SOURCE_SYSTEM == contract_df.SOURCE_SYSTEM), 
        "inner"
    ).select(
        # Set the CONTRACT_SOURCE_SYSTEM to "Europe 3" (hardcoded, assuming this is a default value)
        when(claim_df.CONTRACT_SOURCE_SYSTEM.isNotNull(), "Europe 3").alias("CONTRACT_SOURCE_SYSTEM"),

        # CONTRACT_ID as CONTRACT_SOURCE_SYSTEM_ID
        contract_df.CONTRACT_ID.alias("CONTRACT_SOURCE_SYSTEM_ID"),

        # CLAIM_ID without the prefix "CL", cast to integer
        (claim_df.CLAIM_ID.substr(3, 100)).cast(IntegerType()).alias("SOURCE_SYSTEM_ID"),

        # Map CLAIM_TYPE to TRANSACTION_TYPE
        when(claim_df.CLAIM_TYPE == "2", "Corporate")
        .when(claim_df.CLAIM_TYPE == "1", "Private")
        .otherwise("Unknown").alias("TRANSACTION_TYPE"),

        # Set TRANSACTION_DIRECTION based on CLAIM_ID prefix
        when(claim_df.CLAIM_ID.startswith("CL"), "COINSURANCE")
        .when(claim_df.CLAIM_ID.startswith("RX"), "REINSURANCE")
        .otherwise(None).alias("TRANSACTION_DIRECTION"),

        # CONFORMED_VALUE is the AMOUNT from the claim data
        claim_df.AMOUNT.cast(DecimalType(16, 5)).alias("CONFORMED_VALUE"),

        # BUSINESS_DATE is the DATE_OF_LOSS, converted to date format
        to_date(claim_df.DATE_OF_LOSS, "dd.MM.yyyy").alias("BUSINESS_DATE"),

        # CREATION_DATE is the CREATION_DATE from claim data, converted to timestamp
        to_timestamp(claim_df.CREATION_DATE, "dd.MM.yyyy HH:mm:ss").alias("CREATION_DATE"),

        # SYSTEM_TIMESTAMP is the current timestamp when the transformation occurs
        current_timestamp().alias("SYSTEM_TIMESTAMP"),

        # NSE_ID using the API function
        nse_id_udf(claim_df.CLAIM_ID).alias("NSE_ID")
    )

# Perform the transformation
transaction_df = transform_data(contract_df, claim_df)

# Write the result to a file (CSV format)
transaction_df.write.option("header", "true").mode("overwrite").csv(target_data_path)

# Stop the Spark session
spark.stop()
