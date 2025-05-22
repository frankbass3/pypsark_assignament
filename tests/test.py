import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from unittest.mock import patch
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from main import transform_data

# Create a fixture to initialize the Spark session for testing
@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local").appName("Test").getOrCreate()
    yield spark_session
    spark_session.stop()

# Test for validating schema of contract and claim data
def test_schema_validation(spark):
    """
    Test schema validation for contract and claim data.
    """
    contract_data = [
        ("Contract_SR_Europa_3", 408124123, "Direct", "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
        ("Contract_SR_Europa_3", 46784575, "Direct", "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
    ]
    claim_data = [
        ("Claim_SR_Europa_3", "CL_68545123", "Contract_SR_Europa_3", 97563756, "2", "14.02.2021", 523.21, "17.01.2022 14:45"),
        ("Claim_SR_Europa_3", "CL_962234", "Contract_SR_Europa_4", 408124123, "1", "30.01.2021", 52369.0, "17.01.2022 14:46"),
    ]
    contract_columns = ["SOURCE_SYSTEM", "CONTRACT_ID", "CONTRACT_TYPE", "INSURED_PERIOD_FROM", "INSURED_PERIOD_TO", "CREATION_DATE"]
    claim_columns = ["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID", "CLAIM_TYPE", "DATE_OF_LOSS", "AMOUNT", "CREATION_DATE"]

    contract_df = spark.createDataFrame(contract_data, contract_columns)
    claim_df = spark.createDataFrame(claim_data, claim_columns)

    expected_contract_schema = set(["SOURCE_SYSTEM", "CONTRACT_ID", "CONTRACT_TYPE", "INSURED_PERIOD_FROM", "INSURED_PERIOD_TO", "CREATION_DATE"])
    assert set(contract_df.columns) == expected_contract_schema, f"Contract schema doesn't match. Found: {contract_df.columns}"

    expected_claim_schema = set(["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID", "CLAIM_TYPE", "DATE_OF_LOSS", "AMOUNT", "CREATION_DATE"])
    assert set(claim_df.columns) == expected_claim_schema, f"Claim schema doesn't match. Found: {claim_df.columns}"

# Test transformation logic (TRANSACTION_TYPE, TRANSACTION_DIRECTION, etc.)
def test_transformation_logic(spark):
    # Sample contract and claim data
    contract_data = [
        ("Contract_SR_Europa_3", 408124123, "Direct", "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
        ("Contract_SR_Europa_3", 46784575, "Direct", "01.01.2015", "01.01.2099", "17.01.2022 13:42")
    ]
    claim_data = [
        ("Claim_SR_Europa_3", "CL_68545123", "Contract_SR_Europa_3", 97563756, "2", "14.02.2021", 523.21, "17.01.2022 14:45"),
        ("Claim_SR_Europa_3", "CL_962234", "Contract_SR_Europa_4", 408124123, "1", "30.01.2021", 52369.0, "17.01.2022 14:46")
    ]
    contract_columns = ["SOURCE_SYSTEM", "CONTRACT_ID", "CONTRACT_TYPE", "INSURED_PERIOD_FROM", "INSURED_PERIOD_TO", "CREATION_DATE"]
    claim_columns = ["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID", "CLAIM_TYPE", "DATE_OF_LOSS", "AMOUNT", "CREATION_DATE"]

    contract_df = spark.createDataFrame(contract_data, contract_columns)
    claim_df = spark.createDataFrame(claim_data, claim_columns)

    # Perform the transformation using the transform_data function from main.py
    transaction_df = transform_data(contract_df, claim_df)

    # Validate some transformation logic
    result_df = transaction_df.select("TRANSACTION_TYPE", "TRANSACTION_DIRECTION")

    # Assert that values are correctly transformed
    assert result_df.collect()[0]["TRANSACTION_TYPE"] == "Corporate"
    assert result_df.collect()[0]["TRANSACTION_DIRECTION"] == "COINSURANCE"

# Test invalid data handling (e.g., missing or malformed data)
def test_invalid_data_handling(spark):
    # Data with missing CLAIM_ID
    claim_data_invalid = [
        ("Claim_SR_Europa_3", None, "Contract_SR_Europa_3", 97563756, "2", "14.02.2021", 523.21, "17.01.2022 14:45")
    ]
    claim_columns = ["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID", "CLAIM_TYPE", "DATE_OF_LOSS", "AMOUNT", "CREATION_DATE"]

    claim_df_invalid = spark.createDataFrame(claim_data_invalid, claim_columns)

    # Handle invalid data (assuming you have a function to handle missing data)
    claim_df_validated = claim_df_invalid.dropna(subset=["CLAIM_ID"])

    # Assert that the row with None CLAIM_ID is removed
    assert claim_df_validated.count() == 0

# Mock the external API call for unit testing
@patch('requests.get')
def test_get_nse_id_mock(mock_get, spark):
    # Mock the response to simulate a successful API call
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"Digest": "mocked_nse_id_12345"}

    # Sample data for testing
    claim_data = [
        ("Claim_SR_Europa_3", "CL_68545123", "Contract_SR_Europa_3", 97563756, "2", "14.02.2021", 523.21, "17.01.2022 14:45")
    ]
    claim_columns = ["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID", "CLAIM_TYPE", "DATE_OF_LOSS", "AMOUNT", "CREATION_DATE"]

    claim_df = spark.createDataFrame(claim_data, claim_columns)

    # Add a UDF for the mock API call
    nse_id_udf = udf(lambda claim_id: 'mocked_nse_id_12345', StringType())

    # Apply the mock UDF
    claim_df_with_nse_id = claim_df.withColumn("NSE_ID", nse_id_udf(claim_df.CLAIM_ID))

    # Check the results
    assert claim_df_with_nse_id.collect()[0]["NSE_ID"] == "mocked_nse_id_12345"
