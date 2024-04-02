# test_spark_non_duplicate.py

import pytest
from spark_non_duplicate import remove_duplicates

import pyspark
pyspark.sql.SparkSession.builder \
    .appName("MyApp") \
    .config('spark.logConf', 'true') \
    .config('spark.logLevel', 'ERROR') \
    .getOrCreate()

# Define test cases

def test_remove_duplicates():
    # Define your test data and expected output
    input_data = [1, 2, 3, 1, 2]
    expected_output = [1, 2, 3]

    # Call the function you want to test
    result = remove_duplicates(input_data)

    # Assert that the result matches the expected output
    assert result == expected_output

# Add more test cases as needed

