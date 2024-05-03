from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV to Uppercase") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.option("header", True).csv("netflix_titles.csv")

# Apply transformation: Convert "description" column to uppercase
df_transformed = df.withColumn("description", upper(col("description")))

# Write transformed DataFrame to output location
output_path = "op_output.csv"
df_transformed.write.mode("overwrite").csv(output_path)

# Stop Spark session
spark.stop()

# Remove ".crc" suffix from the output file name
if os.path.isfile(output_path + ".crc"):
    os.remove(output_path + ".crc")
