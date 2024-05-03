from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV to Uppercase") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.option("header", True).csv("netflix_titles.csv")

# Apply transformation: Convert "description" column to uppercase
df_transformed = df.withColumn("description", upper(col("description")))

# Write transformed DataFrame to output location
df_transformed.write.mode("overwrite").csv("op_output.csv")

spark.stop()
