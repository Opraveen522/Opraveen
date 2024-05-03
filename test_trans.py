from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV to Uppercase") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("netflix_titles.csv", header=True)

# Apply transformation: Convert "description" column to uppercase
df_transformed = df.withColumn("description", upper(col("description")))

# Write transformed DataFrame to output location
output_path = "output.csv"
df_transformed.write.mode("overwrite").csv(output_path, header=True)

# Stop Spark session
spark.stop()
