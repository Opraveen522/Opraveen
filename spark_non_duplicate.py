from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RemoveDuplicates") \
    .getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("/home/opraveen/spark_files/employees.csv", header=True, inferSchema=True)

# Remove duplicates based on all columns
df_no_duplicates = df.dropDuplicates()

# Write the DataFrame without duplicates to a new CSV file
df_no_duplicates.write.csv("/home/opraveen/spark_files/employees_no_duplicates.csv", header=True)

# Stop Spark session
spark.stop()

