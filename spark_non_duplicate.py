from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RemoveDuplicates") \
    .getOrCreate()

# Read the first CSV file into a DataFrame
df1 = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Read the second CSV file into a DataFrame
df2 = spark.read.csv("employee2.csv", header=True, inferSchema=True)

# Remove duplicates from the first DataFrame based on all columns
df1_no_duplicates = df1.dropDuplicates()

# Remove duplicates from the second DataFrame based on all columns
df2_no_duplicates = df2.dropDuplicates()

# Union the two DataFrames
combined_df = df1_no_duplicates.union(df2_no_duplicates)

# Write the combined DataFrame to a single CSV file
combined_df.write.csv("combined_employees_no_duplicates.csv", header=True)

# Stop Spark session
spark.stop()


