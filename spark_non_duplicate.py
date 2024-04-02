from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.master("local[1]").appName("DuplicateRemoval").getOrCreate()

# Read the first CSV file
df1 = spark.read.csv("employees.csv")

# Read the second CSV file
df2 = spark.read.csv("employee2.csv")

# Remove duplicates from both DataFrames
df1_no_duplicates = df1.dropDuplicates()
df2_no_duplicates = df2.dropDuplicates()

# Combine the non-duplicate data from both DataFrames
final_df = df1_no_duplicates.union(df2_no_duplicates)

# Write the final non-duplicate data to a new CSV file
final_df.write.csv("non_dupli_records.csv", header=True)

# Stop the Spark session
spark.stop()

