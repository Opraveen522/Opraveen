from pyspark.sql import SparkSession

def remove_duplicates(file1_path, file2_path, output_path):
    # Create a Spark session
    spark = SparkSession.builder.master("local[1]").appName("DuplicateRemoval").getOrCreate()

    # Read the first CSV file
    df1 = spark.read.csv(file1_path)

    # Read the second CSV file
    df2 = spark.read.csv(file2_path)

    # Remove duplicates from both DataFrames
    df1_no_duplicates = df1.dropDuplicates()
    df2_no_duplicates = df2.dropDuplicates()

    # Combine the non-duplicate data from both DataFrames
    final_df = df1_no_duplicates.union(df2_no_duplicates)

    # Write the final non-duplicate data to a new CSV file
    final_df.write.csv(output_path, header=True)

    # Stop the Spark session
    spark.stop()

# Example usage:
if __name__ == "__main__":
    file1_path = "employees.csv"
    file2_path = "employee2.csv"
    output_path = "non_dupli_records_function_test.csv"
    remove_duplicates(file1_path, file2_path, output_path)
