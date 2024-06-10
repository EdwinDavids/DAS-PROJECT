
from pyspark.sql import SparkSession
import csv

def process_data_with_rdd(input_path, output_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("RDDProcessing").getOrCreate()

    try:
        # Read CSV file into DataFrame
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        # Filter DataFrame to keep records where age > 30
        filtered_df = df.filter(df['age'] > 30)

        # Save filtered DataFrame as CSV
        filtered_df.write.csv(output_path, mode='overwrite', header=True)

        print("RDD processing successful")
    except Exception as e:
        print(f"Error processing RDD: {e}")
    # finally:
        # Stop the Spark session
        spark.stop()

# Usage example
if __name__ == "__main__":
    input_path = "/home/edwindavid/PycharmProjects/newhive/Sparkprojects/Intership/input.csv"
    output_path = "/home/edwindavid/PycharmProjects/newhive/Sparkprojects/Intership/output.csv"
    process_data_with_rdd(input_path, output_path)

# a=5
# b=6
# print(a+b)





###################

# import csv
# import random
#
# # Sample data for employees
# employees = [
#     {"name": "Alice", "age": 28, "department": "Engineering", "salary": 80000},
#     {"name": "Bob", "age": 35, "department": "Sales", "salary": 60000},
#     {"name": "Charlie", "age": 40, "department": "Marketing", "salary": 75000},
#     {"name": "David", "age": 32, "department": "Engineering", "salary": 85000},
#     {"name": "Eve", "age": 29, "department": "HR", "salary": 70000}
# ]
#
# # Path to the CSV file
# csv_file_path = "input.csv"
#
# # Writing sample data to CSV file
# with open(csv_file_path, "w", newline="") as csvfile:
#     fieldnames = ["name", "age", "department", "salary"]
#     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#
#     # Write header
#     writer.writeheader()
#
#     # Write rows
#     for employee in employees:
#         writer.writerow(employee)
#
# print(f"Sample data written to {csv_file_path}")
