
from pyspark.sql import SparkSession
import csv

def process_data_with_rdd(input_path, output_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("DemoSparkJob3").getOrCreate()

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
    input_path = "/home/ubuntu/DAS-PROJECT/input.csv"
    output_path = "/home/ubuntu/DAS-PROJECT/output.csv"
    process_data_with_rdd(input_path, output_path)
