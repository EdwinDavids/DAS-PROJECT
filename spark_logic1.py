from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, sum
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ComplexSparkJob") \
        .getOrCreate()

    # Define schema for the input data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True),
        StructField("department", StringType(), True)
    ])

    # Create a sample dataframe
    data = [
        (1, "Alice", 34, 50000, "HR"),
        (2, "Bob", 45, 60000, "Finance"),
        (3, "Charlie", 23, 45000, "IT"),
        (4, "David", 29, 70000, "HR"),
        (5, "Eve", 41, 65000, "Finance"),
        (6, "Frank", 36, 40000, "IT"),
        (7, "Grace", 28, 75000, "HR"),
        (8, "Hank", 50, 80000, "Finance"),
        (9, "Ivy", 26, 48000, "IT"),
        (10, "Jack", 38, 55000, "HR")
    ]

    df = spark.createDataFrame(data, schema)

    # Show the initial dataframe
    print("Initial DataFrame:")
    df.show()

    # Filter rows where age is greater than 30
    df_filtered = df.filter(col("age") > 30)
    print("Filtered DataFrame (age > 30):")
    df_filtered.show()

    # Group by department and calculate aggregate statistics
    df_grouped = df_filtered.groupBy("department").agg(
        count("id").alias("count"),
        avg("age").alias("average_age"),
        sum("salary").alias("total_salary"),
        max("salary").alias("max_salary"),
        min("salary").alias("min_salary")
    )

    print("Grouped DataFrame with Aggregate Statistics:")
    df_grouped.show()

    # Sort the grouped dataframe by total salary in descending order
    df_sorted = df_grouped.sort(col("total_salary").desc())
    print("Sorted Grouped DataFrame by Total Salary (Descending):")
    df_sorted.show()

    # Save the final dataframe to a CSV file
    output_path = "/home/edwindavid/PycharmProjects/newhive/Sparkprojects/Intership/spark_job1_oot.csv"
    df_sorted.write.csv(output_path, header=True)
    print(f"Output written to: {output_path}")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
