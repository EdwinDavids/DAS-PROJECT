from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, desc, rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ComplexSparkJob") \
    .getOrCreate()

# Sample data for testing
data = [
    (1, "2024-01-01", "product1", 100.0, 2),
    (2, "2024-01-01", "product2", 150.0, 3),
    (3, "2024-01-02", "product1", 200.0, 1),
    (4, "2024-01-02", "product3", 300.0, 5),
    (5, "2024-01-03", "product2", 400.0, 2),
    (6, "2024-01-03", "product3", 500.0, 1)
]

# Define schema
schema = ["id", "date", "product", "price", "quantity"]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the initial data
print("Initial Data:")
df.show()

# Transformation 1: Calculate total revenue for each product
df_revenue = df.withColumn("total_revenue", col("price") * col("quantity"))

print("After calculating total revenue:")
df_revenue.show()

# Transformation 2: Calculate average price for each product
df_avg_price = df.groupBy("product").agg(avg("price").alias("avg_price"))

print("Average price per product:")
df_avg_price.show()

# Transformation 3: Calculate total quantity sold for each product
df_total_quantity = df.groupBy("product").agg(sum("quantity").alias("total_quantity"))

print("Total quantity sold per product:")
df_total_quantity.show()

# Transformation 4: Combine average price and total quantity into one DataFrame
df_combined = df_avg_price.join(df_total_quantity, "product")

print("Combined average price and total quantity per product:")
df_combined.show()

# Transformation 5: Calculate total revenue per product and date
df_total_revenue_per_date = df_revenue.groupBy("date", "product").agg(sum("total_revenue").alias("total_revenue"))

print("Total revenue per product and date:")
df_total_revenue_per_date.show()

# Transformation 6: Find the top-selling product by revenue for each day
windowSpec = Window.partitionBy("date").orderBy(desc("total_revenue"))
df_top_selling = df_total_revenue_per_date.withColumn("rank", rank().over(windowSpec)).filter(col("rank") == 1).drop("rank")

print("Top selling product by revenue for each day:")
df_top_selling.show()

# Stop the Spark session
spark.stop()