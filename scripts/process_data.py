from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ProcessData") \
    .getOrCreate()

# Read raw data from Delta Lake
raw_delta_path = "data/delta_tables/raw_table"
df = spark.read.format("delta").load(raw_delta_path)

# Example: Filter and aggregate data
processed_df = df.filter(col("some_column") > 100) \
    .groupBy("another_column") \
    .count()

# Write processed data to Delta Lake
processed_delta_path = "data/delta_tables/processed_table"
processed_df.write.format("delta").mode("overwrite").save(processed_delta_path)
print(f"Processed data saved to Delta Lake at '{processed_delta_path}'!")