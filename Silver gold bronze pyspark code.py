from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -------------------------
# Step 1: Spark Session
# -------------------------
spark = SparkSession.builder.appName("EndToEnd_ETL_Pipeline_Azure").getOrCreate()

# -------------------------
# Step 2: Bronze Layer - Load Raw Data
# -------------------------
# Replace with your actual storage path
bronze_input_path = "abfss://raw@<your-storage-account>.dfs.core.windows.net/olympics/olympics.csv"
bronze_output_path = "abfss://bronze@<your-storage-account>.dfs.core.windows.net/olympics"

# Load CSV dropped by ADF
raw_df = spark.read.csv(bronze_input_path, header=True, inferSchema=True)

# Write to Bronze layer in Delta format
raw_df.write.format("delta").mode("overwrite").save(bronze_output_path)
print("Bronze Layer Completed")

# -------------------------
# Step 3: Silver Layer - Clean & Transform
# -------------------------
silver_output_path = "abfss://silver@<your-storage-account>.dfs.core.windows.net/olympics"

# Read from Bronze
bronze_df = spark.read.format("delta").load(bronze_output_path)

# Filter nulls and blank values from key columns
silver_df = bronze_df.filter(
    (col("Name").isNotNull()) & (col("Name") != "") &
    (col("Age").isNotNull()) &
    (col("Sex").isNotNull()) & (col("Sex") != "") &
    (col("Team").isNotNull()) & (col("Team") != "") &
    (col("Event").isNotNull()) & (col("Event") != "")
)

# Write to Silver layer
silver_df.write.format("delta").mode("overwrite").save(silver_output_path)
print("Silver Layer Completed")

# -------------------------
# Step 4: Gold Layer - Aggregation
# -------------------------
gold_output_path = "abfss://gold@<your-storage-account>.dfs.core.windows.net/olympics"

# Read from Silver
silver_df = spark.read.format("delta").load(silver_output_path)

# Aggregate example: Total participations by Team and Year
gold_df = silver_df.groupBy("Team", "Year").count().withColumnRenamed("count", "Total_Participations")

# Write to Gold layer
gold_df.write.format("delta").mode("overwrite").save(gold_output_path)
print("Gold Layer Completed")

# -------------------------
# Step 5: Done
# -------------------------
print("ETL Pipeline Finished Successfully!")
