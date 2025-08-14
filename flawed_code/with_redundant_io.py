# Databricks notebook source
# MAGIC %md
# MAGIC ## ❌ Version 1: With Redundant Reads/Writes

# COMMAND ----------

from pyspark.sql.functions import rand
from pyspark.sql.types import IntegerType

# Create and write large fact table
fact_sales = spark.range(0, 10_000_000).withColumn("region_id", (rand() * 10).cast(IntegerType()))
fact_sales.write.mode("overwrite").saveAsTable("fact_sales")

# Read the same fact table again (redundant)
fact_sales_1 = spark.read.table("fact_sales")
fact_sales_2 = spark.read.table("fact_sales")  # redundant read

# Write intermediate table unnecessarily
fact_sales_1.write.mode("overwrite").saveAsTable("fact_sales_temp")

# Read intermediate table again (redundant)
fact_sales_final = spark.read.table("fact_sales_temp")

# Create small dimension table
dim_region = spark.createDataFrame(
    [(i, f"Region_{i}") for i in range(10)],
    ["region_id", "region_name"]
)
dim_region.write.mode("overwrite").saveAsTable("dim_region")

# Redundant write
dim_region.write.mode("overwrite").saveAsTable("dim_region_copy")

# Join
df_joined = fact_sales_final.join(dim_region, "region_id")
print("🔄 Count result (redundant IO):")
print(df_joined.count())

# Show physical plan
df_joined.explain(True)