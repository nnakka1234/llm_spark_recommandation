# Databricks notebook source
# MAGIC %md
# MAGIC ## ✅ Version 2: Without Redundant Reads/Writes

# COMMAND ----------

from pyspark.sql.functions import rand
from pyspark.sql.types import IntegerType

# Create large fact table and use it directly
fact_sales = spark.range(0, 10_000_000).withColumn("region_id", (rand() * 10).cast(IntegerType()))

# Create small dimension table
dim_region = spark.createDataFrame(
    [(i, f"Region_{i}") for i in range(10)],
    ["region_id", "region_name"]
)

# Join directly without unnecessary I/O
df_joined = fact_sales.join(dim_region, "region_id")
print("🚀 Count result (optimized IO):")
print(df_joined.count())

# Show physical plan
df_joined.explain(True)