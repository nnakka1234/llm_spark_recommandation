# Databricks notebook source
# MAGIC %md
# MAGIC ## ✅ Version 2: With Broadcast Join - Optimized

# COMMAND ----------

from pyspark.sql.functions import rand, broadcast
from pyspark.sql.types import IntegerType

# Create large fact table (~10M rows)
fact_sales = spark.range(0, 10_000_000).withColumn("region_id", (rand() * 10).cast(IntegerType()))
fact_sales.write.mode("overwrite").saveAsTable("fact_sales")

# COMMAND ----------

# Create small dimension table (~10 rows)
dim_region = spark.createDataFrame(
    [(i, f"Region_{i}") for i in range(10)],
    ["region_id", "region_name"]
)
dim_region.write.mode("overwrite").saveAsTable("dim_region")

# COMMAND ----------

# Join with broadcast
fact_df = spark.table("fact_sales")
region_df = spark.table("dim_region")

df_with_broadcast = fact_df.join(broadcast(region_df), on="region_id")

# Trigger the job
print("✅ Count result (with broadcast):")
print(df_with_broadcast.count())

# Show physical plan
print("\n🚀 Physical plan (with broadcast):")
df_with_broadcast.explain(True)