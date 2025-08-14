# Databricks notebook source
# MAGIC %md
# MAGIC ## ❌ Version 1: No Broadcast Join - Suboptimal

# COMMAND ----------

schema = 'development_nagendra.default'

# COMMAND ----------

from pyspark.sql.functions import rand
from pyspark.sql.types import IntegerType

# Create large fact table (~10M rows)
fact_sales = spark.range(0, 10_000_000).withColumn("region_id", (rand() * 10).cast(IntegerType()))
fact_sales.write.mode("overwrite").saveAsTable(f"{schema}.fact_sales")

# COMMAND ----------

# Create small dimension table (~10 rows)
dim_region = spark.createDataFrame(
    [(i, f"Region_{i}") for i in range(10)],
    ["region_id", "region_name"]
)
dim_region.write.mode("overwrite").saveAsTable(f"{schema}.dim_region")

# COMMAND ----------

# Join without broadcast (may cause shuffle)
df_no_broadcast = spark.sql("""
    SELECT f.id, f.region_id, r.region_name
    FROM fact_sales f
    JOIN dim_region r
    ON f.region_id = r.region_id
""")

# Trigger the job
print("🔍 Count result (no broadcast):")
print(df_no_broadcast.count())

# Show physical plan
print("\n📄 Physical plan (no broadcast):")
df_no_broadcast.explain(True)