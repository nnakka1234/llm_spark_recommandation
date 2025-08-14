# Databricks notebook source
# MAGIC %md
# MAGIC ## ❌ Version 1: Using UDFs Instead of Built-in Functions

# COMMAND ----------

from pyspark.sql.functions import rand, udf
from pyspark.sql.types import IntegerType

# Sample data
df = spark.range(0, 1_000_000).withColumn("value", (rand() * 100).cast("int"))

# Define a UDF to categorize numbers
def categorize(val):
    if val < 20:
        return "Low"
    elif val < 70:
        return "Medium"
    else:
        return "High"

categorize_udf = udf(categorize)

# Use UDF in transformation
df_udf = df.withColumn("category", categorize_udf(df["value"]))

# Trigger execution
print("🔍 Count result (with UDF):")
print(df_udf.count())

# Show plan
df_udf.explain(True)