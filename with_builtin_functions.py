# Databricks notebook source
# MAGIC %md
# MAGIC ## ✅ Version 2: Using Built-in Functions Instead of UDFs

# COMMAND ----------

from pyspark.sql.functions import rand, when

# Sample data
df = spark.range(0, 1_000_000).withColumn("value", (rand() * 100).cast("int"))

# Use built-in Spark SQL functions for categorization
df_builtin = df.withColumn(
    "category",
    when(df["value"] < 20, "Low")
    .when(df["value"] < 70, "Medium")
    .otherwise("High")
)

# Trigger execution
print("🚀 Count result (with built-in functions):")
print(df_builtin.count())

# Show plan
df_builtin.explain(True)