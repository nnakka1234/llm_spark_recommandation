# Databricks notebook source
# Databricks / PySpark (OPTIMIZED VERSION)
from pyspark.sql import functions as F

# Match logical dataset size for apples-to-apples
ROWS = 5000000
REPEATS = 1                # one consolidated action is enough
TARGET_SHUFFLE_PARTS = 256 # right-size for your cluster; tune to cores

# 🧠 Turn on optimizers
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")   # 100MB: broadcast small dims
spark.conf.set("spark.sql.shuffle.partitions", str(TARGET_SHUFFLE_PARTS))
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Build the same logical data, but we won't round-trip to disk or recompute needlessly
fact = (
    spark.range(ROWS)
         .withColumn(
             "region_id",
             F.when(F.rand(seed=42) < 0.90, F.lit(0)).otherwise((F.rand()*9).cast("int"))
         )
         .withColumn("qty", (F.rand()*10).cast("int"))
         .withColumn("price", F.rand()*100)
         .repartition(TARGET_SHUFFLE_PARTS, "region_id")   # partition on the join key once
         .persist()                                        # reuse without recompute
)
_ = fact.count()  # materialize cache once

dim = spark.createDataFrame([(i, f"Region_{i}") for i in range(10)], ["region_id", "region_name"])
dim_b = F.broadcast(dim)                                   # broadcast the tiny dim

# Use built-ins (codegen) instead of UDFs; fuse transformations
df = (
    fact.join(dim_b, "region_id")
        .withColumn("total", F.col("qty") * F.col("price"))
)

# Collapse multiple metrics into a single pass/action
summary = (
    df.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum("total").alias("sum_total"),
    )
    .collect()[0]
)

print("[OPT] rows:", summary["row_count"])
print("[OPT] sum(total):", float(summary["sum_total"]))

# If you must write, coalesce to reasonable file sizes (few hundred MB per file typically)
(
    df.coalesce(64)                     # reduce small files pressure
      .write.mode("overwrite")
      .format("delta")
      .saveAsTable("opt_fact_joined")
)

spark.table("opt_fact_joined").explain(True)
