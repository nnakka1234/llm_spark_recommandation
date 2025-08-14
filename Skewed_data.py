# Databricks notebook source
# Databricks / PySpark (FLAWED VERSION)
from pyspark.sql import functions as F, types as T


# 🔧 Knobs to widen the cost gap
ROWS = 5000000      # increase to amplify cost
REPEATS = 2            # redundant actions to force recomputation
SHUFFLE_PARTS = 2000   # too many partitions for small clusters

# 🔩 Make Spark do more work than needed
spark.conf.set("spark.sql.adaptive.enabled", "false")                # disable AQE
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")       # no skew mitigation
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")         # forbid broadcast; force shuffle
spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))   # excessive shuffle partitions
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "false")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")

# 🧪 Skewed big fact table (90% of rows hit region 0 -> heavy skew)
fact = (
    spark.range(ROWS)
         .withColumn(
             "region_id",
             F.when(F.rand(seed=42) < 0.90, F.lit(0)).otherwise((F.rand()*9).cast("int"))
         )
         .withColumn("qty", (F.rand()*10).cast("int"))
         .withColumn("price", F.rand()*100)
)

# 🤦 Write to disk then read back multiple times (redundant I/O + small files)
fact.write.mode("overwrite").format("delta").saveAsTable("flawed_fact_sales")
fact1 = spark.read.table("flawed_fact_sales")
fact2 = spark.read.table("flawed_fact_sales")  # redundant read
fact1.write.mode("overwrite").format("delta").saveAsTable("flawed_fact_sales_stage")
fact_stage = spark.read.table("flawed_fact_sales_stage")

# 🧱 Tiny dim table (but we will block broadcast so it still shuffles)
dim = spark.createDataFrame([(i, f"Region_{i}") for i in range(10)], ["region_id", "region_name"])
dim.write.mode("overwrite").format("delta").saveAsTable("flawed_dim_region")

# 🧨 Silly UDF for a simple op (slower than built-ins)
@F.udf("double")
def udf_total(qty, price):
    return float(qty) * float(price)

# ⛓️ Chain: compute same heavy thing multiple times; write & re-read in between
joined1 = fact_stage.join(spark.read.table("flawed_dim_region"), "region_id")  # forced shuffle join
stage1 = joined1.withColumn("total", udf_total(F.col("qty"), F.col("price")))
stage1.write.mode("overwrite").format("delta").saveAsTable("flawed_stage1")

# More shuffles: repartition to excessive partitions, then write small files
stage1.repartition(SHUFFLE_PARTS, "region_id").write.mode("overwrite").format("delta").saveAsTable("flawed_stage1_wide")

# 📈 Trigger redundant actions that recompute the whole lineage each time
for i in range(REPEATS):
    # read again (don’t cache), re-join to force another shuffle, then aggregate separately
    s = spark.read.table("flawed_stage1_wide").join(spark.read.table("flawed_dim_region"), "region_id")
    # three independent actions that each walk the plan
    print(f"[FLAWED] Iter {i} count:", s.count())
    print(f"[FLAWED] Iter {i} sum(total):", s.agg(F.sum("total")).collect()[0][0])
    print(f"[FLAWED] Iter {i} avg(price) by region:", s.groupBy("region_id").agg(F.avg("price")).count())

# Keep something around so the plan can be inspected
spark.read.table("flawed_stage1_wide").explain(True)
