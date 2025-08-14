# Databricks notebook source
# DBTITLE 1,Read code content
import sys
from pathlib import Path
def read_code_files(FILE_PATH):
    HEAD_MAX = 20_000_000   # 20 MiB

    # check that the file is visible
    dbutils.fs.ls(FILE_PATH)

    source_code = None

    try:                                 # fast path
        source_code = dbutils.fs.head(FILE_PATH, HEAD_MAX)
    except Exception:
        pass

    if not source_code:                  # stream if head failed / file larger
        try:
            stream = dbutils.fs.open(FILE_PATH, "r")
            chunks = []
            while True:
                data = stream.read(4*1024*1024)    # 4 MiB
                if not data:
                    break
                chunks.append(data)
            stream.close()
            source_code = "".join(chunks)
        except Exception:
            pass

    if not source_code:                  # final fallback via Spark
        df = spark.read.text(FILE_PATH)
        source_code = "\n".join([row.value for row in df.collect()])

    return source_code

# COMMAND ----------

# DBTITLE 1,Read Spark Code and add content to Pandas DF
import json, math, os
import pandas as pd

sql_query = """
SELECT task_key, notebook_path
  FROM development_nagendra.default.cluster_cost_data_collection
  group by 1,2 having notebook_path is not null;
"""

pdf = spark.sql(sql_query).toPandas()
content_dict = {}

pdf['notebook_content'] = pdf['notebook_path'].apply(lambda path: read_code_files(path if path.endswith('.py') else path + '.py'))

df = spark.createDataFrame(pdf)
df.write.saveAsTable("development_nagendra.default.transiant_llm_interaction", mode="overwrite")

# COMMAND ----------



# COMMAND ----------

