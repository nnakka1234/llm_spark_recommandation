# Databricks notebook source
# DBTITLE 1,Call LLM

from openai import OpenAI     # v1 client
import tiktoken
import json

pdf = spark.read.table("development_nagendra.default.transiant_llm_interaction").toPandas()

openai_api_key = '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'

client = OpenAI(api_key=openai_api_key)   # <-- this object will be used for all calls
def dataframe_to_chunks(
    df,
    model: str = "gpt-3.5-turbo",
    max_tokens_per_chunk: int = 3000,
    system_prompt: str = "You are an expert cloud‑ops analyst."
) -> list:
    """
    Returns a list where each element is a **list of two messages**:
        [ {"role":"system", "content":system_prompt},
          {"role":"user",   "content":<chunk‑as‑JSON>} ]
    The `content` values are plain strings, which is exactly what the v1 SDK expects.
    """
    # ----- token counting -------------------------------------------------
    encoding = tiktoken.encoding_for_model(model)
    rows = df.to_dict(orient="records")
    serialized = json.dumps(rows, separators=(",", ":"))
    total_tokens = len(encoding.encode(serialized))
    print(f"Total tokens for whole dataframe: {total_tokens}")

    # ----- chunking --------------------------------------------------------
    chunks = []
    if total_tokens <= max_tokens_per_chunk:
        chunks.append(serialized)
    else:
        rows_per_chunk = max(
            1,
            math.floor(len(rows) * max_tokens_per_chunk / total_tokens)
        )
        for i in range(0, len(rows), rows_per_chunk):
            sub = rows[i : i + rows_per_chunk]
            chunks.append(json.dumps(sub, separators=(",", ":")))

    # ----- build the per‑chunk message list --------------------------------
    batches = []
    for chunk in chunks:
        batches.append([
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": f"Here is a slice of data:\n{chunk}\nPlease give a short insight."}
        ])

    print(f"🗂️  Produced {len(batches)} chunk(s)")
    return batches

# ------------------------------------------------------------
# 5️⃣  Build the chat batches
# ------------------------------------------------------------
batches = dataframe_to_chunks(
    pdf,
    model="gpt-4-turbo-preview",
    max_tokens_per_chunk=3000,
    system_prompt = """You are a senior data‑engineering and PySpark specialist.  
Your task is to:

1. **Analyze** the full Python/PySpark code that will be supplied in column name notebook_content.  
2. **Identify** any anomalies, anti‑patterns, or inefficiencies (e.g.,  
   – expensive shuffles or joins,  
   – unnecessary UDFs,  
   – improper caching,  
   – sub‑optimal partitioning,  
   – resource‑wasteful actions, etc.).  
3. For each issue, **provide a concrete code‑level improvement** that corrects the problem and explains why it helps.
4. As part of response I need to see the exact code fix which can be implemented to my notebook content. Show me the code cells which and recommended updates necessary
 

Present the findings as a clear, structured list (issue → recommendation → expected benefit).  
Feel free to suggest alternative Spark configurations or best‑practice patterns when appropriate.
"""

)

# 4. Quantify the expected impact on:
#    - DBU consumption,  
#    - CPU / memory utilization,  
#    - overall cost. 

# ------------------------------------------------------------
# 6️⃣  Call the model – **no extra wrapper around msgs**
# ------------------------------------------------------------
final_text = ""

for idx, msgs in enumerate(batches, start=1):
    print(f"\n▶️  Sending batch {idx}/{len(batches)} …")
    resp = client.chat.completions.create(
        model="gpt-4-turbo-preview",
        messages=msgs,          # <-- msgs already contains the system+user pair
        max_tokens=500,
        temperature=0.2,
    )
    answer = resp.choices[0].message.content.strip()
    final_text += "\n\n" + answer
    print("✅  Chunk answer (first 400 chars):")
    print(answer[:400] + ("…" if len(answer) > 400 else ""))

# ------------------------------------------------------------
# 7️⃣  Show / store the aggregated answer
# ------------------------------------------------------------
print("\n=== LLM SUMMARY ===\n")
print(final_text)

#Uncomment and adapt the following if you want to persist the result:
# spark.createDataFrame(
#     [(final_text, )],
#     schema="summary STRING"
# ).write.mode("append").saveAsTable("my_schema.cluster_utilization_llm_summary")

# COMMAND ----------

# DBTITLE 1,Create a text file out of LLM
from datetime import datetime
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"llm_summary_{ts}.txt"

local_path =  f'/Workspace/Users/nagendra.nakka@cgi.com/LLM_Assessment_and_Recommendation/{filename}'
with open(local_path, "w", encoding="utf-8") as f:
    f.write(final_text)

print(f"✅  LLM summary written to local file: {local_path}")