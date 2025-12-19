# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Refunds Data
# MAGIC 1. Extract specific portion of the string from refund_reason using split function
# MAGIC 1. Extract specific portion of the string from refund_reason using regexp_extract function
# MAGIC 1. Extract date and time from the refund_timestamp
# MAGIC 1. Write transformed data to the Silver schema in hive metastore [Default Location: /user/hive/warehouse]

# COMMAND ----------

df_refunds = spark.table('gizmobox.bronze.py_refunds')
display(df_refunds)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Extract specific portion of the string from refund_reason using split function
# MAGIC > [Documentation for split Function](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/split)

# COMMAND ----------

from pyspark.sql import functions as F

df_split_refunds = (
                    df_refunds
                    .select("refund_id",
                            "payment_id",
                            "refund_timestamp",
                            "refund_amount",
                            F.split("refund_reason", ":")[0].alias("refund_reason"),
                            F.split("refund_reason", ":")[1].alias("refund_source")                            
                            )
)

display(df_split_refunds)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Extract specific portion of the string from refund_reason using regexp_extract function
# MAGIC > [Documentation for regexp_extract Function](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/regexp_extract)  
# MAGIC > [Regex Pattern](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html)

# COMMAND ----------

df_transformed_refunds = (
                    df_refunds
                    .select("refund_id",
                            "payment_id",
                            F.date_format('refund_timestamp', 'yyyy-MM-dd').cast('date').alias('refund_date'),
                            F.date_format('refund_timestamp', 'HH-mm-ss').alias('refund_time'),
                            "refund_amount",
                            F.regexp_extract("refund_reason", "^([^:]+):", 1).alias("refund_reason"),
                            F.regexp_extract("refund_reason", "^[^:]+:(.*)$", 1).alias("refund_source")                            
                            )
)

display(df_transformed_refunds)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write transformed data to the Silver schema  

# COMMAND ----------

df_transformed_refunds.writeTo('gizmobox.silver.py_refunds').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_refunds;
