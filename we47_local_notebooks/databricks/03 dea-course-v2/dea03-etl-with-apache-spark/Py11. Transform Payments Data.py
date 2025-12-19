# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Payments Data
# MAGIC 1. Extract Date and Time from payment_timestamp and create new columns payment_date and payment_time
# MAGIC 1. Map payment_status to contain descriptive values  
# MAGIC    (1-Success, 2-Pending, 3-Cancelled, 4-Failed)
# MAGIC 1. Write transformed data to the Silver schema 

# COMMAND ----------

df = spark.table('gizmobox.bronze.py_payments')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Extract Date and Time from payment_timestamp
# MAGIC >  [Documentation for date_format function](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/date_format)

# COMMAND ----------

from pyspark.sql import functions as F

df_extract_payments = (
                        df.
                        select('order_id', 
                               'payment_id',
                               F.date_format('payment_timestamp', 'yyyy-MM-dd').cast('date').alias('payment_date'),
                               F.date_format('payment_timestamp', 'HH-mm-ss').alias('payment_time'),
                               'payment_status',
                               'payment_method'
                               )
)

display(df_extract_payments)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Map payment_status to contain descriptive values  
# MAGIC    (1-Success, 2-Pending, 3-Cancelled, 4-Failed)

# COMMAND ----------

df_mapped_payments = (
                        df_extract_payments.
                        select(
                               'order_id', 
                               'payment_id',
                               'payment_date',
                               'payment_time',
                               F.when(df_extract_payments.payment_status == 1, 'Success')
                                .when(df_extract_payments.payment_status == 2, 'Pending')
                                .when(df_extract_payments.payment_status == 3, 'Cancelled')
                                .when(df_extract_payments.payment_status == 4, 'Failed')
                                .alias('payment_status'),
                               'payment_method'
                               )
)

display(df_mapped_payments)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write transformed data to the Silver schema 

# COMMAND ----------

df_mapped_payments.writeTo('gizmobox.silver.py_payments').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_payments;
