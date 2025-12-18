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

df_extrac

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(payment_id AS INT) AS payment_id,
# MAGIC        CAST(order_id AS INT) AS order_id,
# MAGIC        CAST(date_format(payment_timestamp,'yyyy-MM-dd') AS DATE) AS payment_date,
# MAGIC        date_format(payment_timestamp,'HH:mm:ss') AS payment_time,
# MAGIC        payment_status,
# MAGIC        payment_method
# MAGIC   FROM gizmobox.bronze.payments;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Map payment_status to contain descriptive values  
# MAGIC    (1-Success, 2-Pending, 3-Cancelled, 4-Failed)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(payment_id AS INT) AS payment_id,
# MAGIC        CAST(order_id AS INT) AS order_id,
# MAGIC        CAST(date_format(payment_timestamp,'yyyy-MM-dd') AS DATE) AS payment_date,
# MAGIC        date_format(payment_timestamp,'HH:mm:ss') AS payment_time,
# MAGIC        CASE payment_status
# MAGIC          WHEN 1 THEN 'Success'
# MAGIC          WHEN 2 THEN 'Pending'
# MAGIC          WHEN 3 THEN 'Cancelled'
# MAGIC          WHEN 4 THEN 'Failed'
# MAGIC        END AS payment_status,  
# MAGIC        payment_method
# MAGIC   FROM gizmobox.bronze.payments;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write transformed data to the Silver schema 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gizmobox.silver.payments
# MAGIC AS
# MAGIC SELECT CAST(payment_id AS INT) AS payment_id,
# MAGIC        CAST(order_id AS INT) AS order_id,
# MAGIC        CAST(date_format(payment_timestamp,'yyyy-MM-dd') AS DATE) AS payment_date,
# MAGIC        date_format(payment_timestamp,'HH:mm:ss') AS payment_time,
# MAGIC        CASE payment_status
# MAGIC          WHEN 1 THEN 'Success'
# MAGIC          WHEN 2 THEN 'Pending'
# MAGIC          WHEN 3 THEN 'Cancelled'
# MAGIC          WHEN 4 THEN 'Failed'
# MAGIC        END AS payment_status,  
# MAGIC        payment_method
# MAGIC   FROM gizmobox.bronze.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.payments;
