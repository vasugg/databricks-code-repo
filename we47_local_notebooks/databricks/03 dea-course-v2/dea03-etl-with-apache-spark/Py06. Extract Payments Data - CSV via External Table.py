# Databricks notebook source
# MAGIC %md
# MAGIC ## Extract Data From the Payments Files
# MAGIC 1. List the files from Payment folder
# MAGIC 1. Read the Payments file
# MAGIC 1. Demonstrate the effect of Adding/ Updating / Deleting files. 
# MAGIC 1. Demonstrate the effect of Dropping the Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. List the files from payment folder

# COMMAND ----------

# MAGIC %fs ls '/Volumes/gizmobox/landing/external_data/payments/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read the payments file

# COMMAND ----------

payments_schema = 'payment_id INTEGER, order_id INTEGER, payment_timestamp TIMESTAMP, payment_status INTEGER, payment_method STRING'

# COMMAND ----------

#creating schmea using Python coding

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

py_payments_schema = StructType([
    StructField('payment_id', IntegerType()),
    StructField('order_id', IntegerType()),
    StructField('payment_timestamp', TimestampType()),
    StructField('payment_status', IntegerType()),
    StructField('payment_method', StringType())
])

# COMMAND ----------

df = (
      spark.read.format('csv')
      .options(delimiter=',')
      .schema(py_payments_schema)
      .load("/Volumes/gizmobox/landing/external_data/payments")
)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create Payment table in the bronze schema

# COMMAND ----------

df.writeTo('gizmobox.bronze.py_payments').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.py_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Demonstrate the effect of adding/ updating / deleting files. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE gizmobox.bronze.payments;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Demonstrate the effect of Dropping the Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gizmobox.bronze.payments;
