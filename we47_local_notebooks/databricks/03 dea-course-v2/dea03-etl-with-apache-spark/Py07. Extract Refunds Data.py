# Databricks notebook source
# MAGIC %md
# MAGIC ## Extract Data From the Returns SQL Table
# MAGIC 1. Load data from CSV (since cant download data from Azure SQL)
# MAGIC 1. Create External Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create Bronze Schema in Hive Metastore

# COMMAND ----------

df = (
        spark.read.format('csv')
        .option("header", True)  
        .option("sep", ",")
        .load("/Volumes/gizmobox/landing/operational_data/refunds/07__Extract_Refunds_Data___via_JDBC.csv")  
)

display(df)

# COMMAND ----------

df.writeTo('gizmobox.bronze.py_refunds').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gizmobox.bronze.py_refunds
