# Databricks notebook source
# MAGIC %md
# MAGIC ## Extract Data From the Address Files
# MAGIC 1. Read all the files
# MAGIC 1. Create addresses table in the bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read all the address files

# COMMAND ----------

df = (
  spark.read.format('csv')
  .option("header", True)  
  .option("sep", "\t")
  .load("/Volumes/gizmobox/landing/operational_data/addresses")  
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create Addresses table in the Bronze Layer

# COMMAND ----------

df.writeTo('gizmobox.bronze.py_addresses').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.py_addresses;
