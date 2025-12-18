# Databricks notebook source
# MAGIC %md 
# MAGIC ## Extract Data From the Customers JSON File
# MAGIC 1. Query Single File
# MAGIC 1. Query List of Files using wildcard Characters
# MAGIC 1. Query all the files in a Folder
# MAGIC 1. Select File Metadata
# MAGIC 1. Create Table in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Query Single JSON File

# COMMAND ----------

# MAGIC %fs ls /Volumes/gizmobox/landing/operational_data/customers/

# COMMAND ----------

df = spark.read.format("json").load("/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Query Multiple JSON Files

# COMMAND ----------

df = spark.read.format("json").load("/Volumes/gizmobox/landing/operational_data/customers/customers_2024_*.json")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Query all the files in a folder

# COMMAND ----------

df = spark.read.format("json").load("/Volumes/gizmobox/landing/operational_data/customers")
display(df)

# COMMAND ----------

df = spark.read.json("/Volumes/gizmobox/landing/operational_data/customers")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Select File Metadata

# COMMAND ----------

df_with_metadata = df.select("_metadata.file_path", "*")
display(df_with_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Create Table in Bronze Schema

# COMMAND ----------

df_with_metadata.write.format("delta").mode("overwrite").saveAsTable("gizmobox.bronze.py_customers")

# COMMAND ----------

df_with_metadata.writeTo("gizmobox.bronze.py_customers").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gizmobox.bronze.py_customers
