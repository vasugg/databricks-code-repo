# Databricks notebook source
# MAGIC %md
# MAGIC ## Extract Data - Using PySpark
# MAGIC 1. Run SQL Commands using Python - spark.sql Function
# MAGIC 2. Spark Dataframe Reader API
# MAGIC 3. Read Tables using spark.table Function

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Run SQL Commands using Python - spark.sql Function

# COMMAND ----------

df = spark.sql('SELECT * FROM json.`/Volumes/gizmobox/landing/operational_data/customers`')
display(df)

# COMMAND ----------

df = spark.sql('''CREATE OR REPLACE TEMP VIEW tv_customers 
                  AS 
                  SELECT * FROM json.`/Volumes/gizmobox/landing/operational_data/customers`''')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tv_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Spark DataframeReader API
# MAGIC [DataFrameReader API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html)

# COMMAND ----------

df = spark.read.format('json').load('/Volumes/gizmobox/landing/operational_data/customers')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. spark.table Function

# COMMAND ----------

df = spark.table('gizmobox.bronze.v_addresses')
display(df)
