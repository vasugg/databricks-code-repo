# Databricks notebook source
# MAGIC %md
# MAGIC ## Databricks Utilities
# MAGIC - **File system utilities** 
# MAGIC - **Secrets Utilities**
# MAGIC - **Widget Utilities**
# MAGIC - **Notebook Workflow Utilities**

# COMMAND ----------

# MAGIC %md
# MAGIC ### File system utilities

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

items = dbutils.fs.ls('/databricks-datasets/')

# COMMAND ----------

# Count folders and files using list comprehensions 
folder_count = len([item for item in items if item.name.endswith("/")]) 
file_count = len([item for item in items if not item.name.endswith("/")]) 
# Display the results 
print(f"Total Folders: {folder_count}") 
print(f"Total Files: {file_count}")

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('cp')

# COMMAND ----------


