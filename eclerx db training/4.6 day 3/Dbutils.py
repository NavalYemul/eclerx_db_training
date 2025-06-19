# Databricks notebook source
DBFS: Databricks File System

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/raw/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/eclerx_input_files")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/raw/housing.csv","dbfs:/FileStore/eclerx_input_files")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/raw/")

# COMMAND ----------


