# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/eclerx_input_files/

# COMMAND ----------

# DBTITLE 1,Step 1: Extracting / Reading
input_path="dbfs:/FileStore/eclerx_input_files/circuits.csv"
#df=spark.read.csv("dbfs:/FileStore/eclerx_input_files/circuits.csv")
#df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/eclerx_input_files/circuits.csv")
df=spark.read.csv(input_path,header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,Task
# MAGIC %md
# MAGIC - Task: 
# MAGIC -     1. rename circuitId-- circuit_id, circuitRef -- circuit_ref
# MAGIC -     2. add new column with current_date
# MAGIC -     3. drop url col

# COMMAND ----------

# DBTITLE 1,Step 2: Transformation
df_final=df\
.withColumnsRenamed({"circuitId":"circuit_id","circuitref":"circuit_ref"})\
.withColumn("ingestion_date",current_date())\
.drop("url")

# COMMAND ----------

# DBTITLE 1,Step 3: Loading/ writing
# MAGIC %sql
# MAGIC create schema if not exists formula1

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("formula1.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.formula1.circuit
