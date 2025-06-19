# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists formula1

# COMMAND ----------

# DBTITLE 1,ETL
from pyspark.sql.functions import *
input_path="dbfs:/FileStore/eclerx_input_files/circuits.csv"
df=spark.read.csv(input_path,header=True,inferSchema=True)

df_final=df\
.withColumnsRenamed({"circuitId":"circuit_id","circuitref":"circuit_ref"})\
.withColumn("ingestion_date",current_date())\
.drop("url")


df_final.write.mode("overwrite").saveAsTable("formula1.circuit")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.formula1.circuit
