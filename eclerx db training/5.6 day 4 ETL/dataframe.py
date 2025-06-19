# Databricks notebook source
#sql
create table db.tblame (id int, )

# COMMAND ----------

spark

# COMMAND ----------

user_data=([(1,'Naval'),(2,'Karan')])
user_schema="id int, name string"

df=spark.createDataFrame(data=user_data, schema=user_schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

Lazy Evaluation
Transformation and Actions

Transformations 

Dataframe functions
select
alias
withColumnRenamed
withColumnsRenamed
filter
sort
group
agg
join


functions
col
current_date


Actions
show
display 
write

# COMMAND ----------

df.select("*")

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("id","name").display()

# COMMAND ----------

df.select("id".alias("emp_id"))

# COMMAND ----------

df.select(col("id").alias("emp_id"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.display()

# COMMAND ----------

df1=df.select(col("id").alias("emp_id"),"*")

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").withColumnRenamed("name","emp_name").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name"}).display()

# COMMAND ----------

df2=df.withColumn("ingestion_date",current_date())

# COMMAND ----------

df2.display()
