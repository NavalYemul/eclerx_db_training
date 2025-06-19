-- Databricks notebook source
-- MAGIC %python
-- MAGIC data=([(1,'Naval','Pune'),(2,'Karan','Mumbai')])
-- MAGIC schema='id int, name string, city string'
-- MAGIC
-- MAGIC df=spark.createDataFrame(data,schema)
-- MAGIC df.write.saveAsTable("emp_new")

-- COMMAND ----------

desc history emp_new

-- COMMAND ----------

desc detail emp_new

-- COMMAND ----------

select * from emp_new

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data=([(3,'Akshay','Pune','DE'),(4,'Aman','Mumbai','DA')])
-- MAGIC schema='id int, name string, city string, dept string'
-- MAGIC
-- MAGIC df=spark.createDataFrame(data,schema)
-- MAGIC #df.write.mode("append").saveAsTable("emp_new")
-- MAGIC df.write.mode("append").option("mergeSchema", "true").saveAsTable("emp_new")

-- COMMAND ----------

select * from emp_new

-- COMMAND ----------


