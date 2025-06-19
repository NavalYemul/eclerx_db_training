-- Databricks notebook source
create table emp(id int, name string, city string)

-- COMMAND ----------

desc extended emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Internals of Delta Lake
-- MAGIC
-- MAGIC (metadata)
-- MAGIC
-- MAGIC parquet (Data Files)
-- MAGIC + 
-- MAGIC _delta_logs 
-- MAGIC 1. crc (cyclic reducdant check)
-- MAGIC 2. json 
-- MAGIC 3. checkpoint

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000000.json

-- COMMAND ----------

desc detail emp

-- COMMAND ----------

desc history emp

-- COMMAND ----------

insert into emp values (1,'Naval','Solapur');

-- COMMAND ----------

select * from emp

-- COMMAND ----------

create table emp(id int, name string, city string)
desc extended emp


%fs head
dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000000.json


desc history emp

insert into emp values(1,'a','Mumbai')

select * from emp

desc detail emp 

insert into emp values(2,'Gaurav','Mumbai'),(3,'Karan','Pune'),(4,'Sristi','Mumbai')

insert into emp values(5,'Himanshu','Mumbai');
insert into emp values (6,'Swapnil','Pune');

delete from emp where id =2

select * from emp version as of 4

select * from emp timestamp as of ''

-- COMMAND ----------

insert into emp values(2,'Gaurav','Mumbai'),(3,'Karan','Pune'),(4,'Sristi','Mumbai')

-- COMMAND ----------

select * from emp

-- COMMAND ----------

insert into emp values(5,'Himanshu','Mumbai');
insert into emp values (6,'Swapnil','Pune')

-- COMMAND ----------

select * from emp

-- COMMAND ----------

delete from emp where id =2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Time Travel
-- MAGIC 1. version as of
-- MAGIC 2. timestamp as of

-- COMMAND ----------

-- DBTITLE 1,CTAS
--create table emp_v4 as 
select * from emp version as of 4

-- COMMAND ----------

select * from emp timestamp as of '2025-06-09T06:56:53.000+00:00'

-- COMMAND ----------


