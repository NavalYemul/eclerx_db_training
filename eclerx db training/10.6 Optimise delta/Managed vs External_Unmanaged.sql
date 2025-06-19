-- Databricks notebook source
Managed Table (default location : dbfs:/user/hive/warehouse/demo)


create table tablename (col1 datatype, col2 datatype)


df=spark.read.format("path")
df.write.saveAsTable("tblname")


DROP : everything (table + metadata(parquet+ delta logs))

-- COMMAND ----------


External Table/ Unmanaged table ( location : )

create table tblname (c1 datatype, c2 dataype) location 'dbfs:/eclerx/metadata'


df=spark.read.format("path")
df.write.option("path","dbfs:/eclerx/metadata").saveAsTable("tblname")


DROP : table __ metadata(remains)

-- COMMAND ----------

create table demo_vendors (id int, name string) location '/FileStore/eclerx_metadata/vendors'

-- COMMAND ----------

insert into demo_vendors values (1,'a')

-- COMMAND ----------

desc extended demo_vendors

-- COMMAND ----------

desc extended demo

-- COMMAND ----------

drop table demo_vendors
