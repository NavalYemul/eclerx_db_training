-- Databricks notebook source
Databricks Heirarchy 

Catalog
schema (Databases)
Tables, views

-- COMMAND ----------

create schema if not exists demo;

show schemas;

-- COMMAND ----------

create table demo.emp(id int, name string)

-- COMMAND ----------

insert into demo.emp values(1, 'Naval')

-- COMMAND ----------

select * from hive_metastore.demo.emp

-- COMMAND ----------

describe hive_metastore.demo.emp

-- COMMAND ----------

describe extended hive_metastore.demo.emp
