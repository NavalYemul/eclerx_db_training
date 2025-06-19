-- Databricks notebook source
Views: A virtual table

1. standard view (persisted): SQL 
2. Temp View : SQL and PySpark

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv",header=True,inferSchema=True)
-- MAGIC df.write.saveAsTable("bike_day")

-- COMMAND ----------

create or replace view max_month as 
select mnth,round(max(temp),2) as max from bike_day group by mnth order by max desc

-- COMMAND ----------

desc extended max_month

-- COMMAND ----------

select * from max_month

-- COMMAND ----------

show views

-- COMMAND ----------

create or replace temp view holiday_count_temp as 
select mnth, count(*)  as count from hive_metastore.default.bike_day where holiday =1 group by mnth

-- COMMAND ----------

show views

-- COMMAND ----------

select * from holiday_count_temp --where mnth=12

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UDF

-- COMMAND ----------

-- DBTITLE 1,Syntax
create function function_name(para datatype)
returns datatype
return logic/Expression


-- COMMAND ----------

create or replace function fullname(first_name string, last_name string)
returns string
return concat(first_name, " ", last_name)

-- COMMAND ----------

select fullname("naval" ,"yemul") as name 

-- COMMAND ----------

create or replace table example (id int, forename string, surname string, age int);
insert into example values ( 1,'Akash', 'Yadav', 28), (2,'Aman','Arora',28),(3,'Bhavesh','Mahajan',30),(4,'a','v',65),(5,'c','t',15)

-- COMMAND ----------

select * from example

-- COMMAND ----------

select id, fullname(forename, surname)  as fullname, age,agegroup(age) as message from example

-- COMMAND ----------

Task:
udf function:

age>60 senior
age 20 to 60 adult
age <20 teenager

hint: case when

-- COMMAND ----------

create or replace function agegroup(age int)
returns string
return 
  case
  when age >60 then 'senior'
  when age> 18 then 'adult'
  when age<18 then 'teenage'
  else 'kid'
  end
  

-- COMMAND ----------


