-- Databricks notebook source
create table demo (id int, name string)

-- COMMAND ----------

insert into demo values(1,'a');
insert into demo values(2,'b');
insert into demo values(3,'c');
insert into demo values(4,'d');
insert into demo values(5,'k');
insert into demo values(6,'u');
insert into demo values(7,'i');

-- COMMAND ----------

desc detail demo

-- COMMAND ----------

delete from demo where id > 4

-- COMMAND ----------

select * from demo

-- COMMAND ----------

desc history demo

-- COMMAND ----------

desc extended demo 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimize: small files into large files

-- COMMAND ----------

optimize demo
zorder by (id)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Liquid Clustering

-- COMMAND ----------

desc detail demo

-- COMMAND ----------

delete from demo 

-- COMMAND ----------

restore table demo to version as of 9

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Vacuum: Remove slate files and no time travel
-- MAGIC Retention period: 7days or 168 hours

-- COMMAND ----------

vacuum demo 

-- COMMAND ----------

vacuum demo retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum demo retain 0 hours

-- COMMAND ----------

select * from demo --version as of 7

-- COMMAND ----------

A data engineer has realized that they made a mistake when making a daily update to a table. They need to use Delta time travel to restore the table to a version that is 3 days old. However, when the data engineer attempts to time travel to the older version, they are unable to restore the data because the data files have been deleted.
Which of the following explains why the data files are no longer present?

A. The VACUUM command was run on the table
# B. The TIME TRAVEL command was run on the table
# C. The DELETE HISTORY command was run on the table
# D. The OPTIMIZE command was nun on the table
# E. The HISTORY command was run on the table


