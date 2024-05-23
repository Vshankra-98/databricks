-- Databricks notebook source
select current_catalog()

-- COMMAND ----------

create database  db_venkat.f1_raw managed location 'abfss:/mnt/udemy/raw/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating Circuits table

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Creating Constructor table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/mnt/udemy/raw/'))

-- COMMAND ----------

create table if not exists raw_f1.Constructor_table(
constructorId int,
constructorRef string,
name string,
nationality string,
url string
)
using json
options (path 'dbfs:/mnt/udemy/raw/constructors.json')



-- COMMAND ----------


