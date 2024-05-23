-- Databricks notebook source
show databases;

-- COMMAND ----------

create database demo;

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

describe database  demo;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

select current_database()

-- COMMAND ----------

select current_version()

-- COMMAND ----------

select current_metastore()

-- COMMAND ----------

select current_recipient()

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

describe table extended demo.circuit_table_external;

-- COMMAND ----------

drop table demo.circuit_table_external;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls('/mnt/udemy/raw/'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC result_df  = spark.read.json("dbfs:/mnt/udemy/raw/results.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(result_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### using python
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC result_df.write.mode('overwrite').format('delta').saveAsTable('demo.result_table_python')

-- COMMAND ----------

select * from demo.result_table_python
where constructorId = 10;

-- COMMAND ----------

describe table extended demo.result_table_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### using sql
-- MAGIC

-- COMMAND ----------

create table if not exists demo.result_table_sql
as
select * from demo.result_table_python
where constructorId = 10;

-- COMMAND ----------

select * from demo.result_table_sql

-- COMMAND ----------

describe table extended demo.result_table_sql

-- COMMAND ----------

desc table extended demo.result_table_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

drop table demo.result_table_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # External Tables

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## using pyhton
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC result_df.write.mode('overwrite').format('delta').option("path", "dbfs:/mnt/udemy/External_tables/results_ex_py").saveAsTable('db_venkatdemo.result_py_ex_table')

-- COMMAND ----------

CREATE CONNECTION external_tables
  USING DELTA
  LOCATION 'dbfs:/mnt/udemy/External_tables/results_ex_py'

-- COMMAND ----------

CREATE FOREIGN CATALOG external_tables_catalog
  COMMENT 'External Tables Catalog'
  WITH CONNECTION external_tables

-- COMMAND ----------

CREATE LIVE TABLE db_venkat.demo.result_py_ex_table
  USING external_tables_catalog.dbo
  AS SELECT * FROM result_df
