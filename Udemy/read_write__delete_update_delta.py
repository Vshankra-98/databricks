# Databricks notebook source
dbutils.fs.ls('mnt/dataricks-formula-one/bronze/')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## desc history schema.table_name

# COMMAND ----------

circut_df = spark.read.csv('dbfs:/mnt/dataricks-formula-one/bronze/circuits.csv', header=True, inferSchema=True)
display(circut_df)
circut_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists circuit_table(
# MAGIC circuitId  integer,
# MAGIC circuitRef  string,
# MAGIC name  string ,
# MAGIC location  string,
# MAGIC country  string,
# MAGIC lat  double ,
# MAGIC lng double ,
# MAGIC alt integer,
# MAGIC url string 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended circuit_table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table delta.`dbfs:/mnt/udemy/circuit_External_table`(
# MAGIC circuitId  integer,
# MAGIC circuitRef  string,
# MAGIC name  string ,
# MAGIC location  string,
# MAGIC country  string,
# MAGIC lat  double ,
# MAGIC lng double ,
# MAGIC alt integer,
# MAGIC url string 
# MAGIC ) using delta 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create database demo 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists demo.circuit_table_external(
# MAGIC circuitId  integer,
# MAGIC circuitRef  string,
# MAGIC name  string ,
# MAGIC location  string,
# MAGIC country  string,
# MAGIC lat  double ,
# MAGIC lng double ,
# MAGIC alt integer,
# MAGIC url string 
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe EXTENDED    demo.circuit_table_external

# COMMAND ----------

# MAGIC %sql
# MAGIC create database  demo1 location   'dbfs:/mnt/udemy/'

# COMMAND ----------

circut_df.write.mode('overwrite').format('delta').saveAsTable('default.circuit_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.circuit_table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended default.circuit_table

# COMMAND ----------

# MAGIC %sql
# MAGIC create database demo location 'dbfs:/mnt/udemy'

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended circuit circuit_External_table

# COMMAND ----------

circut_df.write.mode('overwrite').format('delta').save('dbfs:/mnt/udemy/circuit_external_from_df')

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended   demo.circuit_external_from_df

# COMMAND ----------

from delta import DeltaTable

DeltaTable.createOrReplace(spark) \
  .addColumn("circuitId", "INT") \
  .addColumn("circuitRef", "STRING") \
  .addColumn("name", "STRING") \
  .addColumn("location", "STRING", comment = "surname") \
  .addColumn("country", "STRING") \
  .addColumn("lat", "string") \
  .addColumn("lng", "float") \
  .addColumn("alt", "float") \
  .addColumn("url", "string") \
  .property("description", "table with people circuit") \
  .location('dbfs:/mnt/dataricks-formula-one/bronze/circuits.csv') \
  .execute()



# COMMAND ----------

# MAGIC %sql
# MAGIC desc history default.circuit_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.circuit_table version as of 0

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history default.circuit_table

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from default.circuit_table timestamp as of '2024-04-28T17:29:59.000+00:00' where circuitId = '1'

# COMMAND ----------


