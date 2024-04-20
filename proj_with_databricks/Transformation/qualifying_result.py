# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/")

# COMMAND ----------

qualifying_df = spark.read.parquet('dbfs:/mnt/silver/qualifying/')
dirver_df = spark.read.parquet('dbfs:/mnt/silver/drivers/')
construnctor_df = spark.read.parquet('dbfs:/mnt/silver/constructors/')


# COMMAND ----------

qualifying_df1 = qualifying_df.select('driver_Id', 'constructor_Id', col('q1').alias('Qualifying1'), col('q2').alias('Qualifying2'), col('q3').alias('Qualifying3'))


construnctor_df1 = construnctor_df.select('constructor_Id', col('constructor_Ref').alias('Team'))


dirver_df1 = dirver_df.select(concat(col('Forename'), lit('  '), col('surname')).alias('Driver'), col('driver_Id') , col('Number'))


# COMMAND ----------


Qualifying = qualifying_df1.join(dirver_df1,'driver_id', 'left').join(construnctor_df1, 'constructor_Id','left')


# COMMAND ----------

list_columns =['Driver', 'Number', "Team", 'Qualifying1', 'Qualifying2', 'Qualifying3']
Qualifying = Qualifying.select('Driver', 'Number', "Team", 'Qualifying1', 'Qualifying2', 'Qualifying3')
Qualifying = Qualifying.select(list_columns)

Qualifying.display()

# COMMAND ----------

Qualifying.write.mode('overwrite').parquet("/mnt/gold/qualifying_result")

# COMMAND ----------


