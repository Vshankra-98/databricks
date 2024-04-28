# Databricks notebook source


# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula-one/silver/"))

# COMMAND ----------

qualifying_df = spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/qualifying')
dirver_df = spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/drivers')
construnctor_df = spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/constructors')


# COMMAND ----------


from pyspark.sql.functions import  col, concat


# qualifying_df1 = qualifying_df.select('driverId', 'constructorId', col('q1').alias('Qualifying1'), col('q2').alias('Qualifying2'), col('q3').alias('Qualifying3'))

display(qualifying_df)

result_df = spark.read.format('delta').load('dbfs:/mnt/formula-one/silver/results')
display(result_df)

display(construnctor_df)

# construnctor_df1 = construnctor_df.select('constructor_Id', col('constructor_Ref').alias('Team'))


# dirver_df1 = dirver_df.select(concat(col('Forename'), lit('  '), col('surname')).alias('Driver'), col('driver_Id') , col('Number'))


# COMMAND ----------


Qualifying = qualifying_df1.join(dirver_df1,'driver_id', 'left').join(construnctor_df1, 'constructor_Id','left')


# COMMAND ----------

list_columns =['Driver', 'Number', "Team", 'Qualifying1', 'Qualifying2', 'Qualifying3']
Qualifying = Qualifying.select('Driver', 'Number', "Team", 'Qualifying1', 'Qualifying2', 'Qualifying3')
Qualifying = Qualifying.select(list_columns)

Qualifying.display()
Qualifying.count()

# COMMAND ----------

Qualifying.write.mode('overwrite').parquet("/mnt/dataricks-formula-one/gold/qualifying_result")

# COMMAND ----------


