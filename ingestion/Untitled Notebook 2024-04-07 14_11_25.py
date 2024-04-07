# Databricks notebook source
dbutils.fs.ls('/mnt/bronze/')

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

input_schema = StructType([
    StructField('resultId', IntegerType()),
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('constructorId', IntegerType()),
    StructField('number', IntegerType()),
    StructField('grid', IntegerType()),
    StructField('position', IntegerType()),
    StructField('positionText', StringType()),
    StructField('positionOrder', IntegerType()),
    StructField('points', FloatType()),
    StructField('laps', IntegerType()),
    StructField('time', StringType()),
    StructField('milliseconds', IntegerType()),
    StructField('fastestLap', IntegerType()),
    StructField('rank', IntegerType()),
    StructField('fastestLapTime', StringType()),
    StructField('fastestLapSpeed', StringType()),   
    StructField('statusId', IntegerType()),

])

# COMMAND ----------

df =spark.read.json('dbfs:/mnt/bronze/results.json', schema= input_schema)

df.display()

# COMMAND ----------


