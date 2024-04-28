# Databricks notebook source
from pyspark.sql.types import *


input_schema = StructType([
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('lap', IntegerType()),
    StructField('position', IntegerType()),
    StructField('time', StringType()),
    StructField('milliseconds', IntegerType()),
])

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula-one/bronze/'))

# COMMAND ----------

df = spark.read.csv('dbfs:/mnt/formula-one/bronze/lap_times/', schema=input_schema)
df.display()
df.count()
df.printSchema()

# COMMAND ----------



df = df.withColumnRenamed('raceId', 'race_Id').withColumnRenamed('driverId','driver_Id').select('race_Id', 'driver_Id', 'lap', 'position')
df.display()

# COMMAND ----------

df.write.mode('overwrite').format('delta').save('/mnt/formula-one/silver/lap_times')

# COMMAND ----------


