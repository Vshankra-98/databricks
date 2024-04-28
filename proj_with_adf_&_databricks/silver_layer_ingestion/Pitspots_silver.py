# Databricks notebook source
display(dbutils.fs.ls('mnt/formula-one/bronze/pitstops/'))

# COMMAND ----------

# define Schema
from pyspark.sql.types import *

input_scema = StructType([
            StructField('raceId', StringType()),
            StructField('driverId', StringType()),
            StructField('stop', StringType()),
            StructField('lap', StringType()),
            StructField('time', StringType()),
            StructField('duration', StringType()),
            StructField('milliseconds', StringType())
])

#  "raceId":841,
#       "driverId":153,
#       "stop":1,
#       "lap":1,
#       "time":"17:05:23",
#       "duration":26.898,
#       "milliseconds":26898

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.option("recursiveFileLookup", "true").json('dbfs:/mnt/formula-one/bronze/pitstops/', multiLine=True)
df = df.select(col("MRData.RaceTable.Races.PitStops"))
display(df)



# COMMAND ----------

from pyspark.sql.functions import explode,col

df1 = df.select((explode('PitStops')).alias('pitStops'))
df1 = df1.select((explode('pitStops')).alias('pitStops'))


field_list = df1.schema.fields

for i in field_list:
    if str(i.dataType).startswith('StructType'):
           nested_fields = [f"{i.name}.{j.name}" for j in i.dataType.fields]
           df1 = df1.select(nested_fields)
   
df1 = df1.withColumnRenamed('driverId', 'driver_Id').withColumnRenamed('stop', 'Pits')
display(df1)

# COMMAND ----------

df1.write.mode('overwrite').format('delta').option("mergeSchema", "true").save('dbfs:/mnt/formula-one/silver/pitstops/')

# COMMAND ----------

 
