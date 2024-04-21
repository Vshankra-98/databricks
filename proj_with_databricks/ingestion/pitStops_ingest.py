# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

# define Schema
from pyspark.sql.types import DoubleType

input_scema = StructType([
            StructField('raceId', IntegerType()),
            StructField('driverId', IntegerType()),
            StructField('stop', IntegerType()),
            StructField('lap', IntegerType()),
            StructField('time', StringType()),
            StructField('duration', DoubleType()),
            StructField('milliseconds', DoubleType())
])

#  "raceId":841,
#       "driverId":153,
#       "stop":1,
#       "lap":1,
#       "time":"17:05:23",
#       "duration":26.898,
#       "milliseconds":26898

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dataricks-formula-one/bronze/'))

# COMMAND ----------

df= spark.read.json('dbfs:/mnt/dataricks-formula-one/bronze/pit_stops.json', multiLine=True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,isnull, when

columns = df.columns
df1 = df
for  i in columns:
    df1 = df1.withColumn(i, regexp_replace(i,'\\\\N','')).withColumn(i, when(col(i).isNull(), '').otherwise(col(i)))
    
display(df1)
df1.write.parquet('/mnt/dataricks-formula-one/silver/pit_stops', mode ='overwrite')

# COMMAND ----------


