# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/bronze'))

# COMMAND ----------

from pyspark.sql.functions import  col

df= spark.read.option('recursiveFileLookup', 'true').json('dbfs:/mnt/formula-one/bronze/races/').select(['Circuit',  'Qualifying', 'date', 'raceName', 'round', 'season', 'time'])

list_fiels = df.schema.fields
print

for i in list_fiels:
    if str(i.dataType).startswith('StructType'):
        nested_fields =[col(f"{i.name}.{j.name}").alias(f"{i.name}_{j.name}") for j in i.dataType.fields]
        df = df.select('*', *nested_fields).drop(i.name)
print(df.columns)
df = df.select(['date', 'raceName', 'round', 'season', 'time',  'Circuit_circuitId', 'Circuit_circuitName',  'Qualifying_date', 'Qualifying_time'])




display(df)

# COMMAND ----------

# 

# COMMAND ----------

df.write.mode('overwrite').format('delta').save('dbfs:/mnt/formula-one/silver/races/')

# COMMAND ----------


