# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/bronze'))

# COMMAND ----------

df =spark.read.option('recursiveFileLookup', 'true').json('dbfs:/mnt/formula-one/bronze/results/')


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import  col, explode


def json_flat(df):
    list_fiels = df.schema.fields

    for i in list_fiels:
        if str(i.dataType).startswith('StructType'):
            nested_fields =[col(f"{i.name}.{j.name}").alias(f"{i.name}_{j.name}") for j in i.dataType.fields]
            df = df.select('*', *nested_fields).drop(i.name)
        if  str(i.dataType).startswith('ArrayType'):
            df = df.withColumn(f"{i.name}", explode(i.name))
    return df

# COMMAND ----------


df= spark.read.option('recursiveFileLookup', 'true').json('dbfs:/mnt/formula-one/bronze/results/')
df =json_flat(df)
df = json_flat(df)
df = json_flat(df)
df = json_flat(df)

# print(df.columns)
columns =['date', 'raceName', 'round', 'season',   'Circuit_circuitId', 'Circuit_circuitName','Results_grid', 'Results_laps', 'Results_number', 'Results_points', 'Results_position',  'Results_Constructor_constructorId', 'Results_Constructor_name',   'Results_Driver_driverId', 'Results_Driver_familyName', 'Results_Driver_givenName', 'Results_Driver_nationality', 'Results_Driver_permanentNumber',  'Results_FastestLap_rank', 'Results_Time_millis', 'Results_Time_time',  'Results_FastestLap_Time_time']
df = df.select(columns)
display(df)

# COMMAND ----------

df.write.mode('overwrite').format('delta').save('dbfs:/mnt/formula-one/silver/results/')

# COMMAND ----------


