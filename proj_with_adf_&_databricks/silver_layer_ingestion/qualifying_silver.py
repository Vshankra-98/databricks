# Databricks notebook source
display(dbutils.fs.ls('/mnt/formula-one/bronze/'))

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

df = spark.read.option("recursiveFileLookup", "true").json('dbfs:/mnt/formula-one/bronze/qualifying/', multiLine=True).select('MRData.RaceTable.Races')
df = json_flat(df)
df = json_flat(df)
df = json_flat(df)
df = json_flat(df)
df = json_flat(df)
df = json_flat(df)

display(df)

# COMMAND ----------


