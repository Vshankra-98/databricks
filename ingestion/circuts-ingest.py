# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read csv file.
# MAGIC 2. apply schema for it.
# MAGIC 3. Rename and remove based on the requirment

# COMMAND ----------

# MAGIC %run ../Utils/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,FloatType

from datetime import datetime 
from pyspark.sql.functions import lit

input_schema = StructType(
    [
    StructField('circuitId', IntegerType()),
    StructField('circuitRef', StringType()),
     StructField('name', StringType()),
    StructField('location', StringType()),
     StructField('country', StringType()),
    StructField('lat', FloatType()),
     StructField('lng', FloatType()),
    StructField('alt', IntegerType()),
    StructField('url', StringType())
]
    )



# COMMAND ----------


df = create_csv_df('dbfs:/mnt/bronze/circuits.csv' , input_schema)
display(df)
df.printSchema()

# COMMAND ----------

# rename column name 
df = df.withColumnRenamed("circuitId", 'circuit_id').withColumnRenamed("circuitRef", 'circuit_ref')
df.display()

# COMMAND ----------

# adding new column with date
dfine_current_dt =  datetime.today().strftime("%Y-%m-%d")
df = df.withColumn("ingest_dt", lit(dfine_current_dt))

display(df)


# COMMAND ----------

display(dbutils.fs.ls('/mnt/bronze'))


# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/silver/circuts/")

# COMMAND ----------

df.write.parquet("dbfs:/mnt/silver/circuts/", mode = 'overwrite')

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/silver/circuts/"))

# COMMAND ----------


