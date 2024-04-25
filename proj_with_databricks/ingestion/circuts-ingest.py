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



# COMMAND ----------


df = create_csv_df('dbfs:/mnt/dataricks-formula-one/bronze/circuits.csv' , input_schema)
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

display(dbutils.fs.ls('/mnt/dataricks-formula-one/bronze'))


# COMMAND ----------

from pyspark.sql.functions import regexp_replace,isnull, when

columns = df.columns
df1 = df
for  i in columns:
    df1 = df1.withColumn(i, regexp_replace(i,'\\\\N','')).withColumn(i, when(col(i).isNull(), '').otherwise(col(i)))
    
display(df1)

df1.write.parquet("dbfs:/mnt/dataricks-formula-one/silver/circuts/", mode = 'overwrite')

# COMMAND ----------


