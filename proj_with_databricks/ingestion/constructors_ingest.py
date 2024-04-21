# Databricks notebook source
# MAGIC %run ../Utils/common_functions

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import IntegerType,StructField, StructType,StringType,FloatType
from pyspark.sql.functions import lit,col

# COMMAND ----------

# define Schema

input_schema = StructType([
    StructField('constructorId', IntegerType()),
    StructField('constructorRef', StringType()),
    StructField('name', StringType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

dbutils.fs.ls('/mnt/dataricks-formula-one/bronze')

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/dataricks-formula-one/bronze/constructors.json', schema=input_schema)
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Few Trasformations
# MAGIC

# COMMAND ----------

current_dt = datetime.today().strftime('%Y-%m-%d')

# COMMAND ----------

df = df.withColumnRenamed('constructorId', 'constructor_Id').withColumnRenamed('constructorRef', 'constructor_Ref').withColumn('ingest_dt', lit(current_dt))

display(df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,isnull, when

columns = df.columns
df1 = df
for  i in columns:
    df1 = df1.withColumn(i, regexp_replace(i,'\\\\N','')).withColumn(i, when(col(i).isNull(), '').otherwise(col(i)))
    
display(df1)

df1.write.parquet('/mnt/dataricks-formula-one/silver/constructors',mode ='overwrite')

# COMMAND ----------


