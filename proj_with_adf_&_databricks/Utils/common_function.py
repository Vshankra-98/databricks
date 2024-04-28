# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text('datset_name','')
dbutils.widgets.text('layer_name','')

# COMMAND ----------

def removing_null(dataframe):
    from pyspark.sql.functions import regexp_replace,isnull, when

    columns = df.columns
    df1 = df
    for  i in columns:
        df1 = df1.withColumn(i, regexp_replace(i,'\\\\N','')).withColumn(i, when(col(i).isNull(), '').otherwise(col(i)))
    return df1
        
  

# COMMAND ----------


