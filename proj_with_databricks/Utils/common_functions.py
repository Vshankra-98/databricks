# Databricks notebook source


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,FloatType

from datetime import datetime 
from pyspark.sql.functions import lit, col, concat, lit, asc_nulls_last


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

def create_csv_df(input_location, input_chema ):
    """
    this function is used to creating spark dataframe on csv file location
    : input_location : provide input csv file location
    : schema : provide input schema
    : return : spar dataframe
    """
    return spark.read.csv(input_location , header = True, schema = input_chema)

