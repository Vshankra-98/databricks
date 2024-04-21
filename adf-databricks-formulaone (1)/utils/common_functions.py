# Databricks notebook source
from pyspark.sql.functions import col, explode, concat, lit,struct, regexp_replace, least, when, row_number,year,sum
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType
)

# COMMAND ----------

from datetime import datetime

# Get the current date and time
now = datetime.now()

# Format the date as YYYYMMDD using strftime()
formatted_date1 = now.strftime("%Y%m%d")

