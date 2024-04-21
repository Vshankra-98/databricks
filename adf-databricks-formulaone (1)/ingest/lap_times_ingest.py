# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

# Create a schema for lap_times
input_schema = StructType(
    [
        StructField("race_id", IntegerType()),
        StructField("driver_id", IntegerType()),
        StructField("lap", IntegerType()),
        StructField("position",IntegerType()),
        StructField("time", StringType()),
        StructField("milliseconds", IntegerType()),
    ]
)

# COMMAND ----------

# Read the data from "lap_times/" into a Spark DataFrame. lap_times/ contains multiple csv files
df = spark.read.csv(f"/mnt/formulaone/bronze/lap times/{formatted_date}/", schema = input_schema, header = False)

# COMMAND ----------

# Save spark dataframe as deltatableat target location
df.distinct().write.mode("overwrite").format("delta").save(f"/mnt/formulaone/silver/lap times/{formatted_date}/")
