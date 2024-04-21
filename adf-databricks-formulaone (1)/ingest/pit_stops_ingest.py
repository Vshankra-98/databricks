# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

# Create a schema for pit_stops
input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("stop", IntegerType()),
        StructField("lap", IntegerType()),
        StructField("time", StringType()),
        StructField("duration", StringType()),
        StructField("milliseconds", IntegerType()),
    ]
)

# COMMAND ----------

dbutils.fs.ls(f"/mnt/formulaone/bronze/pit stops/{formatted_date}/")

# COMMAND ----------

# Read the data from "pit_stops.json" into a Spark DataFrame. pit_stops is multiline json file
df = spark.read.json(f"/mnt/formulaone/bronze/pit stops/{formatted_date}/", schema = input_schema, multiLine = True)
# df.display()

# COMMAND ----------

# Assuming you have columns 'old_name1', 'old_name2', etc.
column_mapping = {"raceId": "race_id", "driverId": "driver_id"}

df = df.withColumnsRenamed(column_mapping)

# COMMAND ----------

# Save the data frame as delta table in silver layer
df.distinct().write.mode("overwrite").format("delta").save(f"/mnt/formulaone/silver/pit stops/{formatted_date}/")
