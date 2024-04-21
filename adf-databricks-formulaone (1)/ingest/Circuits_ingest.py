# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

# Reading data from json files stored in adls gen2
df = spark.read.json(f"/mnt/formulaone/bronze/Circuits/{formatted_date}/")

# COMMAND ----------

# df.display()

# COMMAND ----------

# Selecting the required columns and doing some transformation.
df = (
    df.withColumn("circuit_id", col("circuitid"))
    .withColumn("circuit_name", col("circuitName"))
    .withColumn("Locality", col("Location.locality"))
    .withColumn("country", col("Location.country"))
    .withColumn("lat", col("Location.lat"))
    .withColumn("long", col("Location.long"))
    .withColumn("url", col("url"))
)

# COMMAND ----------

# df.display()

# COMMAND ----------

# Dropping the unwanted columns.
df = df.drop(col("Location"),col("circuitid"), col("circuitName"))

# COMMAND ----------

# Selecting the required columns.
circuit_df = df.select("circuit_id","circuit_name","Locality","country","lat","long","url")
# circuit_df.display()

# COMMAND ----------

# Save the result into silver layer
circuit_df.distinct().write.mode("overwrite").format("delta").save(f"/mnt/formulaone/silver/Circuits/{formatted_date}/")
