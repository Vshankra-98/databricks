# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

# Reading race file from bronze layer
df = spark.read.json(f"/mnt/formulaone/bronze/races/{formatted_date}/")
# df.display()

# COMMAND ----------

# Rename the column as per requirement.
df = df.select(col("date").alias("race_date"),"raceName", "round", "season", col("time").alias("race_time"), col("Circuit.circuitId").alias("circuit_id"))

# COMMAND ----------

# df.display()

# COMMAND ----------

# Save data as delta live table in silver layer
df.distinct().repartition(1).write.mode("overwrite").format("delta").save(f"/mnt/formulaone/silver/races/{formatted_date}/")
