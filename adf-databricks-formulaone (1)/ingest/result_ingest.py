# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

# Create dataframe on top of json file
df = spark.read.json(path = f"/mnt/formulaone/bronze/results/{formatted_date}/")
# df.display()

# COMMAND ----------

# Do some transformation
results_df = df.select("MRData.RaceTable.Races.round", "MRData.RaceTable.Races.Circuit.circuitId", "MRData.RaceTable.Races.Results")
# results_df.display()

# COMMAND ----------

# Do some transformation
results_df = results_df.withColumn("round", explode("round")).withColumn("circuitid", explode("circuitid")).withColumn("Results", explode("Results")).withColumn("Results", explode("Results"))
# results_df.display()

# COMMAND ----------

# Transform the data as per requirement
df1 = results_df.select("round", "circuitid", "Results.Constructor.constructorId", "Results.Driver.driverId", col("Results.FastestLap.Time.time").alias("FastestLapTime"), "Results.grid", "Results.laps", "Results.position", "Results.points", "Results.positionText","Results.Time.time")
# , "Results.Time.time", "Results.Time.millis"
df1 = df1.distinct()
# df1.display()

# COMMAND ----------

column_mapping = {"circuitid":"circuit_id","constructorId":"constructor_id","driverId":"driver_id"}
df1 = df1.withColumnsRenamed(column_mapping)
# df1.display()

# COMMAND ----------

# Write result as delta table in silver layer
df1.repartition(1).write.mode("overwrite").format("delta").save(f"/mnt/formulaone/silver/results/{formatted_date}/")
