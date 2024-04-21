# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

dbutils.widgets.text("formatted_date","20240417")
formatted_date = dbutils.widgets.get("formatted_date")

# COMMAND ----------

# Creating a dataframe on the top of parquet file in silver layer
drivers_df = spark.read.format("delta").load(f"/mnt/formulaone/silver/drivers/{formatted_date}/")
# drivers_df.display()

# COMMAND ----------

drivers_df = drivers_df.select("driver_id","nationality","Driver","Number")
# drivers_df.display()

# COMMAND ----------

# Create dataframe based on locations
constructors_df = spark.read.format("delta").load(f"/mnt/formulaone/silver/constructors/{formatted_date}/")

# constructors_df.display()

# COMMAND ----------

# Select the columns which are required for qualifying result
# Constructors: constructor_id, name
constructors_df = constructors_df.select("constructor_id", col("name").alias("Team"), col("constructor_ref"))

# COMMAND ----------

# constructors_df.display()

# COMMAND ----------

# Create dataframe based on locations
results_df = spark.read.format("delta").load(f"/mnt/formulaone/silver/results/{formatted_date}/")

# results_df.display()

# COMMAND ----------

results_df = results_df.select(col("circuit_id"), col("driver_id"), col("constructor_id"), col("grid"), col("FastestLapTime"), col("time"), col("points"))

# Replacing '\N' in the column FastestLapTime
results_df = results_df.withColumn("FastestLapTime", regexp_replace(col("FastestLapTime"), r"[\\,N]", "")).withColumn("time", regexp_replace(col("time"), r"[\\,N]", ""))

# results_df.display()

# COMMAND ----------

final_race = results_df.join(drivers_df,['driver_id'],'left').select(col("nationality").alias("Nationality"),col("Driver"), col("Number"),col("constructor_id"),col("grid"),col("FastestLapTime"),col("time").alias("Race Time"),col("points").alias("Points"),col("circuit_id")).join(constructors_df,results_df.constructor_id == constructors_df.constructor_ref,'left')

# COMMAND ----------

final_race = final_race.select("Nationality","Driver","Number","Team","grid", "FastestLapTime", "Race Time","Points","circuit_id")
# final_race.display()

# COMMAND ----------

dbutils.fs.ls("/mnt/formulaone/silver/races/")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"/mnt/formulaone/silver/races/{formatted_date}/").select(col("season"),col("circuit_id"))
# races_df.display()

# COMMAND ----------

f_standing = final_race.join(races_df,['circuit_id'],'left').filter(col("FastestLapTime")!= '').distinct().withColumn("Ranking",row_number().over(Window.partitionBy("season", "circuit_id").orderBy(col("Points").cast("int").desc()))).withColumnRenamed("Race Time","Race_Time")
# f_standing.display()
# Get the standing of abu dhabhi grand prix
# f_standing.filter((col("season")=='2018') & (col("circuit_id")=='yas_marina')).withColumnRenamed("Race Time","Race_Time").display()

# COMMAND ----------

f_standing.write.mode("overwrite").format("delta").save(f"/mnt/formulaone/gold/result_table/{formatted_date}/")
