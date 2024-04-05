# Databricks notebook source
dbutils.notebook.run('/Workspace/Repos/sachink9763@gmail.com/bwt_formula_one/ingestion/circuts-ingest', 0)

# COMMAND ----------


dbutils.notebook.run('constructors_ingest',0)

# COMMAND ----------

dbutils.notebook.run('drivers_ingest',0)
dbutils.notebook.run('lap_times_ingest',0)
dbutils.notebook.run('pitStops_ingest',0)
dbutils.notebook.run('qualifying_ingest',0)
dbutils.notebook.run('race_ingest',0)
dbutils.notebook.run('results_ingest',0)

# COMMAND ----------


