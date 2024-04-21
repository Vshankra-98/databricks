# Databricks notebook source
# dbutils.notebook.run(path, timeout_seconds=None, arguments=None)
dbutils.notebook.run("Circuits_ingest", 0, arguments={"formatted_date": "20240417"})

# COMMAND ----------

# dbutils.notebook.run(path, timeout_seconds=None, arguments=None)
dbutils.notebook.run("constructors_ingest", 0, arguments={"formatted_date": "20240417"})

# COMMAND ----------

# dbutils.notebook.run(path, timeout_seconds=None, arguments=None)
dbutils.notebook.run("drivers_ingest", 0, arguments={"formatted_date": "20240417"})

# COMMAND ----------

# dbutils.notebook.run(path, timeout_seconds=None, arguments=None)
dbutils.notebook.run("lap_times_ingest", 0, arguments={"formatted_date": "20240417"})

# COMMAND ----------

# dbutils.notebook.run(path, timeout_seconds=None, arguments=None)
dbutils.notebook.run("pit_stops_ingest", 0, arguments={"formatted_date": "20240417"})

# COMMAND ----------

# dbutils.notebook.run(path, timeout_seconds=None, arguments=None)
dbutils.notebook.run("qualifying_ingest", 0, arguments={"formatted_date": "20240417"})

# COMMAND ----------

# dbutils.notebook.run(path, timeout_seconds=None, arguments=None)
dbutils.notebook.run("race_ingest", 0, arguments={"formatted_date": "20240417"})

# COMMAND ----------

# dbutils.notebook.run(path, timeout_seconds=None, arguments=None)
dbutils.notebook.run("result_ingest", 0, arguments={"formatted_date": "20240417"})
