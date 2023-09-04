# Databricks notebook source
output=dbutils.notebook.run("1. ingesting_circuits.csv",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
output

# COMMAND ----------

output=dbutils.notebook.run("2. ingesting_races.csv",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
output

# COMMAND ----------

output=dbutils.notebook.run("3. ingesting_constructors.json",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
output

# COMMAND ----------

output=dbutils.notebook.run("4. ingesting_drivers.json",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
output

# COMMAND ----------

output=dbutils.notebook.run("5. ingesting_results.json",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
output

# COMMAND ----------

output=dbutils.notebook.run("6. ingesting_pit_stops.json",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
output

# COMMAND ----------

output=dbutils.notebook.run("7. ingesting_time_laps",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
output

# COMMAND ----------

output=dbutils.notebook.run("8. ingesting_qualifying",0,{"p_data_source":"Ergast API","p_file_date":"2021-04-18"})
output