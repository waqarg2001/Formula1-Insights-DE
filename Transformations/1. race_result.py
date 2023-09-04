# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

# MAGIC %run "../include/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
vp_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

races=spark.read.format('delta').load(f'{processed_path}/races')
circuits=spark.read.format('delta').load(f'{processed_path}/circuits')
drivers=spark.read.format('delta').load(f'{processed_path}/drivers')
constructors=spark.read.format('delta').load(f'{processed_path}/constructors')
results=spark.read.format('delta').load(f'{processed_path}/results') \
        .filter(f"`File Date` = '{vp_file_date}'") \
        .withColumnRenamed('time','race_time') \
        .withColumnRenamed('race_id','result_race_id')

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit
new_race=races.join(circuits,races.circuit_id==circuits.circuit_id,'inner').select(races.race_id,races.race_year,races.name,races.race_timestamp,circuits.location)


new_race=results.join(new_race,results.result_race_id==new_race.race_id) \
                .join(drivers,results.driver_id==drivers.driver_id) \
                .join(constructors,results.constructor_id==constructors.constructor_id) \
                .withColumn('created_date',current_timestamp()) \
                .withColumn('File_Date', lit(vp_file_date))
                

# COMMAND ----------

display(new_race)

# COMMAND ----------

new_race1=new_race.select(races.race_year,results.result_race_id.alias('race_id'),races.name.alias('Race'),circuits.location,races.race_timestamp.alias('race date'),drivers.name.alias('Driver'),drivers.number,drivers.nationality,constructors.name.alias('Team'),results.grid,results.fastest_lap,results.race_time,results.points,'created_date',results.position,'File_Date')

# COMMAND ----------

#new_race.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.result')
#incremental_1(new_race,'f1_presentation','result','race_id')

condition="tgt.race_id=src.race_id and tgt.driver_id=src.driver_id"
merge_delta_data(new_race,'f1_presentation','result','/mnt/f1racedlwg/presentation',condition,'race_id')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.result

# COMMAND ----------

