# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

# MAGIC %run "../include/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_src','')
v_data_src=dbutils.widgets.get('p_data_src')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
vp_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

pit_stops_schema='driverId INT, duration STRING,lap INT,milliseconds INT, raceId INT,stop STRING, time STRING'

# COMMAND ----------

pit_stops_df=spark.read.option('multiLine',True).schema(pit_stops_schema).json(f'{raw_path}/{vp_file_date}/pit_stops.json')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
pit_stops_renamed=pit_stops_df.withColumnRenamed('driverId','driver_id') \
    .withColumnRenamed('raceId','race_id') \
    .withColumn('Data Source',lit(v_data_src)) \
    .withColumn('File Date',lit(vp_file_date))

pit_stops_renamed=ingest_date(pit_stops_renamed)

# COMMAND ----------

#pit_stops_renamed.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

pit_stops_renamed=pit_stops_renamed.select(['driver_id',
 'duration',
 'lap',
 'milliseconds',
 'stop',
 'time',
 'Data Source',
 'File Date',
 'ingestion_date',
 'race_id'])

# COMMAND ----------

#incremental(pit_stops_renamed,'pit_stops')

condition="tgt.driver_id=src.driver_id and tgt.race_id=src.race_id"
merge_delta_data(pit_stops_renamed,'f1_processed','pit_stops','/mnt/f1racedlwg/processed',condition,'race_id')


# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.pit_stops group by race_id order by race_id desc

# COMMAND ----------

