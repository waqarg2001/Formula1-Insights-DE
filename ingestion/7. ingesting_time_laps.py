# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

# MAGIC %run "../include/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_src','')
v_data_src=dbutils.widgets.get('p_data_src')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-28')
vp_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

schema_time_laps='race_id INT,driver_id INT, lap INT, position INT,time STRING,milliseconds INT'

time_laps_df=spark.read.schema(schema_time_laps).option('multiLine',True).csv(f'{raw_path}/{vp_file_date}/lap_times')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
time_laps_renamed=ingest_date(time_laps_df)
time_laps_renamed=time_laps_renamed.withColumn('Data Source',lit(v_data_src)) \
                                    .withColumn('File Date',lit(vp_file_date))

# COMMAND ----------

time_laps_renamed=time_laps_renamed.select([
 'driver_id',
 'lap',
 'position',
 'time',
 'milliseconds',
 'ingestion_date',
 'Data Source',
 'File Date',
 'race_id'])

# COMMAND ----------

#time_laps_renamed.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.time_laps')

# COMMAND ----------

#incremental(time_laps_renamed,'time_laps')

condition="tgt.driver_id=src.driver_id and tgt.race_id=src.race_id"
merge_delta_data(time_laps_renamed,'f1_processed','time_laps','/mnt/f1racedlwg/processed',condition,'race_id')


# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.time_laps group by race_id order by race_id desc

# COMMAND ----------

