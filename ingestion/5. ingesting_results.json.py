# Databricks notebook source
# MAGIC %run "../include/common_functions"

# COMMAND ----------

# MAGIC %run "../include/configuration"

# COMMAND ----------

dbutils.widgets.text('p_data_src','')
v_data_src=dbutils.widgets.get('p_data_src')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
vp_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

result_schema='constructorId INT, driverId INT,fastestLap INT,fastestLapSpeed FLOAT,grid INT,laps INT,milliseconds INT,number INT, points INT,position INT,positionOrder INT,positionText INT,raceId INT,rank INT,resultId INT,statudId INT, time STRING '

# COMMAND ----------

result_df=spark.read.schema(result_schema).json(f'{raw_path}/{vp_file_date}/results.json')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
result_df_renamed=result_df.withColumnRenamed('constructorId','constructor_id') \
    .withColumnRenamed('driverId','driver_id') \
    .withColumnRenamed('raceId','race_id') \
    .withColumnRenamed('resultId','result_id') \
    .withColumnRenamed('positionId','position_id') \
    .withColumnRenamed('positionText','position_text') \
    .withColumnRenamed('positionOrder','position_order') \
    .withColumnRenamed('fastestLapTime','fastest_lap_time') \
    .withColumnRenamed('fastestLap','fastest_lap') \
    .withColumnRenamed('fastestLapSpeed','fastest_lap_speed') \
    .withColumn('Data Source',lit(v_data_src)) \
    .withColumn('File Date',lit(vp_file_date)) \
    .drop('statudId')


result_df_renamed=ingest_date(result_df_renamed)
    


# COMMAND ----------

de_duped_result_df_renamed=result_df_renamed.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Incremental Load Method 1

# COMMAND ----------

# for race_id_list in result_df_renamed.select('race_id').distinct().collect():
#      if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#          spark.sql(f"alter table f1_processed.results  drop if exists partition(race_id={race_id_list.race_id})")

# COMMAND ----------

# result_df_renamed.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Incremental Load Method 2

# COMMAND ----------

spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')


# COMMAND ----------

result_df_renamed=result_df_renamed.select(['constructor_id',
 'driver_id',
 'fastest_lap',
 'fastest_lap_speed',
 'grid',
 'laps',
 'milliseconds',
 'number',
 'points',
 'position',
 'position_order',
 'position_text',
 'rank',
 'result_id',
 'time',
 'Data Source',
 'File Date',
 'ingestion_date',
 'race_id'])

# COMMAND ----------


condition="tgt.result_id=src.result_id and tgt.race_id=src.race_id"
merge_delta_data(de_duped_result_df_renamed,'f1_processed','results','/mnt/f1racedlwg/processed',condition,'race_id')


# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.results group by race_id

# COMMAND ----------



# COMMAND ----------

