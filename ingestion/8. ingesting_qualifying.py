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

qualifying_schema='qualifyId INT,raceId INT,driverId INT,constructorId INT,number INT, position INT,q1 STRING,q2 STRING, q3 STRING'
qualifying_df=spark.read.option('multiLine',True).schema(qualifying_schema).json(f'{raw_path}/{vp_file_date}/qualifying')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
qualifying_df_renamed=qualifying_df.withColumnRenamed('qualifyId','qualify_id') \
.withColumnRenamed('raceId','race_id') \
.withColumnRenamed('driverId','driver_id') \
.withColumnRenamed('constructorId','constructor_id') \
.withColumn('Data Source',lit(v_data_src)) \
.withColumn('File Date',lit(vp_file_date))

qualifying_df_renamed=ingest_date(qualifying_df_renamed)

# COMMAND ----------

#qualifying_df_renamed.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

qualifying_df_renamed=qualifying_df_renamed.select(['qualify_id',
 'driver_id',
 'constructor_id',
 'number',
 'position',
 'q1',
 'q2',
 'q3',
 'Data Source',
 'File Date',
 'ingestion_date',
 'race_id'])


# COMMAND ----------

#incremental(qualifying_df_renamed,'qualifying')

condition="tgt.driver_id=src.driver_id and tgt.race_id=src.race_id"
merge_delta_data(qualifying_df_renamed,'f1_processed','qualifying','/mnt/f1racedlwg/processed',condition,'race_id')


# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.qualifying group by race_id order by race_id desc

# COMMAND ----------

