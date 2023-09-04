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

races_df=spark.read.option('header',True).csv(f'{raw_path}/{vp_file_date}/races.csv')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col,to_timestamp,concat
races_df_renamed=races_df.withColumnRenamed('raceId','race_id') \
    .withColumnRenamed('year','race_year') \
    .withColumnRenamed('circuitId','circuit_id') \
    .withColumn('Data Source',lit(v_data_src)) \
    .withColumn('File Date',lit(vp_file_date))



# COMMAND ----------


races_df_new=races_df_renamed.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))
races_df_new=ingest_date(races_df_new)
races_df_final=races_df_new.select("race_id","race_year",'round',"circuit_id","name",'race_timestamp','ingestion_date')


# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,TimestampType
races_schema= StructType(fields=[StructField('race_id',IntegerType(),False),
                                 StructField('race_year',IntegerType(),True),
                                 StructField('round',IntegerType(),True),
                                 StructField('circuit_id',IntegerType(),True),
                                 StructField('name',StringType(),True),
                                 StructField('race_timestamp',TimestampType(),False),
                                 StructField('ingestion_date',TimestampType(),False)])

races_df_final = races_df_final \
    .withColumn('race_id', col('race_id').cast(races_schema['race_id'].dataType)) \
    .withColumn('race_year', col('race_year').cast(races_schema['race_year'].dataType)) \
    .withColumn('round', col('round').cast(races_schema['round'].dataType)) \
    .withColumn('circuit_id', col('circuit_id').cast(races_schema['circuit_id'].dataType)) \
    .withColumn('name', col('name').cast(races_schema['name'].dataType)) \
    .withColumn('race_timestamp', col('race_timestamp').cast(races_schema['race_timestamp'].dataType)) \
    .withColumn('ingestion_date', col('ingestion_date').cast(races_schema['ingestion_date'].dataType))


# COMMAND ----------

races_df_final.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

