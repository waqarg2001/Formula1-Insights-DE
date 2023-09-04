# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

# MAGIC %run "../include/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
vp_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

races_df=spark.read.format('delta').load(f'{presentation_path}/result') \
                   .filter(f"`File_Date` = '{vp_file_date}'") 

# COMMAND ----------

from pyspark.sql.functions import count,when,col,sum
driver_df=races_df.groupBy('race_year','Driver','nationality') \
.agg(sum("points").alias('Total Points'),count(when(col("position")==1,True)).alias('Position'))

display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import desc,rank
from pyspark.sql.window import Window
race_rank_spec=Window.partitionBy('race_year').orderBy(desc('Total Points'))
driver_standings=driver_df.withColumn('Rank',rank().over(race_rank_spec))


# COMMAND ----------

#driver_standings.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_standings')


condition="tgt.race_year=src.race_year and tgt.Driver=src.Driver"
merge_delta_data(driver_standings,'f1_presentation','driver_standings','/mnt/f1racedlwg/presentation',condition,'race_year')


# COMMAND ----------

