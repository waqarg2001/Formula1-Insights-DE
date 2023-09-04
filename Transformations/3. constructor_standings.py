# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f'{presentation_path}/result')
display(races_df)

# COMMAND ----------

from pyspark.sql.functions import sum,col,when,count,desc,rank
constructor_standings=races_df.groupBy('Team','race_year').agg(sum(col('points')).alias('Total Points'),count(when(col('position')==1,True)).alias('Wins'))
display(constructor_standings)

# COMMAND ----------

from pyspark.sql.window import Window
win_spec=Window.partitionBy('race_year').orderBy(desc(col('Total Points')),desc('Wins'))
cs=constructor_standings.withColumn('Rank',rank().over(win_spec))
constructor_standings=cs.filter("race_year=2020")

# COMMAND ----------

constructor_standings.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')

# COMMAND ----------

