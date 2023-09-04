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

constructors_schema='constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df=spark.read.schema(constructors_schema).json(f'{raw_path}/{vp_file_date}/constructors.json')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
constructors_renamed=constructors_df.withColumnRenamed('constructorId','constructor_id') \
    .withColumnRenamed('constructorRef','constructor_ref') \
    .withColumn('Data Source',lit(v_data_src)) \
    .withColumn('File Date',lit(vp_file_date)) \
    .drop('url')

constructors_renamed=ingest_date(constructors_renamed)



# COMMAND ----------

display(constructors_renamed)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode","name")
constructors_renamed.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

