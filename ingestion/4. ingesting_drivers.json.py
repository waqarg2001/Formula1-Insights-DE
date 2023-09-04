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

from pyspark.sql.types import StructField,StructType,StringType,IntegerType,DateType
name_schema=StructType(fields=[StructField('forename',StringType(),True),
                               StructField('surname',StringType(),True)])
                       
driver_schema=StructType(fields=[StructField('code',StringType(),True),
                               StructField('name',name_schema),
                               StructField('dob',DateType(),True),
                               StructField('driverId',IntegerType(),False),
                               StructField('driverRef',StringType(),True),
                               StructField('nationality',StringType(),True),
                               StructField('number',IntegerType(),True),
                               StructField('url',StringType(),True)
                                           
                                           ])

# COMMAND ----------

drivers_df=(spark.read.schema(driver_schema).json(f'{raw_path}/{vp_file_date}/drivers.json'))


# COMMAND ----------

from pyspark.sql.functions import lit,concat,current_timestamp,col
drivers_df_renamed=drivers_df.withColumnRenamed('driverId','driver_id') \
    .withColumnRenamed('driverRef','driver_ref') \
    .withColumn('name',concat(col('name.forename'),lit(' '),col('name.surname'))) \
    .withColumn('Data Source',lit(v_data_src)) \
    .withColumn('File Date',lit(vp_file_date))

drivers_df_renamed=ingest_date(drivers_df_renamed)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode","name")
drivers_df_renamed.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit('Success')