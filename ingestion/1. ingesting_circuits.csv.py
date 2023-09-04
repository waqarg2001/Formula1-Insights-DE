# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingesting circuits.csv

# COMMAND ----------

# MAGIC %run "../include/configuration"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../include/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_src=dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
vp_file_date=dbutils.widgets.get('p_file_date')


# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

# COMMAND ----------

schema=StructType(fields=[StructField('circuitId',IntegerType(),False),
                          StructField('circuitRef',StringType(),True),
                          StructField('name',StringType(),True),
                          StructField('location',StringType(),True),
                          StructField('country',StringType(),True),
                          StructField('lat',DoubleType(),True),
                          StructField('lng',DoubleType(),True),
                          StructField('alt',IntegerType(),True),
                          StructField('url',StringType(),True)
                          
                          ])

# COMMAND ----------

circuits_df=spark.read.option('header',True).schema(schema).csv(f'{raw_path}/{vp_file_date}/circuits.csv')


# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuit_selected=circuits_df.select('circuitId',"circuitRef","name","location","country","lat","lng",'alt')


# COMMAND ----------

from pyspark.sql.functions import lit
circuit_renamed_df=circuit_selected.withColumnRenamed('circuitId','circuit_id') \
    .withColumnRenamed('circuitRef','circuit_ref') \
    .withColumnRenamed('lat','latitude') \
    .withColumnRenamed('lng','longitude') \
    .withColumnRenamed('alt','altitude') \
    .withColumn('dataSource',lit(v_data_src)) \
    .withColumn('file_date',lit(vp_file_date))
          

          


# COMMAND ----------

circuits_final_df=ingest_date(circuit_renamed_df)


# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

