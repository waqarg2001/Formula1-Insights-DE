# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def ingest_date(df):
    ingested_date=df.withColumn('ingestion_date',current_timestamp())
    return ingested_date

# COMMAND ----------

def incremental(df,table_name):
    spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
    if (spark._jsparkSession.catalog().tableExists(f"f1_processed.{table_name}")):
        df.write.mode("overwrite").insertInto(f"f1_processed.{table_name}") 
    else: 
        df.write.mode ("overwrite").partitionBy ("race_id").format("parquet").saveAsTable(f"f1_processed.{table_name}")

# COMMAND ----------

def incremental_1(df,db_name,table_name,part_id):
    spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}") 
    else: 
        df.write.mode ("overwrite").partitionBy(f"{part_id}").format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_delta_data(df,db,table,path,condition,partition_key):
    spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode","name")
    spark.conf.set('spark.databricks.optimizer.dynamicPartitionPruning','true')
    from delta.tables import DeltaTable 

    if (spark._jsparkSession.catalog().tableExists(f"{db}.{table}")):
        deltaTable=DeltaTable.forPath(spark,f'{path}/{table}')
        deltaTable.alias('tgt').merge(
            df.alias('src'),
            condition) \
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
        
    else: 
        df.write.mode ("overwrite").partitionBy (f"{partition_key}").format("delta").saveAsTable(f"{db}.{table}")

# COMMAND ----------

