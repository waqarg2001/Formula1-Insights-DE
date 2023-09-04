-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### creating circuits.csv table

-- COMMAND ----------

create database if not exists f1_raw;

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
  circuitId int,
  circuitRef string,
  name string,
  location string,
  country string,
  latitude double,
  longitude double,
  altitude int,
  url string
)
using csv
options(path "/mnt/f1racedlwg/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### creating races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
  race_id int,
  race_year int,
  round int,
  circuit_id int,
  name string,
  race_timestamp timestamp,
  ingestion_date timestamp
)
using csv
options(path "/mnt/f1racedlwg/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### creating constructors table

-- COMMAND ----------

drop table if exists f1_raw.constructor;
create table if not exists f1_raw.constructor(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
using json
options(path '/mnt/f1racedlwg/raw/constructors.json')

-- COMMAND ----------

select * from f1_raw.constructor

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### creating drivers table

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists 
f1_raw.drivers(
  code string,
  name struct<forename: string,surname string>,
  dob date,
  driverId int,
  driverRef string,
  nationality string,
  number int,
  url string
)
using json
options(path '/mnt/f1racedlwg/raw/drivers.json')

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### creating results table

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
  constructorId INT, 
  driverId INT,
  fastestLap INT,
  fastestLapSpeed FLOAT,
  grid INT,
  laps INT,
  milliseconds INT,
  number INT, 
  points INT,
  position INT,
  positionOrder INT,
  positionText INT,
  raceId INT,
  rank INT,
  resultId INT,
  statusId INT, 
  time STRING 
)
using json
options(path '/mnt/f1racedlwg/raw/results.json')

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### creating pit_stops table

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
  driverId INT, 
  duration STRING,
  lap INT,
  milliseconds INT, 
  raceId INT,
  stop STRING, 
  time STRING
)
using json
options(path '/mnt/f1racedlwg/raw/pit_stops.json',multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### creating time_lapse table

-- COMMAND ----------

drop table if exists f1_raw.time_lapse;
create table f1_raw.time_lapse(
  race_id INT,
  driver_id INT, 
  lap INT, 
  position INT,
  time STRING,
  milliseconds INT
)
using csv 
options (path '/mnt/f1racedlwg/raw/lap_times')

-- COMMAND ----------

select * from f1_raw.time_lapse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### creating qualifying table

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT, 
  position INT,
  q1 STRING,
  q2 STRING, 
  q3 STRING
)
using json
options(path '/mnt/f1racedlwg/raw/qualifying' , multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

