-- Databricks notebook source
select * from f1_presentation.calculated_race_results

-- COMMAND ----------

select driver_name, sum(calculated_points) as total_points
from f1_presentation.calculated_race_results
group by(driver_name)
order by total_points desc

-- COMMAND ----------

select driver_name, sum(calculated_points) as total_points,count(*) as total_matches
from f1_presentation.calculated_race_results
group by(driver_name)
order by total_points desc

-- COMMAND ----------

select 
  driver_name, 
  sum(calculated_points) as total_points,
  count(*) as total_matches,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by(driver_name)
order by avg_points desc

-- COMMAND ----------

select 
  driver_name, 
  sum(calculated_points) as total_points,
  count(*) as total_matches,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by(driver_name)
having count(1)>=50
order by avg_points desc

-- COMMAND ----------

select 
  driver_name, 
  sum(calculated_points) as total_points,
  count(*) as total_matches,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year>2010 and race_year<=2020
group by(driver_name)
order by avg_points desc

-- COMMAND ----------

select 
  driver_name, 
  sum(calculated_points) as total_points,
  count(*) as total_matches,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2000 and 2010
group by(driver_name)
order by avg_points desc

-- COMMAND ----------

