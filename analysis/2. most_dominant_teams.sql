-- Databricks notebook source
select * from f1_presentation.calculated_race_results

-- COMMAND ----------

select team,
       sum(calculated_points) as total_points 
from f1_presentation.calculated_race_results
group by(team)
order by total_points desc

-- COMMAND ----------

select team,
       sum(calculated_points) as total_points,
       count(1) as total_matches 
from f1_presentation.calculated_race_results
group by(team)
order by total_points desc

-- COMMAND ----------

select team,
       sum(calculated_points) as total_points,
       count(1) as total_matches,
       avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by(team)
order by avg_points desc

-- COMMAND ----------

select team,
       sum(calculated_points) as total_points,
       count(1) as total_matches,
       avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by(team)
having count(1)>100
order by avg_points desc

-- COMMAND ----------

select team,
       sum(calculated_points) as total_points,
       count(1) as total_matches,
       avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2010 and 2020
group by(team)
order by avg_points desc

-- COMMAND ----------

select team,
       sum(calculated_points) as total_points,
       count(1) as total_matches,
       avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2000 and 2010
group by(team)
order by avg_points desc

-- COMMAND ----------

