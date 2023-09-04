-- Databricks notebook source
-- MAGIC %python
-- MAGIC html='<h1 style="color:black; font-family: arial;text-align:center">Dominant F1 Drivers Dashboard</h1>'
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view race_result_view
as
select 
  driver_name, 
  sum(calculated_points) as total_points,
  count(*) as total_matches,
  avg(calculated_points) as avg_points,
  rank() over(order by avg(calculated_points) desc) as driver_rank
from f1_presentation.calculated_race_results
group by(driver_name)
having count(1)>=50
order by avg_points desc

-- COMMAND ----------

select 
  race_year,
  driver_name, 
  sum(calculated_points) as total_points,
  count(1) as total_races,
  avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in ( select driver_name from race_result_view where driver_rank <=10 )
group by race_year,driver_name
order by race_year ,avg_points desc

-- COMMAND ----------

