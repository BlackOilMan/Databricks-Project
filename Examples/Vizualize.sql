-- Databricks notebook source
select state_id AS state, count(zip) AS nzips
from uszips_delta_unmanaged
where state_id not in ('AS', 'GU', 'MP', 'PR', 'VI')
group by state
order by nzips

-- COMMAND ----------

select state_id AS state, sum(population) AS population
from uszips_delta_unmanaged
where state_id not in ('AS', 'GU', 'MP', 'PR', 'VI')
group by state
order by state
