-- This runs automatically when the Postgres container starts for the first time
-- WHY two databases in one Postgres?
-- 'airflow' db = Airflow's internal tracking (created by POSTGRES_DB env var)
-- 'retail_dw'  = our data warehouse substitute for Redshift

CREATE DATABASE retail_dw;