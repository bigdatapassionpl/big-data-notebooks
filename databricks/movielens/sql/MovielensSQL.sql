-- Databricks notebook source
SHOW CATALOGS;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG politechnika;

-- COMMAND ----------

show databases;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

use database movielens;

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe extended movies;

-- COMMAND ----------

select * from movies limit 10;