# Databricks notebook source

aws_bucket_name="radek-datasets-public"

# COMMAND ----------

display(dbutils.fs.ls(f"s3a://{aws_bucket_name}/movielens/demo/movies/"))

# COMMAND ----------

moviesDataFrame = spark.read.\
  option("header", "false").\
  option("charset", "UTF8").\
  option("delimiter", "@").\
  option("inferSchema", "true").\
  csv(f"s3a://{aws_bucket_name}/movielens/demo/movies/movies.dat").\
  withColumnRenamed("_c0", "movieId").\
  withColumnRenamed("_c1", "title").\
  withColumnRenamed("_c2", "genres")

# COMMAND ----------

display(moviesDataFrame)
