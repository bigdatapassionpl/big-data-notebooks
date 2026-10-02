# Databricks notebook source
access_key = ""
secret = ""
token = ""
sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret)
sc._jsc.hadoopConfiguration().set("fs.s3a.session.token", token)

# COMMAND ----------

aws_bucket_name="radek-datasets-aws-academy"

# COMMAND ----------

display(dbutils.fs.ls(f"s3a://{aws_bucket_name}/movielens/demo/movies/"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/"))

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
