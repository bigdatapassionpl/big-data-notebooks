# Databricks notebook source
display(dbutils.fs.ls("/"))

# COMMAND ----------


directoryPath = "/Volumes/politechnika/default/"

moviesPath = f"{directoryPath}/movies/movies.dat"
ratingsPath = f"{directoryPath}/ratings/ratings.dat"
tagsPath = f"{directoryPath}/tags/tags.dat"

movielensSeparator = "@"

# COMMAND ----------

display(dbutils.fs.ls(f"{moviesPath}"))

# COMMAND ----------

# reading from DBFS to DataFrames
moviesDataFrame = (spark.read
      .option("header", "false")
      .option("charset", "UTF8")
      .option("delimiter", movielensSeparator)
      .option("inferSchema", "true")
      .csv(moviesPath)
      .withColumnRenamed("_c0", "movieId")
      .withColumnRenamed("_c1", "title")
      .withColumnRenamed("_c2", "genres"))

ratingsDataFrame = (spark.read
      .option("header", "false")
      .option("charset", "UTF8")
      .option("delimiter", movielensSeparator)
      .option("inferSchema", "true")
      .csv(ratingsPath)
      .withColumnRenamed("_c0", "userId")
      .withColumnRenamed("_c1", "movieId")
      .withColumnRenamed("_c2", "rating")
      .withColumnRenamed("_c3", "timestamp"))

tagsDataFrame = (spark.read
      .option("header", "false")
      .option("charset", "UTF8")
      .option("delimiter", movielensSeparator)
      .option("inferSchema", "true")
      .csv(tagsPath)
      .withColumnRenamed("_c0", "userId")
      .withColumnRenamed("_c1", "movieId")
      .withColumnRenamed("_c2", "tag")
      .withColumnRenamed("_c3", "timestamp"))

# COMMAND ----------

display(moviesDataFrame)

# COMMAND ----------

moviesDataFrame.show(10)
moviesDataFrame.printSchema()

# COMMAND ----------

ratingsDataFrame.show(10)
ratingsDataFrame.printSchema()

# COMMAND ----------

tagsDataFrame.show(10)
tagsDataFrame.printSchema()

# COMMAND ----------

moviesDataFrame.createOrReplaceTempView("movies")
ratingsDataFrame.createOrReplaceTempView("ratings")
ratingsDataFrame.createOrReplaceTempView("tags")

# COMMAND ----------

spark.sql("USE CATALOG politechnika")

# COMMAND ----------

spark.sql("create database if not exists movielens")
spark.sql("drop table if exists movielens.movies")
spark.sql("drop table if exists movielens.ratings")
spark.sql("drop table if exists movielens.tags")
spark.sql("create table movielens.movies as select * from movies")
spark.sql("create table movielens.ratings as select * from ratings")
spark.sql("create table movielens.tags as select * from tags")

# COMMAND ----------

from pyspark.sql.functions import count, avg, col

moviesAvg = (moviesDataFrame.alias("m")
    .join(ratingsDataFrame.alias("r"), col("m.movieId") == col("r.movieId"), "left")
    .groupBy(col("m.title"))
    .agg(
        count("r.rating").alias("votes"),
        avg("r.rating").alias("rate")
    )
    .filter(col("votes") > 100)
    .orderBy(col("rate").desc())
)

# COMMAND ----------

moviesAvg.show

# COMMAND ----------

display(moviesAvg)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT genre, count(1) as count 
# MAGIC from (
# MAGIC   select explode(split(genres, '\\|')) as genre
# MAGIC   from movies
# MAGIC )
# MAGIC group by genre
# MAGIC order by count desc