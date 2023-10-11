// Databricks notebook source
dbutils.fs.ls("/")

// COMMAND ----------

display(dbutils.fs.ls("/"))

// COMMAND ----------

// List files on Databricks store (Azure blob)
display(dbutils.fs.ls("/FileStore/"))

// COMMAND ----------

// Directories
// val directoryPath: String = "/FileStore/tables/movielens/hive"
val directoryPath: String = "/bigdatapassion/movielens/hive"
val moviesPath: String = directoryPath + "/movies/movies.dat"
val ratingsPath: String = directoryPath + "/ratings/ratings.dat"
val tagsPath: String = directoryPath + "/tags/tags.dat"
val movielensSeparator: String = "@"

// COMMAND ----------

display(dbutils.fs.ls(s"$directoryPath/"))

// COMMAND ----------

display(dbutils.fs.ls(s"$directoryPath/movies/"))

// COMMAND ----------

// reading from DBFS to DataFrames
val moviesDataFrame = spark.read.
      option("header", "false").
      option("charset", "UTF8").
      option("delimiter", movielensSeparator).
      option("inferSchema", "true").
      csv(moviesPath).
      withColumnRenamed("_c0", "movieId").
      withColumnRenamed("_c1", "title").
      withColumnRenamed("_c2", "genres")

val ratingsDataFrame = spark.read.
      option("header", "false").
      option("charset", "UTF8").
      option("delimiter", movielensSeparator).
      option("inferSchema", "true").
      csv(ratingsPath).
      withColumnRenamed("_c0", "userId").
      withColumnRenamed("_c1", "movieId").
      withColumnRenamed("_c2", "rating").
      withColumnRenamed("_c3", "timestamp")

val tagsDataFrame = spark.read.
      option("header", "false").
      option("charset", "UTF8").
      option("delimiter", movielensSeparator).
      option("inferSchema", "true").
      csv(tagsPath).
      withColumnRenamed("_c0", "userId").
      withColumnRenamed("_c1", "movieId").
      withColumnRenamed("_c2", "tag").
      withColumnRenamed("_c3", "timestamp")

// COMMAND ----------

display(moviesDataFrame)

// COMMAND ----------

    moviesDataFrame.show(10)
    moviesDataFrame.printSchema()

// COMMAND ----------

    ratingsDataFrame.show(10)
    ratingsDataFrame.printSchema()

// COMMAND ----------

    tagsDataFrame.show(10)
    tagsDataFrame.printSchema()

// COMMAND ----------

moviesDataFrame.createOrReplaceTempView("movies")
ratingsDataFrame.createOrReplaceTempView("ratings")
ratingsDataFrame.createOrReplaceTempView("tags")

// COMMAND ----------

// Registering temp table as real Table in Spark/Hive metastore :)
// spark.sql("drop database if exists movielens cascade");

// COMMAND ----------

// Registering temp table as real Table in Spark/Hive metastore :)
spark.sql("create database if not exists movielens");
spark.sql("drop table if exists movielens.movies");
spark.sql("drop table if exists movielens.ratings");
spark.sql("drop table if exists movielens.tags");
spark.sql("create table movielens.movies as select * from movies");
spark.sql("create table movielens.ratings as select * from ratings");
spark.sql("create table movielens.tags as select * from tags");

// COMMAND ----------

val moviesAvg = spark.sql(
      """
      SELECT m.title, count(*) as votes, avg(rating) rate
      FROM movies m
      left join ratings r on m.movieid = r.movieid
      group by m.title
      having votes > 100
      order by rate desc
      """)

// COMMAND ----------

moviesAvg.show

// COMMAND ----------

display(moviesAvg)

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC SELECT genre, count(1) as count 
// MAGIC from (
// MAGIC   select explode(split(genres, '\\|')) as genre
// MAGIC   from movies
// MAGIC )
// MAGIC group by genre
// MAGIC order by count desc
