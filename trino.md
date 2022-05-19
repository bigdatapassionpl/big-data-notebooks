
~~~sql

show catalogs;

show schemas in hive;

CREATE SCHEMA IF NOT EXISTS hive.movielens
WITH (location = 's3://big-data-cloud-radek2/');

show tables in hive.movielens;

drop table hive.movielens.movies;

CREATE TABLE IF NOT EXISTS hive.movielens.movies (
  movieid VARCHAR,
  title VARCHAR,
  genres VARCHAR 
)
WITH (
  external_location = 's3://big-data-cloud-radek2/movielens/hive/movies/',
  format = 'CSV',
  csv_escape = '\',
  csv_quote = '"',
  csv_separator = '@'
);

select * from hive.movielens.movies limit 10;

USE hive.movielens;
select * from movies limit 10;
~~~