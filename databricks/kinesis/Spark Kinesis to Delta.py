# Databricks notebook source
# requires cluster with access mode: Dedicated (Single user), standard / shared clusters do not support awsSessionToken for Kinesis
# for serverless (Databricks Free Edition) use: Spark & Boto3 Kinesis to Delta
# AWS Academy -> AWS Details -> AWS CLI (temporary credentials)
aws_access_key_id = ""
aws_secret_access_key = ""
aws_session_token = ""

# COMMAND ----------

aws_region = "us-east-1"
kinesis_stream_name = "kinesis-data-stream-example"

catalog = "politechnika"
schema = "kinesis"
table_name = f"{catalog}.{schema}.kinesis_delta"
checkpoint_path = f"/Volumes/{catalog}/{schema}/checkpoints/kinesis_delta"

print(table_name)
print(checkpoint_path)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

# COMMAND ----------

# reading from Kinesis, every record has: partitionKey, data (binary), stream, shardId, sequenceNumber, approximateArrivalTimestamp
kinesisDataFrame = (spark.readStream
      .format("kinesis")
      .option("streamName", kinesis_stream_name)
      .option("region", aws_region)
      .option("initialPosition", "trim_horizon")
      .option("awsAccessKey", aws_access_key_id)
      .option("awsSecretKey", aws_secret_access_key)
      .option("awsSessionToken", aws_session_token)
      .load())

# COMMAND ----------

from pyspark.sql.functions import col, from_json, to_timestamp

# JSON sent by KinesisProducerExample
personSchema = """
  partitionkey STRING,
  currentdate  STRING,
  name         STRING,
  phonenumber  STRING,
  streetname   STRING,
  number       STRING,
  city         STRING,
  country      STRING,
  animal       STRING
"""

personDataFrame = (kinesisDataFrame
      .select(
          from_json(col("data").cast("string"), personSchema).alias("person"),
          col("shardId"),
          col("sequenceNumber"),
          col("approximateArrivalTimestamp"))
      .select("person.*", "shardId", "sequenceNumber", "approximateArrivalTimestamp")
      .withColumn("event_time", to_timestamp(col("currentdate"), "dd-MM-yyyy HH:mm:ss")))

personDataFrame.printSchema()

# COMMAND ----------

# availableNow: reads everything that is in the stream now, writes it to Delta and stops
# for continuous streaming use: .trigger(processingTime="10 seconds")
query = (personDataFrame.writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint_path)
      .trigger(availableNow=True)
      .toTable(table_name))

query.awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {table_name} ORDER BY event_time DESC LIMIT 10"))

# COMMAND ----------

display(spark.sql(f"""
SELECT animal, COUNT(*) AS cnt
FROM {table_name}
GROUP BY animal
ORDER BY cnt DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT COUNT(animal) AS all_animals, COUNT(DISTINCT animal) AS unique_animals
FROM {table_name}
"""))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {table_name}"))

# COMMAND ----------

# start from scratch: uncomment and run
# spark.sql(f"DROP TABLE IF EXISTS {table_name}")
# dbutils.fs.rm(checkpoint_path, True)
