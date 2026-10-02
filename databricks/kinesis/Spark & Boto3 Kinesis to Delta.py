# Databricks notebook source
# for serverless compute (Databricks Free Edition): Spark "kinesis" source does not support awsSessionToken there,
# so records are pulled from Kinesis with boto3 and saved to Delta with Spark
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

print(table_name)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

import boto3
from botocore.config import Config

kinesis = boto3.client(
    "kinesis",
    region_name=aws_region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 2}))

# connection test: Free Edition allows outbound traffic only to trusted domains, this fails when AWS is not reachable
print(kinesis.describe_stream_summary(StreamName=kinesis_stream_name)["StreamDescriptionSummary"]["StreamStatus"])

# COMMAND ----------

# last saved sequence number per shard, so every run reads only new records
# sequence numbers are long numeric strings: LPAD makes string MAX() work like numeric MAX()
last_sequence_numbers = {}
if spark.catalog.tableExists(table_name):
    rows = spark.sql(f"""
        SELECT shardId, MAX(LPAD(sequenceNumber, 128, '0')) AS sequenceNumber
        FROM {table_name}
        GROUP BY shardId
    """).collect()
    last_sequence_numbers = {row.shardId: row.sequenceNumber.lstrip("0") for row in rows}

print(last_sequence_numbers)

# COMMAND ----------

import time

records = []
for shard in kinesis.list_shards(StreamName=kinesis_stream_name)["Shards"]:
    shard_id = shard["ShardId"]
    if shard_id in last_sequence_numbers:
        iterator = kinesis.get_shard_iterator(
            StreamName=kinesis_stream_name,
            ShardId=shard_id,
            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
            StartingSequenceNumber=last_sequence_numbers[shard_id])["ShardIterator"]
    else:
        iterator = kinesis.get_shard_iterator(
            StreamName=kinesis_stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON")["ShardIterator"]

    # read the shard until we reach the newest record
    while iterator:
        response = kinesis.get_records(ShardIterator=iterator, Limit=10000)
        for record in response["Records"]:
            records.append((
                shard_id,
                record["SequenceNumber"],
                record["PartitionKey"],
                record["ApproximateArrivalTimestamp"],
                record["Data"].decode("utf-8")))
        if response["MillisBehindLatest"] == 0:
            break
        iterator = response.get("NextShardIterator")
        time.sleep(0.2)  # GetRecords limit: 5 calls per second per shard

print(f"fetched records: {len(records)}")

# COMMAND ----------

from pyspark.sql.functions import col, from_json, to_timestamp

kinesisDataFrame = spark.createDataFrame(
    records,
    "shardId STRING, sequenceNumber STRING, partitionKey STRING, approximateArrivalTimestamp TIMESTAMP, data STRING")

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
          from_json(col("data"), personSchema).alias("person"),
          col("shardId"),
          col("sequenceNumber"),
          col("approximateArrivalTimestamp"))
      .select("person.*", "shardId", "sequenceNumber", "approximateArrivalTimestamp")
      .withColumn("event_time", to_timestamp(col("currentdate"), "dd-MM-yyyy HH:mm:ss")))

personDataFrame.printSchema()

# COMMAND ----------

(personDataFrame.write
      .format("delta")
      .mode("append")
      .saveAsTable(table_name))

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
