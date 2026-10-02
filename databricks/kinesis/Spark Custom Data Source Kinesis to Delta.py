# Databricks notebook source
# Spark Structured Streaming with a custom PySpark data source that reads Kinesis with boto3
# works on serverless (Databricks Free Edition) and on every cluster access mode
# requires serverless environment version 2+ or Databricks Runtime 15.4 LTS+
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
checkpoint_path = f"/Volumes/{catalog}/{schema}/checkpoints/kinesis_delta_custom_source"

print(table_name)
print(checkpoint_path)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

# COMMAND ----------

from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader


class KinesisDataSource(DataSource):
    """spark.readStream.format("kinesis_boto3"), every record has: shardId, sequenceNumber, partitionKey, approximateArrivalTimestamp, data"""

    @classmethod
    def name(cls):
        return "kinesis_boto3"

    def schema(self):
        return "shardId STRING, sequenceNumber STRING, partitionKey STRING, approximateArrivalTimestamp TIMESTAMP, data STRING"

    def simpleStreamReader(self, schema):
        return KinesisStreamReader(self.options)


class KinesisStreamReader(SimpleDataSourceStreamReader):
    """runs on the driver (good for small streams), offset = last read sequence number per shard"""

    def __init__(self, options):
        self.options = options

    def initialOffset(self):
        # empty offset: every shard is read from the oldest record (TRIM_HORIZON)
        return {}

    def read(self, start):
        # next micro-batch: all records after start, returns records and the new offset
        return self._read_shards(start)

    def readBetweenOffsets(self, start, end):
        # after a failure Spark reads the same micro-batch again
        records, _ = self._read_shards(start, end)
        return records

    def commit(self, end):
        pass

    def _read_shards(self, start, end=None):
        # libraries must be imported inside methods of a custom data source
        import time
        import boto3

        stream_name = self.options["stream_name"]
        kinesis = boto3.client(
            "kinesis",
            region_name=self.options["region"],
            # empty value -> default AWS credentials chain
            aws_access_key_id=self.options.get("aws_access_key_id") or None,
            aws_secret_access_key=self.options.get("aws_secret_access_key") or None,
            aws_session_token=self.options.get("aws_session_token") or None)

        records = []
        offset = dict(start)
        for shard in kinesis.list_shards(StreamName=stream_name)["Shards"]:
            shard_id = shard["ShardId"]
            if end is not None and end.get(shard_id) == start.get(shard_id):
                continue  # replayed micro-batch has no records from this shard
            if shard_id in start:
                iterator = kinesis.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    StartingSequenceNumber=start[shard_id])["ShardIterator"]
            else:
                iterator = kinesis.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="TRIM_HORIZON")["ShardIterator"]

            # read the shard until the newest record (or until end when replaying)
            while iterator:
                response = kinesis.get_records(ShardIterator=iterator, Limit=10000)
                for record in response["Records"]:
                    if end is not None and int(record["SequenceNumber"]) > int(end[shard_id]):
                        iterator = None
                        break
                    records.append((
                        shard_id,
                        record["SequenceNumber"],
                        record["PartitionKey"],
                        record["ApproximateArrivalTimestamp"],
                        record["Data"].decode("utf-8")))
                    offset[shard_id] = record["SequenceNumber"]
                if iterator is None or response["MillisBehindLatest"] == 0:
                    break
                iterator = response.get("NextShardIterator")
                time.sleep(0.2)  # GetRecords limit: 5 calls per second per shard

        return iter(records), offset


spark.dataSource.register(KinesisDataSource)

# COMMAND ----------

kinesisDataFrame = (spark.readStream
      .format("kinesis_boto3")
      .option("stream_name", kinesis_stream_name)
      .option("region", aws_region)
      .option("aws_access_key_id", aws_access_key_id)
      .option("aws_secret_access_key", aws_secret_access_key)
      .option("aws_session_token", aws_session_token)
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
          from_json(col("data"), personSchema).alias("person"),
          col("shardId"),
          col("sequenceNumber"),
          col("approximateArrivalTimestamp"))
      .select("person.*", "shardId", "sequenceNumber", "approximateArrivalTimestamp")
      .withColumn("event_time", to_timestamp(col("currentdate"), "dd-MM-yyyy HH:mm:ss")))

personDataFrame.printSchema()

# COMMAND ----------

# availableNow: reads everything that is in the stream now, writes it to Delta and stops
# serverless supports only availableNow, to run it all the time use a job with a Continuous trigger
# on classic compute for continuous streaming use: .trigger(processingTime="10 seconds")
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
