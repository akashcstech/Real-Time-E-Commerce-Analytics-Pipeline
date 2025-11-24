# cmd to run 
# spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1   spark/consumer.py




from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for event data
event_schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("product_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", DoubleType())

# Read stream from Kafka topic 'events'
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events,payments") \
    .option("startingOffsets", "latest") \
    .load()

# Extract JSON payload
df_json = df_raw.selectExpr("CAST(value AS STRING) as json")

# Parse JSON into columns
df_parsed = df_json.select(from_json(col("json"), event_schema).alias("data")).select("data.*")

# Basic cleaning (example: filter nulls)
df_clean = df_parsed.filter(col("user_id").isNotNull())

# Write to console (for testing)
query = (
    df_clean.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "spark/checkpoints/events")
    .start()
)
# Write clean data to Parquet (for later analytics)
df_clean.writeStream \
    .format("parquet") \
    .option("path", "spark/output/events") \
    .option("checkpointLocation", "spark/checkpoints/events_parquet") \
    .outputMode("append") \
    .start()

query.awaitTermination()
