# Connect spark to snowflake

# make sure these jars exist:
# net.snowflake_snowflake-jdbc-3.24.2.jar                    org.apache.spark_spark-sql-kafka-0-10_2.13-4.0.1.jar
# net.snowflake_spark-snowflake_2.13-3.1.5.jar               org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.0.jar

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,net.snowflake:spark-snowflake_2.13:3.1.5 spark/consumer_to_snowflake.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
from dotenv import load_dotenv
import os

spark = SparkSession.builder.appName("KafkaToSnowflake").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
    .add("event_id", StringType())
    .add("user_id", StringType())
    .add("product_id", StringType())
    .add("event_type", StringType())
    .add("timestamp", DoubleType())
)

df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "events,payments")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

load_dotenv()
# Snowflake options
sf_options = {
    "sfURL": os.getenv("SF_URL"),
    "sfDatabase": os.getenv("SF_DATABASE"),
    "sfSchema": os.getenv("SF_SCHEMA"),
    "sfWarehouse": os.getenv("SF_WAREHOUSE"),
    "sfUser": os.getenv("SF_USER"),
    "sfPassword": os.getenv("SF_PASSWORD"),
}

# Snowflake environment variables exist sanity check:
for k, v in sf_options.items():
    if v is None:
        raise ValueError(f"Missing Snowflake option: {k}")
    

# Write to Snowflake in micro-batches
def write_to_snowflake(batch_df, batch_id):
    batch_df.write.format("snowflake") \
        .option("autocreate", "on") \
        .options(**sf_options) \
        .option("dbtable", "EVENTS_RAW") \
        .mode("append") \
        .save()

query = (
    df_parsed.writeStream
    .foreachBatch(write_to_snowflake)
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_snowflake")
    .outputMode("append")
    .start()
)

# for one time batch job process [note: comment_out : streaming job] & [set:.option("startingOffsets", "earliest") ]
# query.processAllAvailable()
# query.stop()
# spark.stop()

# for streaming job [note: comment_out : batch job] & [set:.option("startingOffsets", "latest") ]
query.awaitTermination() 

