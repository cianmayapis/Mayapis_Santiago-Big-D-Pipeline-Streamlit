"""Spark Structured Streaming: Kafka -> MongoDB Atlas."""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "streaming-data")
MONGO_URI = os.environ["MONGO_URI"]  # from environment/.env, never hard-coded
MONGO_DB = os.getenv("MONGO_DB", "weather_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "readings")
CHECKPOINT = os.path.join(
    os.path.expanduser(os.getenv("SPARK_CHECKPOINT_DIR", "~/pipeline-data/spark-checkpoints")),
    "readings",
)

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("value", DoubleType()),
    StructField("metric_type", StringType()),
    StructField("sensor_id", StringType()),
    StructField("location", StringType()),
    StructField("unit", StringType()),
])

spark = (
    SparkSession.builder.appName("kafka-to-mongo")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssX"))
    .dropna(subset=["timestamp", "value", "metric_type", "sensor_id"])  # drop malformed records
    .withColumn("ingested_at", F.current_timestamp())
)

query = (
    parsed.writeStream.format("mongodb")
    .option("checkpointLocation", CHECKPOINT)
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLLECTION)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)
query.awaitTermination()