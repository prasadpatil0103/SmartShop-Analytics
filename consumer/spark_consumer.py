import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# ─── Setup Spark Session ──────────────────────────────
spark = SparkSession.builder \
    .appName("SmartShop Streaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
	    "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark Session started!")

# ─── Define Schema of our Kafka messages ─────────────
schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", StringType()),
    StructField("payment_type", StringType()),
    StructField("payment_value", FloatType()),
    StructField("product_id", StringType()),
    StructField("price", FloatType()),
    StructField("customer_city", StringType()),
    StructField("customer_state", StringType()),
])

# ─── Read from Kafka ──────────────────────────────────
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

print("✅ Connected to Kafka topic: orders")

# ─── Parse JSON messages ──────────────────────────────
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ─── Transform & Enrich data ──────────────────────────
df_transformed = df_parsed \
    .withColumn("order_size",
        when(col("payment_value") < 50, "small")
        .when(col("payment_value") < 200, "medium")
        .otherwise("large")
    ) \
    .withColumn("is_high_value",
        when(col("payment_value") > 500, True).otherwise(False)
    )

# ─── Output Path ──────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "delta_lake", "orders")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "delta_lake", "checkpoints", "orders")

# ─── Write to Delta Lake ──────────────────────────────
query = df_transformed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start(OUTPUT_PATH)

print("✅ Streaming to Delta Lake...")
print(f"📁 Saving to: {OUTPUT_PATH}")
print("👀 Watching for new orders...\n")

query.awaitTermination()