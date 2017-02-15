#!/usr/bin/env python3
"""
Real-Time Streaming ETL Pipeline
Processes order events from Kafka and writes to Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    builder = SparkSession.builder \
        .appName("StreamingETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.checkpointLocation", "/opt/spark-data/checkpoints") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def define_schemas():
    """Define schemas for incoming data"""
    order_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("region", StringType(), True)
    ])
    
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("registration_date", TimestampType(), True)
    ])
    
    return order_schema, customer_schema

def read_kafka_stream(spark, topic, schema):
    """Read streaming data from Kafka"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load() \
        .select(
            col("key").cast("string").alias("key"),
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("key", "data.*", "kafka_timestamp")

def enrich_order_data(orders_df, customers_df):
    """Enrich order data with customer information"""
    return orders_df.join(
        customers_df,
        orders_df.customer_id == customers_df.customer_id,
        "left"
    ).select(
        orders_df["*"],
        customers_df.customer_name,
        customers_df.email,
        customers_df.segment
    ).withColumn(
        "total_amount", 
        col("quantity") * col("unit_price")
    ).withColumn(
        "processing_time",
        current_timestamp()
    )

def apply_business_rules(df):
    """Apply business validation rules"""
    return df.withColumn(
        "is_valid",
        when(
            (col("quantity") > 0) & 
            (col("unit_price") > 0) & 
            (col("total_amount") <= 10000), 
            True
        ).otherwise(False)
    ).withColumn(
        "risk_level",
        when(col("total_amount") > 5000, "HIGH")
        .when(col("total_amount") > 1000, "MEDIUM")
        .otherwise("LOW")
    )

def write_to_delta_lake(df, path, checkpoint_path):
    """Write streaming data to Delta Lake"""
    return df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", path) \
        .trigger(processingTime='30 seconds') \
        .start()

def write_aggregations(df, path, checkpoint_path):
    """Write real-time aggregations"""
    aggregated = df \
        .withWatermark("order_date", "10 minutes") \
        .groupBy(
            window(col("order_date"), "5 minutes"),
            col("region"),
            col("segment")
        ).agg(
            count("*").alias("order_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("region"),
            col("segment"),
            col("order_count"),
            col("total_revenue"),
            col("avg_order_value"),
            col("unique_customers"),
            current_timestamp().alias("calculated_at")
        )
    
    return aggregated.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("path", path) \
        .trigger(processingTime='1 minute') \
        .start()

def main():
    """Main streaming ETL pipeline"""
    logger.info("Starting Streaming ETL Pipeline...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Define schemas
    order_schema, customer_schema = define_schemas()
    
    # Read streaming data
    orders_stream = read_kafka_stream(spark, "orders", order_schema)
    customers_stream = read_kafka_stream(spark, "customers", customer_schema)
    
    # Convert customers stream to table for joins
    customers_stream.writeStream \
        .format("memory") \
        .queryName("customers_table") \
        .outputMode("append") \
        .start()
    
    # Create temporary view for customers
    customers_df = spark.table("customers_table")
    
    # Enrich and transform order data
    enriched_orders = enrich_order_data(orders_stream, customers_df)
    validated_orders = apply_business_rules(enriched_orders)
    
    # Write to Delta Lake
    raw_orders_query = write_to_delta_lake(
        validated_orders,
        "/opt/spark-data/delta/raw_orders",
        "/opt/spark-data/checkpoints/raw_orders"
    )
    
    # Write aggregations
    aggregations_query = write_aggregations(
        validated_orders.filter(col("is_valid") == True),
        "/opt/spark-data/delta/order_aggregations",
        "/opt/spark-data/checkpoints/aggregations"
    )
    
    # Write invalid orders for investigation
    invalid_orders_query = write_to_delta_lake(
        validated_orders.filter(col("is_valid") == False),
        "/opt/spark-data/delta/invalid_orders",
        "/opt/spark-data/checkpoints/invalid_orders"
    )
    
    logger.info("Streaming queries started successfully")
    
    # Wait for termination
    try:
        raw_orders_query.awaitTermination()
        aggregations_query.awaitTermination()
        invalid_orders_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping streaming queries...")
        raw_orders_query.stop()
        aggregations_query.stop()
        invalid_orders_query.stop()
        spark.stop()

if __name__ == "__main__":
    main()