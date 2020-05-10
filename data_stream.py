import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# TODO Create a schema for incoming resources
schema = StructType([StructField("crime_id", StringType(), True),
                     StructField("original_crime_type_name", StringType(), True),
                     StructField("report_date", TimestampType(), True),
                     StructField("call_date", TimestampType(), True),
                     StructField("offense_date", TimestampType(), True),
                     StructField("call_time", StringType(), True),
                     StructField("call_date_time", TimestampType(), True),
                     StructField("disposition", StringType(), True),
                     StructField("address", StringType(), True),
                     StructField("city", StringType(), True),
                     StructField("state", StringType(), True),
                     StructField("agency_id", StringType(), True),
                     StructField("address_type", StringType(), True),
                     StructField("common_location", StringType(), True)
                     ])


def run_spark_job(spark):
    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sf.police.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetPerTrigger", 200) \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    service_table.printSchema()
   
    distinct_table = service_table.select("call_date_time", "original_crime_type_name", "disposition")  
    
    distinct_table.printSchema()

    agg_df = distinct_table \
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(
            psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),
            psf.col("original_crime_type_name")
        ).count()

    query = agg_df \
        .writeStream \
        .queryName('count') \
        .format("console") \
        .outputMode("complete") \
        .start()

    query.awaitTermination()

    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = distinct_table \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .queryName("join") \
        .format("console") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config('spark.ui.port', 3000) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")
    logger.info("Set Spark Log Level to ERROR")
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    run_spark_job(spark)

    spark.stop()