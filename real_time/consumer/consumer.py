# Import necessary libraries
import findspark

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_date, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = (SparkSession.builder
         .appName("ElasticsearchSparkIntegration")
         .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,"
                                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4")
         .getOrCreate())

cars_topic = 'cars_sales_topic'

cars_schema = StructType([
    StructField("Car Make", StringType(), True),
    StructField("Car Model", StringType(), True),
    StructField("Car Year", IntegerType(), True),
    StructField("Commission Earned", FloatType(), True),
    StructField("Commission Rate", FloatType(), True),
    StructField("Country", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Sale Price", IntegerType(), True),
    StructField("Salesperson", StringType(), True)
])

cars_stream_df = (spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", "localhost:9092")
                   .option("subscribe", cars_topic)
                   .load()
                   .selectExpr("CAST(value AS STRING)")
                   .select(from_json("value", cars_schema).alias("data"))
                   .select("data.*"))

def write_to_elasticsearch_and_console(stream_df, es_index, checkpoint_location):
    # Write to Elasticsearch
    es_query = (stream_df.writeStream
                .format("org.elasticsearch.spark.sql")
                .outputMode("append")
                .option("es.resource", es_index)
                .option("es.nodes", "localhost")
                .option("es.port", "9200")
                .option("es.nodes.wan.only", "true")
                .option("es.index.auto.create", "false")
                .option("checkpointLocation", checkpoint_location)
                .start())

    # Write to Console for testing
    console_query = (stream_df.writeStream
                     .outputMode("append")
                     .format("console")
                     .start())

    return es_query, console_query


# Write data from movies topic to Elasticsearch index and Console
cars_es_query, cars_console_query = write_to_elasticsearch_and_console(cars_stream_df, "cars_sales_analysis_index",
                                                                           "./checkpointLocation/cars/")
# Await termination of the streaming queries
cars_es_query.awaitTermination()