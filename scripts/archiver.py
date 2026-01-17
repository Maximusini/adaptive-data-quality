import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from schemas import base_schema

spark = SparkSession\
    .builder\
    .appName('AdaptiveDataQuality') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()

source = spark.readStream\
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('subscribe', 'technically_valid_events') \
    .option('startingOffsets', 'earliest') \
    .load()
    
df = source \
    .select(sf.from_json(sf.col('value').cast('string'), base_schema).alias('data')) \
    .select('data.*')
    
sink = df.writeStream \
    .format('parquet') \
    .option('path', '/opt/spark/data/training_dataset') \
    .option('checkpointLocation', '/tmp/checkpoints/archiver') \
    .outputMode('append') \
    .trigger(processingTime='1 minute') \
    .start()

spark.streams.awaitAnyTermination()