"""
archivier.py
Архивация технически валидных событий в хранилище данных для последующего использования в обучении моделей.
"""
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from schemas import base_schema
import settings

def run_archiver():
    spark = SparkSession.builder \
        .appName('AdaptiveDataQuality_Archiver') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    source = spark.readStream\
        .format('kafka') \
        .option('kafka.bootstrap.servers', settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option('subscribe', settings.TOPIC_VALID) \
        .option('startingOffsets', 'earliest') \
        .load()
        
    df = source \
        .select(sf.from_json(sf.col('value').cast('string'), base_schema).alias('data')) \
        .select('data.*')
        
    sink = df.writeStream \
        .format('parquet') \
        .option('path', str(settings.BASE_DIR / 'training_dataset')) \
        .option('checkpointLocation', str(settings.CHECKPOINT_DIR / 'archiver')) \
        .outputMode('append') \
        .trigger(processingTime='1 minute') \
        .start()

    spark.streams.awaitAnyTermination()
    
if __name__ == '__main__':
    run_archiver()