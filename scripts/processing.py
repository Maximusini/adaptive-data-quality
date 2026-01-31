"""
processing.py
Первичная обработка и валидация входящих событий из Kafka.
Отделяет технически валидные события от некорректных и помещает их в соответствующие топики Kafka.
"""
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from schemas import base_schema
import settings

def run_processing():
    spark = SparkSession.builder \
        .appName('AdaptiveDataQuality_Processing') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    source = spark.readStream\
        .format('kafka') \
        .option('kafka.bootstrap.servers', settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option('subscribe', settings.TOPIC_RAW) \
        .option('startingOffsets', 'earliest') \
        .load()

    df = source \
        .select(sf.from_json(sf.col('value').cast('string'), base_schema).alias('data')) \
        .select('data.*')
        
    df_validated = df.withColumn('validation_error',
        sf.when(
            sf.col('sensor_id').isNull() |
            sf.col('group_id').isNull() |
            sf.col('timestamp').isNull() |
            sf.col('temperature').isNull() |
            sf.col('humidity').isNull() |
            sf.col('event_id').isNull(),
            'missing_critical_fields'                                 
        )
                                
        .when(
            ~sf.col('event_id').rlike('^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'),
            'invalid_format'
        )
                                
        .when(
            (sf.col('temperature') < -50) | (sf.col('temperature') > 100),
            'temperature_out_of_range'
        )
                                
        .when(
            (sf.col('humidity') < 0) | (sf.col('humidity') > 100),
            'humidity_out_of_range'
        )
                                
        .otherwise(None)
    )

    df_validated = df_validated.withColumn('is_frozen', sf.lit(False))
    df_validated = df_validated.withColumn('is_anomaly_ml', sf.lit(False))
        
    valid_df = df_validated.where('validation_error IS NULL').select(sf.to_json(sf.struct('*')).alias('value'))
    invalid_df = df_validated.where('validation_error IS NOT NULL').select(sf.to_json(sf.struct('*')).alias('value'))

    query_valid = valid_df.writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option('topic', settings.TOPIC_VALID) \
        .option('checkpointLocation', str(settings.CHECKPOINT_DIR / 'processing_valid')) \
        .start()
        
    query_invalid = invalid_df.writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option('topic', settings.TOPIC_QUARANTINE) \
        .option('checkpointLocation', str(settings.CHECKPOINT_DIR / 'processing_invalid')) \
        .start()

    spark.streams.awaitAnyTermination()
    
if __name__ == '__main__':
    run_processing()