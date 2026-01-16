from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType, BooleanType

spark = SparkSession\
    .builder\
    .appName('AdaptiveDataQuality') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

meta_schema = StructType([
    StructField('is_anomaly', BooleanType()),
    StructField('error_type', StringType()),
    
])

schema = StructType([
    StructField('event_id', StringType()),
    StructField('timestamp', TimestampType()),
    StructField('sensor_id', IntegerType()),
    StructField('group_id', IntegerType()),
    StructField('temperature', DoubleType()),
    StructField('humidity', DoubleType()),
    StructField('battery', DoubleType()),
    StructField('firmware', StringType()),
    StructField('meta_info', meta_schema)
])

source = spark.readStream\
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('subscribe', 'raw_events') \
    .option('startingOffsets', 'earliest') \
    .load()

df = source \
    .select(sf.from_json(sf.col('value').cast('string'), schema).alias('data')) \
    .select('data.*')
    
df_validated = df.withColumn('validation_error',
                             sf.when(
                                 sf.col('sensor_id').isNull() |
                                 sf.col('group_id').isNull() |
                                 sf.col('timestamp').isNull() |
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
    
valid_df = df_validated.where('validation_error IS NULL').select(sf.to_json(sf.struct('*')).alias('value'))
invalid_df = df_validated.where('validation_error IS NOT NULL').select(sf.to_json(sf.struct('*')).alias('value'))

query_valid = valid_df.writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('topic', 'technically_valid_events') \
    .option('checkpointLocation', '/tmp/checkpoints/valid') \
    .start()
    
query_invalid = invalid_df.writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('topic', 'quarantined_events') \
    .option('checkpointLocation', '/tmp/checkpoints/invalid') \
    .start()

spark.streams.awaitAnyTermination()