from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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
    .select(from_json(col('value').cast('string'), schema).alias('data')) \
    .select('data.*')
    
console = df.writeStream \
    .outputMode('append') \
    .format('console') \
    .option('truncate', False) \
    .start()
    
console.awaitTermination()