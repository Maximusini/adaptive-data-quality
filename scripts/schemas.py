from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType, BooleanType

meta_schema = StructType([
    StructField('is_anomaly', BooleanType()),
    StructField('error_type', StringType()),
    
])

base_schema = StructType([
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