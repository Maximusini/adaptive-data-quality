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

state_schema = StructType([
    StructField('group_id', IntegerType()),
    StructField('sensors_data_json', StringType())
])

output_schema = StructType([
    StructField('event_id', StringType()),
    StructField('timestamp', TimestampType()),
    StructField('sensor_id', IntegerType()),
    StructField('group_id', IntegerType()),
    StructField('temperature', DoubleType()),
    StructField('humidity', DoubleType()),
    StructField('validation_error', StringType()),
    StructField('is_frozen', BooleanType()),
    StructField('is_anomaly_ml', BooleanType())
])

FEATURE_COLUMNS = [
    'temperature', 
    'temp_diff', 
    'temp_std',
    'humidity', 
    'hum_diff', 
    'hum_std',
    'hour_sin', 
    'hour_cos',
    'temp_dev', 
    'hum_dev', 
    'temp_z'
]