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
    StructField('temp_std', DoubleType()),
    StructField('temp_diff', DoubleType()),
    StructField('temp_dev', DoubleType()),
    StructField('hum_std', DoubleType()),
    StructField('hum_diff', DoubleType()),
    StructField('hum_dev', DoubleType()),
    StructField('temp_z', DoubleType()),
    StructField('hour_sin', DoubleType()),
    StructField('hour_cos', DoubleType()),
    StructField('is_frozen', BooleanType()),
    StructField('is_anomaly_ml', BooleanType())
])