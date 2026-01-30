"""
schemas.py
Определение схем данных PySpark.
"""
from pyspark.sql.types import StructType, StructField, DoubleType, \
                              IntegerType, StringType, TimestampType, BooleanType

# Метаданные от генератора событий
meta_schema = StructType([
    StructField('is_anomaly', BooleanType()),
    StructField('error_type', StringType())
])

# Основная схема входящих событий
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

# Схема для хранения состояния между батчами
state_schema = StructType([
    StructField('group_id', IntegerType()),
    StructField('sensors_data_json', StringType())
])

# Схема выходных событий после инференса
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