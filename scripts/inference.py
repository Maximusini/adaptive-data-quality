import pandas as pd
import numpy as np
import joblib
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from schemas import base_schema, state_schema, output_schema

MODEL_PATH = '/opt/spark/data/model.joblib'
WINDOW_SIZE = 30

model = None

def get_model(path):
    global model
    if model is None:
        model = joblib.load(path)
    return model

def process_batch(key, pandasdf_iter, state):
    
    if state.exists:
        current_state_row = state.get
        room_history = json.loads(current_state_row[1])
    
    else:
        room_history = {}
        
    model = get_model(MODEL_PATH)
    
    results = []
    
    for df in pandasdf_iter:
        for index, row in df.iterrows():
            sensor_id = str(row['sensor_id'])
            group_id = int(row['group_id'])
            curr_temp = float(row['temperature'])
            curr_hum = float(row['humidity'])
            
            if sensor_id not in room_history:
                room_history[sensor_id] = {'temps': [], 'hums': []}
            
            room_history[sensor_id]['temps'].append(curr_temp)
            room_history[sensor_id]['hums'].append(curr_hum)
                
            if len(room_history[sensor_id]['temps']) > WINDOW_SIZE:
                room_history[sensor_id]['temps'] = room_history[sensor_id]['temps'][-WINDOW_SIZE:]
                room_history[sensor_id]['hums'] = room_history[sensor_id]['hums'][-WINDOW_SIZE:]
                
            temps_arr = np.array(room_history[sensor_id]['temps'])
            hums_arr = np.array(room_history[sensor_id]['hums'])
        
            if len(temps_arr) > 1:
                temp_std = float(np.std(temps_arr, ddof=1))
                temp_diff = float(abs(temps_arr[-1] - temps_arr[-2]))
                
                hum_std = float(np.std(hums_arr, ddof=1))
                hum_diff = float(abs(hums_arr[-1] - hums_arr[-2]))
            
            else:
                temp_std = 0.0
                temp_diff = 0.0
                hum_std = 0.0
                hum_diff = 0.0
            
            room_temps = []
            room_hums = []
            for sensor in room_history.values():
                if sensor['temps']:
                    room_temps.append(sensor['temps'][-1])
                if sensor['hums']:
                    room_hums.append(sensor['hums'][-1])
            
            group_temp_mean = np.mean(room_temps)
            
            if len(room_temps) > 1:
                group_temp_std = np.std(room_temps, ddof=1)
            else:
                group_temp_std = 0.0
            
            if group_temp_std == 0 or np.isnan(group_temp_std):
                group_temp_std = 0.1
            
            if len(room_hums) > 0:
                group_hum_mean = np.mean(room_hums)
            else:
                group_hum_mean = 0.0
            
            temp_dev = abs(curr_temp - group_temp_mean)
            hum_dev = abs(curr_hum - group_hum_mean)
            temp_z = abs(temp_dev / group_temp_std)
            
            timestamp = pd.to_datetime(row['timestamp'])
            hour = timestamp.hour + timestamp.minute / 60.0
            hour_sin = np.sin(2 * np.pi * hour / 24.0)
            hour_cos = np.cos(2 * np.pi * hour / 24.0)
            
            is_frozen = False
            if temp_std <= 0.001 or hum_std <= 0.001:
                is_frozen = True
                
            is_anomaly_ml = False
            
            if not is_frozen:
                input_data = {
                    'temperature': [curr_temp],
                    'temp_diff': [temp_diff],
                    'temp_std': [temp_std],
                    'humidity': [curr_hum],
                    'hum_diff': [hum_diff],
                    'hum_std': [hum_std],
                    'hour_sin': [hour_sin],
                    'hour_cos': [hour_cos],
                    'temp_dev': [temp_dev],
                    'hum_dev': [hum_dev],
                    'temp_z': [temp_z]
                }
                
                X = pd.DataFrame(input_data)
                feature_order = [
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
                X = X[feature_order]
                
                X = X.fillna(0)
                
                pred = model.predict(X)[0]
                
                if pred == 1:
                    is_anomaly_ml = True
                    
            results.append({
                'event_id': str(row['event_id']),
                'timestamp': row['timestamp'],
                'sensor_id': int(sensor_id),
                'group_id': int(group_id),
                'temperature': float(curr_temp),
                'humidity': float(curr_hum),
                'validation_error': None,
                'is_frozen': bool(is_frozen),
                'is_anomaly_ml': bool(is_anomaly_ml)
            })
            
    new_json_str = json.dumps(room_history)
    state.update((key[0], new_json_str))
    
    yield pd.DataFrame(results)

spark = SparkSession \
    .builder \
    .appName('AdaptiveDataQuality') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('WARN')
    
source = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('subscribe', 'technically_valid_events') \
    .option('startingOffsets', 'latest') \
    .load()
    
df = source.select(sf.from_json(sf.col('value').cast('string'), base_schema).alias('data')).select('data.*')

processed_df = df \
    .withWatermark('timestamp', '10 minutes') \
    .groupBy('group_id') \
    .applyInPandasWithState(
        func=process_batch,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode='append',
        timeoutConf='NoTimeout'
    )
    
quarantine_df = processed_df.filter('is_frozen = true OR is_anomaly_ml = true')

query_quarantine = quarantine_df \
    .select(sf.to_json(sf.struct('*'), {'ignoreNullFields': 'false'}).alias('value')) \
    .writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('topic', 'quarantined_events') \
    .option('checkpointLocation', '/tmp/checkpoints/inference_quarantine') \
    .start()
    
clean_df = processed_df.filter('is_frozen = false AND is_anomaly_ml = false')

query_clean = clean_df \
    .select(sf.to_json(sf.struct('*'), {'ignoreNullFields': 'false'}).alias('value')) \
    .writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('topic', 'clean_events') \
    .option('checkpointLocation', '/tmp/checkpoints/inference_clean') \
    .start()
    
spark.streams.awaitAnyTermination()