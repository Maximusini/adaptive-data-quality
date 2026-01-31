"""
inference.py
Потоковая обработка данных: расчёт статистик, детекция "зависаний" сенсоров и аномалий с помощью ML-модели.
"""
import pandas as pd
import numpy as np
import logging
import joblib
import json
import os
from typing import Iterator, List, Dict, Any, Optional

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

from schemas import base_schema, state_schema, output_schema
import settings

logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ModelLoader:
    """
    Класс для безопасной загрузки и обновления модели.
    Гарантирует, что используется актуальная версия модели при изменении файла.
    """
    instance = None
    model = None
    last_load_time = 0.0
    
    @classmethod
    def get_model(cls) -> Optional[any]:
        path = settings.MODEL_PATH
        
        if not path.exists():
            return None
        
        try:
            current_mod_time = os.path.getmtime(path)
            
            if cls.model is None or current_mod_time > cls.last_load_time:
                cls.model = joblib.load(path)
                cls.last_load_time = current_mod_time
                logger.info(f'Model loaded/updated from {path}')
                
            return cls.model
        except Exception as e:
            logger.error(f'Error loading model: {e}')
            return cls.model

def calculate_features(history: Dict, sensor_id: str, current_temp: float, current_hum: float) -> Dict[str, float]:
    """
    Функция для расчёта статистических признаков на основе истории сенсора.
    """
    
    # Обновление истории
    s_hist = history.setdefault(sensor_id, {'temps': [], 'hums': []})
    s_hist['temps'].append(current_temp)
    s_hist['hums'].append(current_hum)
    
    # Обрезание по окну
    if len(s_hist['temps']) > settings.WINDOW_SIZE:
        s_hist['temps'] = s_hist['temps'][-settings.WINDOW_SIZE:]
        s_hist['hums'] = s_hist['hums'][-settings.WINDOW_SIZE:]
        
    # Расчёт признаков
    temps = np.array(s_hist['temps'])
    hums = np.array(s_hist['hums'])
    
    features = {}
    if len(temps) > 1:
        features['temp_std'] = float(np.std(temps, ddof=1))
        features['temp_diff'] = float(abs(temps[-1] - temps[-2]))
        features['hum_std'] = float(np.std(hums, ddof=1))
        features['hum_diff'] = float(abs(hums[-1] - hums[-2]))
    else:
        features['temp_std'] = 0.0
        features['temp_diff'] = 0.0
        features['hum_std'] = 0.0
        features['hum_diff'] = 0.0
        
    return features

def calculate_group_stats(history: Dict) -> Dict[str, float]:
    """
    Функция для расчёта статистик по группе (помещению) сенсоров.
    """
    room_temps = [sensor['temps'][-1] for sensor in history.values() if sensor['temps']]
    room_hums = [sensor['hums'][-1] for sensor in history.values() if sensor['hums']]
    
    group_stats = {}
    
    # Температура группы
    if room_temps:
        group_stats['g_temp_mean'] = np.mean(room_temps)
        group_stats['g_temp_std'] = np.std(room_temps, ddof=1) if len(room_temps) > 1 else 0.0
        
        # Защита от деления на ноль
        if group_stats['g_temp_std'] == 0 or np.isnan(group_stats['g_temp_std']):
            group_stats['g_temp_std'] = 0.1
    else:
        group_stats['g_temp_mean'] = 0.0
        group_stats['g_temp_std'] = 0.1
    
    # Влажность группы
    if room_hums:
        group_stats['g_hum_mean'] = np.mean(room_hums)
    else:
        group_stats['g_hum_mean'] = 0.0  
        
    return group_stats

def process_batch(key: tuple, pandasdf_iter: Iterator[pd.DataFrame], state: Any) -> Iterator[pd.DataFrame]:
    """
    Основная функция обработки микро-батча.
    Запускается для каждой группы отдельно.
    """
    
    # Восстановление состояния
    if state.exists:
        group_history = json.loads(state.get[1])
    else:
        group_history = {}
        
    model = ModelLoader.get_model()
    
    output_rows = []
    batch_features = []
    batch_indices = []
    
    # Обработка событий
    for df in pandasdf_iter:
        for _, row in df.iterrows():
            # Извлечение данных
            sensor_id = str(row['sensor_id'])
            curr_temp = float(row['temperature'])
            curr_hum = float(row['humidity'])
            timestamp = pd.to_datetime(row['timestamp'])
            
            # Расчёт признаков
            features = calculate_features(group_history, sensor_id, curr_temp, curr_hum)
            group_stats = calculate_group_stats(group_history)
            
            # Z-score и отклонения
            temp_dev = abs(curr_temp - group_stats['g_temp_mean'])
            hum_dev = abs(curr_hum - group_stats['g_hum_mean'])
            temp_z = abs(temp_dev / group_stats['g_temp_std'])
            
            # Временные признаки
            hour = timestamp.hour + timestamp.minute / 60.0
            hour_sin = np.sin(2 * np.pi * hour / 24.0)
            hour_cos = np.cos(2 * np.pi * hour / 24.0)
            
            # Проверка "зависания"
            is_frozen = (features['temp_std'] <= 0.001) or (features['hum_std'] <= 0.001)
            
            # Формирование выходной строки
            result_row = {
                'event_id': str(row['event_id']),
                'timestamp': row['timestamp'],
                'sensor_id': int(sensor_id),
                'group_id': key[0],
                'temperature': float(curr_temp),
                'humidity': float(curr_hum),
                'validation_error': None,
                'is_frozen': bool(is_frozen),
                'is_anomaly_ml': False
            }
            output_rows.append(result_row)
            
            if not is_frozen:
                # Формирование словаря. Обязательно в том же порядке, что и FEATURE_COLUMNS
                features_dict = {
                'temperature': curr_temp,
                'temp_diff': features['temp_diff'],
                'temp_std': features['temp_std'],
                'humidity': curr_hum,
                'hum_diff': features['hum_diff'],
                'hum_std': features['hum_std'],
                'hour_sin': hour_sin,
                'hour_cos': hour_cos,
                'temp_dev': temp_dev,
                'hum_dev': hum_dev,
                'temp_z': temp_z
                }
                
                feature_vector = [features_dict[col] for col in settings.FEATURE_COLUMNS]
                
                batch_features.append(feature_vector)
                batch_indices.append(len(output_rows) - 1)
                
    # Предсказания модели                
    if len(batch_features) > 0:
        X_batch = np.array(batch_features)
        X_batch = np.nan_to_num(X_batch)
        
        if model is None:
            logger.warning('Model not available. Skipping ML anomaly detection.')
            preds = [0] * len(X_batch)
        else:    
            preds = model.predict(X_batch)

        for i, pred in enumerate(preds):
            row_index = batch_indices[i]
            if pred == 1:
                row_index = batch_indices[i]
                output_rows[row_index]['is_anomaly_ml'] = True
    
    state.update((key[0], json.dumps(group_history)))
    
    yield pd.DataFrame(output_rows)

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('AdaptiveDataQuality_Inference') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel('WARN')
        
    source = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option('subscribe', settings.TOPIC_VALID) \
        .option('startingOffsets', 'latest') \
        .load()
        
    df = source.select(sf.from_json(sf.col('value').cast('string'), base_schema).alias('data')).select('data.*')

    # Обработка с сохранением состояния
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
    
    # Разделение на карантин и чистые события        
    quarantine_df = processed_df.filter('is_frozen = true OR is_anomaly_ml = true')

    query_quarantine = quarantine_df \
        .select(sf.to_json(sf.struct('*'), {'ignoreNullFields': 'false'}).alias('value')) \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option('topic', settings.TOPIC_QUARANTINE) \
        .option('checkpointLocation', str(settings.CHECKPOINT_DIR / 'quarantine')) \
        .start()
        
    clean_df = processed_df.filter('is_frozen = false AND is_anomaly_ml = false')

    query_clean = clean_df \
        .select(sf.to_json(sf.struct('*'), {'ignoreNullFields': 'false'}).alias('value')) \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option('topic', settings.TOPIC_CLEAN) \
        .option('checkpointLocation', str(settings.CHECKPOINT_DIR / 'clean')) \
        .start()
        
    spark.streams.awaitAnyTermination()