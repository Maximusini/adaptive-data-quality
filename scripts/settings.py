"""
settings.py
Централизованная конфигурация проекта.
Содержит пути к файлам, параметры Kafka и список признаков для ML.
"""
import os 
from pathlib import Path

BASE_DIR = Path('/opt/spark/data') if os.path.exists('/opt/spark/data') else Path('./data')
MODEL_PATH = BASE_DIR / 'model.joblib'
CHECKPOINT_DIR = Path('/tmp/checkpoints')


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC_RAW = 'raw_events'
TOPIC_VALID = 'technically_valid_events'
TOPIC_QUARANTINE = 'quarantined_events'
TOPIC_CLEAN = 'clean_events'


# Порядок признаков очень важен. Он должен быть одинаковым при обучении и инференсе.
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
WINDOW_SIZE = 30