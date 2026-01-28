from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

DATA_PATH = '/opt/airflow/data/training_dataset'
REPORTS_PATH = '/opt/airflow/data/reports'

def generate_report():
    if not os.path.exists(REPORTS_PATH):
        os.makedirs(REPORTS_PATH)
    
    df = pd.read_parquet(DATA_PATH)
    max_ts = df['timestamp'].max()
    one_day_ago = max_ts - timedelta(days=1)
    
    df = df[df['timestamp'] >= one_day_ago]
    date = datetime.now()
    
    if not df.empty:
        num_rows = len(df)
        sensor_std = df.groupby('sensor_id')['temperature'].std()
        frozen_count = (sensor_std < 0.01).sum()
        
        
        report = f'''
        ОТЧЁТ ЗА ПОСЛЕДНИЕ СУТКИ
        Период: с {one_day_ago} по {max_ts}.
        ===========================
        Трафик: {num_rows} событий.
        Найдено {frozen_count} зависших датчиков.
        '''
        
    else:
        report = 'Нет данных за последний час'
        
    filename = f'report_{date.strftime("%Y-%m-%d_%H-%M-%S")}.txt'
    
    with open(os.path.join(REPORTS_PATH, filename), 'w') as f:
        f.write(report)

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag_object = DAG(
    dag_id='data_quality_report',
    default_args=default_args,
    schedule=timedelta(minutes=24),
    catchup=False
)

operator = PythonOperator(
    task_id='create_report',
    python_callable=generate_report,
    dag=dag_object
)