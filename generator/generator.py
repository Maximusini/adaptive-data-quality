import math
import os
from kafka import KafkaProducer
from faker import Faker
import numpy as np
from datetime import datetime, timedelta
import time
import json

fake = Faker()

class VirtualSensor:
    def __init__(self, sensor_id, group_id):
        self.sensor_id = sensor_id
        self.group_id = group_id

        # Физические показатели
        self.base_humidity_bias = np.random.normal(0, 2) # У каждого датчика своя 'погрешность' влажности
        self.battery = 100.0
        
        # Состояние 'здоровья'
        self.anomaly_mode = None  # 'drift', 'frozen', или None (здоров)
        self.drift_offset = 0.0   # Накопленная ошибка для дрейфа
        self.frozen_temp_val = None  # Значение, на котором завис граудсник
        self.frozen_hum_val = None # Значение, на котором завис гигрометр
        self.steps_in_anomaly = 0 # Сколько тактов уже длится глюк
        
    def update_health(self, current_true_temp, current_true_hum):
        # Если датчик здоров, есть шанс 0.1%, что он сломается
        if self.anomaly_mode is None:
            if np.random.random() < 0.001:
                r = np.random.random()
                
                if r < 0.2:
                    self.anomaly_mode = 'drift'
                    self.drift_offset = 0.0
                    
                elif r < 0.4:
                    self.anomaly_mode = 'frozen_temp'
                    self.frozen_temp_val = current_true_temp + np.random.normal(0, 0.2)
                    
                elif r < 0.6:
                    self.anomaly_mode = 'frozen_hum'
                    self.frozen_hum_val = current_true_hum + np.random.normal(0, 0.2)
                    
                elif r < 0.8:
                    self.anomaly_mode = 'jitter'
                    
                else:
                    self.anomaly_mode = 'physics_error'
                
                self.steps_in_anomaly = 0

        else:
            self.steps_in_anomaly += 1

            # Шанс починиться растет со временем, или фиксированный 10% на каждом шаге
            if np.random.random() < 0.1: # 10% шанс починиться
                self.anomaly_mode = None
                self.drift_offset = 0.0
                self.frozen_temp_val = None
                self.frozen_hum_val = None
    
    def emit_data(self, true_temp, current_time):
        approx_hum = 45.0 - (true_temp - 22.0) * 2.5 + self.base_humidity_bias
        self.update_health(true_temp, approx_hum)
        
        humidity_noise = np.random.normal(0, 0.5)
        is_physics_broken = (self.anomaly_mode == 'physics_error')
        
        if is_physics_broken:
            true_humidity = 45.0 + (true_temp - 22.0) * 2.5 + self.base_humidity_bias + humidity_noise
            
        else:
            # Если температура +25, влажность падает. Если +18, растет.
            true_humidity = 45.0 - (true_temp - 22.0) * 2.5 + self.base_humidity_bias + humidity_noise
            true_humidity = max(0, min(100, true_humidity))
        
        final_temp = 0.0
        final_hum = 0.0
        
        temp_is_anomaly = False
        hum_is_anomaly = False
        
        error_type_label = 'Ok'

        # --- ТЕМПЕРАТУРА ---
        # Зависший градусник
        if self.anomaly_mode == 'frozen_temp':
            final_temp = self.frozen_temp_val # Игнорируем реальную true_temp
            temp_is_anomaly = True
            error_type_label = 'frozen_temp'
        
        # Дрейф (плавный уход)
        elif self.anomaly_mode == 'drift':
            self.drift_offset += np.random.uniform(0.1, 0.3) # Каждый шаг ошибка растет на 0.1 - 0.3 градуса
            final_temp = true_temp + self.drift_offset
            
            # Если ушли достаточно далеко, то считаем аномалией
            if abs(self.drift_offset) > 3.0:
                temp_is_anomaly = True
                error_type_label = 'calibration_drift'
            else:
                temp_is_anomaly = False
        
        # Шум/лихорадка градусника
        elif self.anomaly_mode == 'jitter':
            final_temp = true_temp + np.random.normal(0, 2.5)
            temp_is_anomaly = True
            error_type_label = 'jitter'
        
        # Здоров
        else:
            final_temp = true_temp + np.random.normal(0, 0.2)
            temp_is_anomaly = False
            
        # --- ВЛАЖНОСТЬ ---
        # Зависший гигрометр
        if self.anomaly_mode == 'frozen_hum':
            final_hum = self.frozen_hum_val
            hum_is_anomaly = True
            error_type_label = 'frozen_hum'
            
        # Здоров
        else:
            final_hum = true_humidity
            hum_is_anomaly = False
            
        if is_physics_broken:
            error_type_label = 'physics_error'
            
        final_is_anomaly = temp_is_anomaly or hum_is_anomaly or is_physics_broken
        
        self.battery -= np.random.uniform(0.0001, 0.0005)
        if self.battery < 0: self.battery = 0.0
        
        data = {
            'event_id': fake.uuid4(),
            'timestamp': current_time,
            'sensor_id': self.sensor_id,
            'group_id': self.group_id,
            'temperature': float(round(final_temp, 2)),
            'humidity': float(round(final_hum, 2)),
            'battery': float(round(self.battery, 2)),
            'firmware': '1.2.0v',
            'meta_info': {'is_anomaly': final_is_anomaly, 'error_type': error_type_label}
        }
        return data

def inject_format_errors(data):
    if data['meta_info']['is_anomaly']: 
        return data # Если уже поломка, то не трогаем

    if np.random.random() < 0.01:
        data['meta_info']['is_anomaly'] = True
        if np.random.random() < 0.5:
            data['temperature'] = None
            data['meta_info']['error_type'] = 'null_error'
        else:
            data['temperature'] = 500 # Скачок напряжения
            data['meta_info']['error_type'] = 'voltage_spike'
            
    return data


if __name__ == '__main__':
    kafka_broker = os.environ.get('KAFKA_BROKER_URL', 'localhost:9092')
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker, 
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )   
    
    NUM_SENSORS = 40
    NUM_GROUPS = 10
    
    sensors = []
    for i in range(1, NUM_SENSORS + 1):
        group_id = (i % NUM_GROUPS) + 1
        sensors.append(VirtualSensor(sensor_id=i, group_id=group_id))
    
    # Начинаем эксперимент с 1 января 2024 года, 00:00
    sim_time = datetime(2025, 1, 1, 0, 0, 0)
    
    time_step = timedelta(minutes=1)
    
    try:
        while True:
            current_hour = sim_time.hour + sim_time.minute / 60.0 # Расчет текущего часа (с долями), например 14.5 = 14:30
            
            environment_state = {}
            for g_id in range(1, NUM_GROUPS + 1):
                base_temp_offset = ((g_id * 3) % 7) - 3 # Смещение от -3 до +3
                base_temp = 22.0 + base_temp_offset
                
                # Синусоида суточного цикла (пик в 14:00)
                current_temp = base_temp + 2.5 * math.cos((current_hour - 14) * math.pi / 12)
                environment_state[g_id] = current_temp
            
            for sensor in sensors:
                event = sensor.emit_data(environment_state[sensor.group_id], sim_time.isoformat())
                
                event = inject_format_errors(event)
                
                producer.send('raw_events', event)
                
                print(f'Отправлен event sensor_id={sensor.sensor_id}')
            
            sim_time += time_step
                
            time.sleep(0.5)
            print('-' * 50)
                
    except KeyboardInterrupt:
        print('\nГенератор остановлен.')