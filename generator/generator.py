from faker import Faker
import numpy as np
from datetime import datetime
import time
import json

fake = Faker()

class VirtualSensor:
    def __init__(self, sensor_id, group_id):
        self.sensor_id = sensor_id
        self.group_id = group_id

        # Физические показатели
        self.humidity = 45.0
        self.battery = 100.0
        
        # Состояние "здоровья"
        self.anomaly_mode = None  # 'drift', 'frozen', или None (здоров)
        self.drift_offset = 0.0   # Накопленная ошибка для дрейфа
        self.frozen_value = None  # Значение, на котором завис
        self.steps_in_anomaly = 0 # Сколько тактов уже длится глюк
        
    def update_health(self, current_true_temp):
        # Если датчик здоров, есть шанс 5%, что он сломается
        if self.anomaly_mode is None:
            if np.random.random() < 0.05:
                if np.random.random() < 0.5:
                    self.anomaly_mode = 'drift'
                    self.drift_offset = 0.0
                else:
                    self.anomaly_mode = 'frozen'
                    self.frozen_value = current_true_temp + np.random.normal(0, 0.2)
                
                self.steps_in_anomaly = 0

        else:
            self.steps_in_anomaly += 1

            # Шанс починиться растет со временем, или фиксированный 10% на каждом шаге
            if np.random.random() < 0.1: # 10% шанс починиться
                self.anomaly_mode = None
                self.drift_offset = 0.0
                self.frozen_value = None
    
    def emit_data(self, true_temp):
        self.update_health(true_temp)
        
        final_temp = 0.0
        is_anomaly_flag = False
        error_type_label = None

        # Зависший датчик
        if self.anomaly_mode == 'frozen':
            final_temp = self.frozen_value # Игнорируем реальную true_temp
            is_anomaly_flag = True
            error_type_label = 'frozen_sensor'
        
        # Дрейф (плавный уход)
        elif self.anomaly_mode == 'drift':
            self.drift_offset += np.random.uniform(0.2, 0.5) # Каждый шаг ошибка растет на 0.2 - 0.5 градуса
            final_temp = true_temp + self.drift_offset
            
            # Если ушли достаточно далеко, то считаем аномалией
            if abs(self.drift_offset) > 3.0:
                is_anomaly_flag = True
                error_type_label = 'calibration_drift'
            else:
                is_anomaly_flag = False 
        
        # Здоров
        else:
            final_temp = true_temp + np.random.normal(0, 0.2)
            is_anomaly_flag = False
        
        self.humidity += np.random.normal(0, 1)
        if self.humidity > 100: self.humidity = 100
        if self.humidity < 0: self.humidity = 0
        
        self.battery -= 0.01
        
        data = {
            'event_id': fake.uuid4(),
            'timestamp': datetime.now().isoformat(),
            'sensor_id': self.sensor_id,
            'group_id': self.group_id,
            'temperature': round(final_temp, 2),
            'humidity': round(self.humidity, 2),
            'battery': round(self.battery, 2),
            'firmware': '1.2.0v',
            'meta_info': {'is_anomaly': is_anomaly_flag, 'error_type': error_type_label}
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
    
    environment_temps = {1: 22.0, 2: 25.0}
    
    sensors = [
        VirtualSensor(sensor_id=1, group_id=1),
        VirtualSensor(sensor_id=2, group_id=1),
        VirtualSensor(sensor_id=3, group_id=2)
    ]
    
    try:
        while True:
            for group_id in environment_temps:
                environment_temps[group_id] += np.random.normal(0, 0.1)
            
            for sensor in sensors:
                event = sensor.emit_data(environment_temps[sensor.group_id])
                
                event = inject_format_errors(event)
                    
                print(json.dumps(event))
                
            time.sleep(1)
            print("-" * 50)

    except KeyboardInterrupt:
        print("\nГенератор остановлен.")