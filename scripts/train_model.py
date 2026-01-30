import pandas as pd
import joblib
import numpy as np
import os
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from schemas import FEATURE_COLUMNS

if os.path.exists('/opt/spark/data'):
    DATA_DIR = '/opt/spark/data'
else:
    DATA_DIR = './data'
    
TRAIN_PATH = os.path.join(DATA_DIR, 'training_dataset')
MODEL_PATH = os.path.join(DATA_DIR, 'model.joblib')
WINDOW_SIZE = 30

df = pd.read_parquet(TRAIN_PATH)

df_meta = pd.json_normalize(df['meta_info'])
df_meta.index = df.index
df = pd.concat([df.drop('meta_info', axis=1), df_meta], axis=1)

df = df.dropna()

df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values(['sensor_id', 'timestamp'])

df['temp_std'] = df.groupby('sensor_id')['temperature'].transform(lambda x: x.rolling(window=WINDOW_SIZE).std())
df['hum_std'] = df.groupby('sensor_id')['humidity'].transform(lambda x: x.rolling(window=WINDOW_SIZE).std())
df['temp_diff'] = df.groupby('sensor_id')['temperature'].transform(lambda x: x.diff().abs())
df['hum_diff'] = df.groupby('sensor_id')['humidity'].transform(lambda x: x.diff().abs())

df['ts_minute'] = df['timestamp'].dt.floor('min')

group_stats = df.groupby(['ts_minute', 'group_id']).agg({
    'temperature': ['mean', 'std'],
    'humidity': ['mean']
}).reset_index()

group_stats.columns = ['ts_minute', 'group_id', 'g_temp_mean', 'g_temp_std', 'g_hum_mean']

group_stats['g_temp_std'] = group_stats['g_temp_std'].fillna(0.1).replace(0, 0.1)

df = pd.merge(df, group_stats, on=['ts_minute', 'group_id'])

df['temp_dev'] = (df['temperature'] - df['g_temp_mean']).abs()

df['hum_dev'] = (df['humidity'] - df['g_hum_mean']).abs()

df['temp_z'] = df['temp_dev'] / df['g_temp_std']

# NaN из-за сдвига
df = df.dropna(subset=['temp_std', 'hum_std', 'temp_diff'])

df['hour_float'] = df['timestamp'].dt.hour + df['timestamp'].dt.minute / 60.0
df['hour_sin'] = np.sin(2 * np.pi * df['hour_float'] / 24.0)
df['hour_cos'] = np.cos(2 * np.pi * df['hour_float'] / 24.0)

df['is_frozen'] = (df['temp_std'] <= 0.001) | (df['hum_std'] <= 0.001)

df_for_ml = df[df['is_frozen'] == False].copy()

X = df_for_ml[FEATURE_COLUMNS]
y = df_for_ml['is_anomaly'].astype(int)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

rf = RandomForestClassifier(
    n_estimators=50,
    random_state=42,
    n_jobs=-1
)

rf.fit(X_train, y_train)

preds = rf.predict(X_test)

print('\nРезультаты')
print(classification_report(y_test, preds))
print('\nМатрица ошибок')
print(confusion_matrix(y_test, preds))

importances = pd.Series(rf.feature_importances_, index=FEATURE_COLUMNS).sort_values(ascending=False)
print('\nТоп признаков')
print(importances)

joblib.dump(rf, MODEL_PATH)

# isofor = IsolationForest(
#     n_estimators=5000,
#     contamination=0.008,
#     random_state=42,
#     n_jobs=-1
# )

# isofor.fit(X_train)

# preds = isofor.predict(X_test)
# preds_converted = np.where(preds == 1, 0, 1)

# print(classification_report(y_test, preds_converted, target_names=['Normal', 'Anomaly']))

# print(confusion_matrix(y_test, preds_converted))
# contamination_rate = y.mean()
# print(f'Реальный процент аномалий (без Frozen): {contamination_rate}')